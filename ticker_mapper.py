"""
Semi-automated ticker → company ID mapping: Finnhub/Index Align names → Firebase companies.
Uses Firebase Realtime Database only (not Firestore). Exact match → save; no exact match
→ prompt user to pick from fuzzy matches, then save ticker/id mapping.
"""
import os
import re
import json
import sys
import csv
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Iterable
from rapidfuzz import process, fuzz
import requests
import firebase_admin
from firebase_admin import credentials, db  # Realtime Database (not Firestore)

# Legal/designator words stripped for core-name matching (reduces false matches on REIT, Inc, etc.)
CORENAME_DESIGNATORS = frozenset({
    'inc', 'incorporated', 'corp', 'corporation', 'llc', 'ltd', 'limited',
    'plc', 'reit', 'lp', 'co', 'company', 'companies', 'sa', 'ag', 'nv',
    'adr', 'group', 'holdings', 'holding', 'trust', 'fund', 'partners',
    'international', 'industries', 'energy', 'resources', 'technologies',
})


# Finnhub symbol types to skip (indexes) when API returns "type"
SKIP_SYMBOL_TYPES = frozenset({'index', 'idx'})

# For auto-match: leading token of core name must be this similar (avoids NG→NRG, etc.)
LEADING_TOKEN_MIN_RATIO = 85

# Sector groups: if API name has a word in one group and DB name in another (and no shared group), reject.
# Enables Excellon (mining) ≠ Exelon (utility), BlackRock Silver (mining) ≠ BlackRock (asset), Rocket Lab (aerospace) ≠ Rocket Companies (finance), etc.
SECTOR_GROUPS = (
    frozenset({'mining', 'resources', 'silver', 'gold', 'zinc', 'oil', 'gas', 'exploration', 'drilling', 'lead'}),  # 0 extractive
    frozenset({'utility', 'electric', 'power', 'water'}),  # 1 utility
    frozenset({'bank', 'insurance', 'financial', 'express', 'capital', 'asset', 'management', 'fund', 'trust', 'reit', 'mortgage', 'loan'}),  # 2 finance
    frozenset({'paper'}),  # 3 paper
    frozenset({'technology', 'digital', 'storage'}),  # 4 tech
    frozenset({'airline', 'aviation', 'airways'}),  # 5 airline
    frozenset({'casino', 'gaming'}),  # 6 casino
    frozenset({'hotel', 'resort', 'hospitality'}),  # 7 hospitality
    frozenset({'exchange', 'market'}),  # 8 exchange/market infra
    frozenset({'aerospace', 'space', 'satellite', 'rocket', 'launch'}),  # 9 aerospace
    frozenset({'cannabis', 'green', 'hemp', 'marijuana'}),  # 10 cannabis (green = cannabis/green energy context)
    frozenset({'nuclear', 'atomic'}),  # 11 nuclear
    frozenset({'property', 'real', 'estate', 'lease'}),  # 12 property
    frozenset({'industrial', 'manufacturing', 'pumps', 'flow'}),  # 13 industrial
)
# Flat set for "has any sector word"
SECTOR_WORDS = frozenset(w for g in SECTOR_GROUPS for w in g)

# Short ticker + only these tokens → don't auto-match (e.g. INTERNATIONAL GOLD RESOURCES vs International Paper)
GENERIC_ONLY = frozenset({'international', 'global', 'world'})

# Geographic/generic: when overlap is only these + designators, require high threshold or reject by sector
GEO_GENERIC = frozenset({'western', 'eastern', 'northern', 'southern', 'american', 'national', 'global', 'international', 'world'})

# Fund/product indicators: if API name has these and DB name is just the brand, don't auto-match (e.g. BlackRock Income Trust ≠ BlackRock)
FUND_INDICATORS = frozenset({'trust', 'income', 'fund', 'plc'})


class TickerMapper:
    def __init__(self, finnhub_api_key: str, firebase_credentials_path: str, 
                 auto_match_threshold: int = 90, min_prompt_confidence: int = 50,
                 firebase_collection: str = 'companies', firebase_name_field: str = 'name',
                 firebase_database_url: str = '', skip_symbol_types: Optional[set] = None,
                 acronym_expansions: Optional[Dict[str, str]] = None,
                 hard_negative_pairs: Optional[Iterable[Tuple[str, str]]] = None):
        """Initialize the TickerMapper.
        acronym_expansions: optional map acronym -> full name (e.g. BOA -> Bank of America).
        hard_negative_pairs: optional list of (core_api, core_db) pairs to never match; from config for known exceptions / regression only.
        """
        self.finnhub_api_key = finnhub_api_key
        self.auto_match_threshold = auto_match_threshold
        self.min_prompt_confidence = min_prompt_confidence  # below this, skip without prompting
        self.firebase_collection = firebase_collection
        self.firebase_name_field = firebase_name_field
        self.skip_symbol_types = skip_symbol_types or SKIP_SYMBOL_TYPES
        self.acronym_expansions = acronym_expansions or {}
        self._hard_negative_pairs = frozenset(
            (a.strip().lower(), b.strip().lower())
            for a, b in (hard_negative_pairs or [])
            if isinstance(a, str) and isinstance(b, str)
        )
        self.mappings = {}
        
        # Initialize Firebase (Realtime Database URL required)
        db_url = (firebase_database_url or '').strip()
        if not db_url:
            try:
                with open(firebase_credentials_path, 'r') as f:
                    cred_data = json.load(f)
                project_id = cred_data.get('project_id', '')
                if project_id:
                    db_url = f'https://{project_id}-default-rtdb.firebaseio.com'
            except Exception:
                pass
        if not db_url:
            print('Error: firebase_database_url is required in config.json.')
            print('Find it in Firebase Console → Realtime Database → URL (e.g. https://YOUR_PROJECT-default-rtdb.firebaseio.com)')
            sys.exit(1)
        if not firebase_admin._apps:
            cred = credentials.Certificate(firebase_credentials_path)
            firebase_admin.initialize_app(cred, {'databaseURL': db_url})
        self.db = db.reference()
    
    def normalize_name(self, name: str) -> str:
        """Normalize company name for matching (exact and fuzzy)."""
        if not name:
            return ""
        s = name.lower().strip()
        s = re.sub(r'[.,\-/]+', ' ', s)   # punctuation to space
        s = re.sub(r'\s+', ' ', s)         # collapse spaces
        return s.strip()

    def normalize_core_name(self, name: str) -> str:
        """Strip legal/designator words (Inc, REIT, Corp, etc.) so fuzzy matching
        emphasizes core name and designators don't dominate scores."""
        if not name:
            return ""
        s = self.normalize_name(name)
        # Bank parent/subsidiary: "Zions Bancorp NA" vs "Zions Bancorporation" -> same core
        s = re.sub(r'\bbancorporation\b', 'bancorp', s, flags=re.IGNORECASE)
        s = re.sub(r'\s+na\s*$', '', s, flags=re.IGNORECASE)
        # Strip share-class / perpetual suffixes so "Constellation Brands INC-A" and "Public Storage - PSA 4 PERP" match
        s = re.sub(r'\s+inc-?a\s*$', '', s, flags=re.IGNORECASE)
        s = re.sub(r'\s+class\s+[ab]\s*$', '', s, flags=re.IGNORECASE)
        s = re.sub(r'\s+cl\s+[ab]\s*$', '', s, flags=re.IGNORECASE)
        s = re.sub(r'\s+[a-z0-9]+\s+\d+\s*perp\s*$', '', s, flags=re.IGNORECASE)  # "psa 4 perp"
        s = re.sub(r'\s+perp\s*$', '', s, flags=re.IGNORECASE)
        s = re.sub(r'\s+the\s*$', '', s, flags=re.IGNORECASE)
        s = re.sub(r'\s+co\s*/\s*the\s*$', '', s, flags=re.IGNORECASE)
        words = s.split()
        while words and words[-1] in CORENAME_DESIGNATORS:
            words.pop()
        return ' '.join(words).strip() or s

    def _name_tokens(self, name: str) -> set:
        """Normalized name as set of tokens (for sector check)."""
        return set(self.normalize_name(name).split())

    def _sector_group_ids(self, name: str) -> set:
        """Set of sector group indices (0..len(SECTOR_GROUPS)-1) that appear in name."""
        tok = self._name_tokens(name)
        ids = set()
        for i, g in enumerate(SECTOR_GROUPS):
            if tok & g:
                ids.add(i)
        return ids

    def _sector_conflict(self, api_name: str, db_name: str) -> bool:
        """True if both names have sector words but in different sectors (e.g. mining vs utility, aerospace vs finance)."""
        api_g = self._sector_group_ids(api_name)
        db_g = self._sector_group_ids(db_name)
        if not api_g or not db_g:
            return False
        return api_g.isdisjoint(db_g)

    def _suspicious_generic_match(self, ticker: str, finnhub_name: str) -> bool:
        """True if short ticker and name is mostly generic words (don't auto-match)."""
        core_tokens = set(self.normalize_core_name(finnhub_name).split())
        return (
            len(ticker) <= 4
            and core_tokens.issubset(GENERIC_ONLY | CORENAME_DESIGNATORS)
        )

    def _fund_vs_company(self, api_name: str, db_name: str) -> bool:
        """True if API name looks like a fund/product and DB name is the parent (e.g. BlackRock Income Trust vs BlackRock)."""
        api_tok = self._name_tokens(api_name)
        db_tok = self._name_tokens(db_name)
        if not (FUND_INDICATORS & api_tok):
            return False
        if db_tok <= api_tok and len(db_tok) < len(api_tok):
            return True
        return False

    def _hard_negative_match(self, api_name: str, db_name: str) -> bool:
        """True if (core_api, core_db) is in config hard_negative_pairs (known exceptions only)."""
        if not self._hard_negative_pairs:
            return False
        core_api = self.normalize_core_name(api_name).lower().strip()
        core_db = self.normalize_core_name(db_name).lower().strip()
        if not core_api or not core_db:
            return False
        return (core_api, core_db) in self._hard_negative_pairs or (core_db, core_api) in self._hard_negative_pairs

    def _only_generic_overlap(self, api_name: str, db_name: str) -> bool:
        """True if the only shared tokens are geographic/generic/designators (weak match)."""
        api_tok = self._name_tokens(api_name)
        db_tok = self._name_tokens(db_name)
        common = api_tok & db_tok
        substantive = common - GEO_GENERIC - CORENAME_DESIGNATORS
        return len(substantive) <= 1

    def fetch_finnhub_companies(self) -> Dict[str, str]:
        """Fetch all publicly traded companies from Finnhub."""
        print("Fetching companies from Finnhub...")
        url = f'https://finnhub.io/api/v1/stock/symbol?exchange=US&token={self.finnhub_api_key}'
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            companies = response.json()
            
            company_map = {}
            skipped_index = 0
            for company in companies:
                sym_type = (company.get('type') or '').strip().lower()
                if sym_type and sym_type in self.skip_symbol_types:
                    skipped_index += 1
                    continue
                name = company.get('description', '').strip()
                ticker = company.get('symbol', '').strip()
                if name and ticker:
                    company_map[name] = ticker
            if skipped_index:
                print(f"Skipped {skipped_index} index/non-stock symbols (type in skip list).")
            print(f"Fetched {len(company_map)} companies from Finnhub")
            return company_map
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching from Finnhub: {e}")
            sys.exit(1)
    
    def fetch_firebase_companies(self) -> Dict[str, str]:
        """Fetch all companies from Firebase."""
        print("Fetching companies from Firebase...")
        try:
            companies_ref = self.db.child(self.firebase_collection)
            companies_data = companies_ref.get()
            
            company_map = {}
            if companies_data:
                for company_id, data in companies_data.items():
                    if isinstance(data, dict):
                        name = data.get(self.firebase_name_field, '').strip()
                        if name:
                            company_map[name] = company_id
            
            print(f"Fetched {len(company_map)} companies from Firebase")
            return company_map
        
        except Exception as e:
            print(f"Error fetching from Firebase: {e}")
            sys.exit(1)
    
    def find_exact_match(self, finnhub_name: str, normalized_firebase: Dict[str, str],
                        core_to_firebase_names: Optional[Dict[str, List[str]]] = None) -> Optional[str]:
        """Exact match: full normalized name, or core name (designator-agnostic). When multiple
        Firebase companies share the same core, pick the one with highest name similarity."""
        normalized_finnhub = self.normalize_name(finnhub_name)
        match = normalized_firebase.get(normalized_finnhub)
        if match:
            return match
        # Core-name match: "Company Name, Inc." / "Company Name Inc." vs "Company Name" (same core)
        if not core_to_firebase_names:
            return None
        core = self.normalize_core_name(finnhub_name)
        if not core:
            return None
        names = core_to_firebase_names.get(core)
        if not names:
            return None
        if len(names) == 1:
            return names[0]
        # Multiple Firebase companies with same core: pick the one that best matches Finnhub name
        result = process.extractOne(
            finnhub_name, names, scorer=fuzz.token_sort_ratio
        )
        best_name, best_score = result[0], result[1]
        if best_score >= 85:  # designator-only difference gives very high score
            return best_name
        return None
    
    def find_fuzzy_matches(self, finnhub_name: str, firebase_names: List[str], 
                          top_n: int = 5) -> List[Tuple[str, float]]:
        """Find top N fuzzy matches using core names (designators stripped). Uses acronym expansion if configured."""
        name_for_fuzzy = (
            self.acronym_expansions.get(finnhub_name.strip()) or
            self.acronym_expansions.get(finnhub_name.strip().upper()) or
            finnhub_name
        )
        core_finnhub = self.normalize_core_name(name_for_fuzzy)
        firebase_core_list = [self.normalize_core_name(n) for n in firebase_names]
        matches = process.extract(
            core_finnhub,
            firebase_core_list,
            scorer=fuzz.token_sort_ratio,
            limit=top_n
        )
        return [(firebase_names[m[2]], m[1]) for m in matches]
    
    def prompt_user_selection(self, finnhub_name: str, ticker: str, 
                             matches: List[Tuple[str, float]]) -> Optional[str]:
        """Prompt user to select from a list of matches."""
        print(f"\n{'='*60}")
        print(f"Finnhub Company: {finnhub_name}")
        print(f"Ticker: {ticker}")
        print(f"{'='*60}")
        print("No exact match found. Select a company or skip:")
        print()
        
        for idx, (name, score) in enumerate(matches, 1):
            print(f"  {idx}. {name} (confidence: {score:.1f}%)")
        
        skip_num = len(matches) + 1
        print(f"  {skip_num}. Skip (not in our list)")
        print(f"\n  Enter 1–{len(matches)} to select, {skip_num} to skip, or just Enter to skip.")
        print()
        
        while True:
            try:
                choice = input("Your choice: ").strip()
                if not choice:
                    print("Skipped.\n")
                    return None
                
                choice_num = int(choice)
                if 1 <= choice_num <= len(matches):
                    selected_name = matches[choice_num - 1][0]
                    print(f"Selected: {selected_name}\n")
                    return selected_name
                elif choice_num == skip_num:
                    print("Skipped.\n")
                    return None
                else:
                    print(f"Enter a number from 1 to {skip_num} (1–{len(matches)} = pick, {skip_num} = skip).")
            except ValueError:
                print("Please enter a valid number")
            except KeyboardInterrupt:
                print("\n\nInterrupted by user. Saving progress...")
                return None
    
    def _write_log_row(self, log_file, fieldnames: List[str], row: Dict[str, str]):
        if not log_file:
            return
        w = csv.DictWriter(log_file, fieldnames=fieldnames, lineterminator='\n')
        w.writerow(row)

    def process_matching(self, finnhub_companies: Dict[str, str], 
                        firebase_companies: Dict[str, str],
                        existing_mappings: Dict[str, str] = None,
                        save_progress_file: str = None,
                        match_log_path: str = None) -> Dict[str, str]:
        """Process matching between Finnhub and Firebase companies."""
        if existing_mappings is None:
            existing_mappings = {}
        
        mappings = existing_mappings.copy()
        firebase_names = list(firebase_companies.keys())
        normalized_firebase = {self.normalize_name(name): name for name in firebase_names}
        # Acronym expansion: e.g. BOA -> Bank of America so exact/fuzzy can match
        for acronym, expansion in self.acronym_expansions.items():
            exp_norm = self.normalize_name(expansion)
            for fb_name in firebase_names:
                if self.normalize_name(fb_name) == exp_norm:
                    normalized_firebase[self.normalize_name(acronym)] = fb_name
                    break
        core_to_firebase_names: Dict[str, List[str]] = {}
        for name in firebase_names:
            core = self.normalize_core_name(name)
            core_to_firebase_names.setdefault(core, []).append(name)
        total = len(finnhub_companies)
        already_mapped = sum(1 for ticker in finnhub_companies.values() if ticker in mappings)
        
        log_fields = ['ticker', 'finnhub_name', 'match_type', 'firebase_name', 'firebase_id', 'fuzzy_score', 'notes']
        log_file = None
        if match_log_path:
            log_file = open(match_log_path, 'w', newline='', encoding='utf-8')
            log_file.write(f"# Ticker match log — {datetime.now(timezone.utc).isoformat()}\n")
            csv.DictWriter(log_file, fieldnames=log_fields, lineterminator='\n').writeheader()
        
        def log_row(match_type: str, firebase_name: str = '', firebase_id: str = '', fuzzy_score: str = '', notes: str = ''):
            self._write_log_row(log_file, log_fields, {
                'ticker': ticker, 'finnhub_name': finnhub_name, 'match_type': match_type,
                'firebase_name': firebase_name, 'firebase_id': firebase_id, 'fuzzy_score': fuzzy_score, 'notes': notes
            })
        
        print(f"\nProcessing {total} companies...")
        print(f"Auto-match threshold: {self.auto_match_threshold}% (prompt only if best match >= {self.min_prompt_confidence}%)")
        if already_mapped > 0:
            print(f"Already mapped: {already_mapped} (will be skipped)")
        if match_log_path:
            print(f"Match log: {match_log_path}")
        print()
        
        exact_matches = 0
        auto_fuzzy_matches = 0
        manual_matches = 0
        skipped = 0
        rejected_self = 0
        processed = 0
        
        for idx, (finnhub_name, ticker) in enumerate(finnhub_companies.items(), 1):
            # Skip if already mapped
            if ticker in mappings:
                continue
            
            processed += 1
            print(f"[{processed}/{total - already_mapped}] Processing: {finnhub_name} ({ticker})")
            
            exact_match = self.find_exact_match(finnhub_name, normalized_firebase, core_to_firebase_names)
            if exact_match:
                company_id = firebase_companies[exact_match]
                if company_id.lower() == ticker.lower():
                    print(f"  Rejected: company_id equals ticker (exact match was self)")
                    log_row('rejected_ticker_self', exact_match, company_id, notes='company_id equals ticker')
                    rejected_self += 1
                else:
                    mappings[ticker] = company_id
                    exact_matches += 1
                    print(f"  Exact match: {exact_match}")
                    log_row('exact', exact_match, company_id)
                    if save_progress_file:
                        self.save_mappings(mappings, save_progress_file)
                continue
            
            fuzzy_matches = self.find_fuzzy_matches(finnhub_name, firebase_names)
            if not fuzzy_matches:
                print(f"  No matches found, skipping")
                log_row('skipped', notes='no fuzzy matches')
                skipped += 1
                continue

            # Only keep candidates whose leading token aligns (e.g. NG vs NRG → drop NRG from list)
            core_finnhub = self.normalize_core_name(finnhub_name)
            tok_f = (core_finnhub.split() or [''])[0]
            filtered_matches = []
            for fb_name, score in fuzzy_matches:
                tok_b = (self.normalize_core_name(fb_name).split() or [''])[0]
                if not tok_f or not tok_b or fuzz.ratio(tok_f, tok_b) >= LEADING_TOKEN_MIN_RATIO:
                    filtered_matches.append((fb_name, score))
            fuzzy_matches = filtered_matches
            if not fuzzy_matches:
                print(f"  No matches with aligned leading token (e.g. {tok_f!r}), skipping")
                log_row('skipped', notes='no matches after leading-token filter')
                skipped += 1
                continue

            # Drop candidates with sector conflict, fund-vs-company, or hard negative pairs
            filtered_matches = []
            for fb_name, score in fuzzy_matches:
                if self._sector_conflict(finnhub_name, fb_name):
                    log_row('rejected_sector_conflict', fb_name, firebase_companies.get(fb_name, ''), str(score),
                            'sector groups conflict (e.g. mining vs utility, aerospace vs finance)')
                    continue
                if self._fund_vs_company(finnhub_name, fb_name):
                    log_row('rejected_fund_vs_company', fb_name, firebase_companies.get(fb_name, ''), str(score),
                            'fund/product vs operating company')
                    continue
                if self._hard_negative_match(finnhub_name, fb_name):
                    log_row('rejected_hard_negative', fb_name, firebase_companies.get(fb_name, ''), str(score),
                            'hard negative pair (e.g. Excellon vs Exelon, IMDEX vs IDEX)')
                    continue
                filtered_matches.append((fb_name, score))
            fuzzy_matches = filtered_matches
            if not fuzzy_matches:
                print(f"  No matches after sector/fund/negative filter, skipping")
                log_row('skipped', notes='no matches after sector, fund_vs_company, or hard_negative filter')
                skipped += 1
                continue

            best_match, best_score = fuzzy_matches[0]
            # When overlap is only geographic/generic words, require higher bar to avoid Western Gold → Western Digital
            if self._only_generic_overlap(finnhub_name, best_match) and best_score < 92:
                print(f"  Only generic/geographic overlap (score {best_score:.1f}% < 92%), skipping")
                log_row('skipped', best_match, firebase_companies.get(best_match, ''), str(best_score),
                        'only_generic_overlap')
                skipped += 1
                continue
            if self._suspicious_generic_match(ticker, finnhub_name):
                print(f"  Suspicious generic name (short ticker + generic words only), skipping")
                log_row('skipped', best_match, firebase_companies.get(best_match, ''), str(best_score),
                        'suspicious_generic_match')
                skipped += 1
                continue
            if best_score >= self.auto_match_threshold:
                company_id = firebase_companies[best_match]
                if company_id.lower() == ticker.lower():
                    print(f"  Rejected: company_id equals ticker (auto fuzzy was self)")
                    log_row('rejected_ticker_self', best_match, company_id, str(best_score), 'company_id equals ticker')
                    rejected_self += 1
                elif company_id in mappings.values():
                    existing = [t for t, cid in mappings.items() if cid == company_id]
                    print(f"  Company already mapped to {existing[0]}; prompting (one-to-many safeguard).")
                    log_row('one_to_many_prompt', best_match, company_id, str(best_score), f"already mapped to {existing[0]}")
                else:
                    mappings[ticker] = company_id
                    auto_fuzzy_matches += 1
                    print(f"  Auto-matched (confidence: {best_score:.1f}%): {best_match}")
                    log_row('auto_fuzzy', best_match, company_id, str(best_score))
                    if save_progress_file:
                        self.save_mappings(mappings, save_progress_file)
                    continue
            
            if best_score < self.min_prompt_confidence:
                print(f"  Best match only {best_score:.1f}% — skipping (no prompt)")
                log_row('skipped', best_match, firebase_companies.get(best_match, ''), str(best_score), 'below min_prompt_confidence')
                skipped += 1
                continue
            
            selected_name = self.prompt_user_selection(finnhub_name, ticker, fuzzy_matches)
            if selected_name:
                company_id = firebase_companies[selected_name]
                if company_id.lower() == ticker.lower():
                    print(f"  Rejected: company_id equals ticker (manual selection was self)")
                    log_row('rejected_ticker_self', selected_name, company_id, notes='company_id equals ticker')
                    rejected_self += 1
                else:
                    mappings[ticker] = company_id
                    manual_matches += 1
                    score = next((s for n, s in fuzzy_matches if n == selected_name), '')
                    log_row('manual', selected_name, company_id, str(score) if score else '')
                    if save_progress_file:
                        self.save_mappings(mappings, save_progress_file)
            else:
                log_row('skipped', fuzzy_matches[0][0], firebase_companies.get(fuzzy_matches[0][0], ''), str(fuzzy_matches[0][1]), 'user skipped')
                skipped += 1
        
        if log_file:
            log_file.close()
        
        print(f"\n{'='*60}")
        print("Matching Summary:")
        print(f"  Exact matches: {exact_matches}")
        print(f"  Auto fuzzy matches: {auto_fuzzy_matches}")
        print(f"  Manual matches: {manual_matches}")
        print(f"  Rejected (ticker=company_id): {rejected_self}")
        print(f"  Skipped: {skipped}")
        print(f"  Total mappings: {len(mappings)}")
        print(f"{'='*60}\n")
        
        return mappings
    
    def load_existing_mappings(self, input_file: str = "ticker_mappings.json") -> Dict[str, str]:
        """Load existing mappings from a JSON file."""
        if not os.path.exists(input_file):
            return {}
        
        try:
            with open(input_file, 'r') as f:
                mappings = json.load(f)
                print(f"Loaded {len(mappings)} existing mappings from {input_file}")
                return mappings
        except Exception as e:
            print(f"Warning: Could not load existing mappings: {e}")
            return {}
    
    def save_mappings(self, mappings: Dict[str, str], output_file: str = "ticker_mappings.json"):
        """Save ticker to company ID mappings to a JSON file."""
        print(f"Saving mappings to {output_file}...")
        with open(output_file, 'w') as f:
            json.dump(mappings, f, indent=2)
        print(f"Saved {len(mappings)} mappings to {output_file}")
    
    def save_mappings_to_firebase(self, mappings: Dict[str, str], 
                                  collection_name: str = "ticker_mappings"):
        """Save ticker to company ID mappings to Firebase."""
        print(f"Saving mappings to Firebase path '{collection_name}'...")
        mappings_ref = self.db.child(collection_name)
        
        updates = {}
        for ticker, company_id in mappings.items():
            updates[ticker] = {
                'ticker': ticker,
                'company_id': company_id
            }
        
        mappings_ref.update(updates)
        print(f"Saved {len(mappings)} mappings to Firebase")
    
    def run(self, save_to_firebase: bool = False, firebase_collection: str = "ticker_mappings",
            resume: bool = True, output_file: str = "ticker_mappings.json",
            save_progress: bool = True, match_log_path: str = None):
        """Run the complete matching process."""
        finnhub_companies = self.fetch_finnhub_companies()
        firebase_companies = self.fetch_firebase_companies()
        
        if not finnhub_companies:
            print("No companies fetched from Finnhub. Exiting.")
            return
        
        if not firebase_companies:
            print("No companies fetched from Firebase. Exiting.")
            return
        
        # Load existing mappings if resuming
        existing_mappings = {}
        if resume:
            existing_mappings = self.load_existing_mappings(output_file)
        
        progress_file = output_file if save_progress else None
        mappings = self.process_matching(
            finnhub_companies, 
            firebase_companies,
            existing_mappings=existing_mappings,
            save_progress_file=progress_file,
            match_log_path=match_log_path
        )
        
        if not mappings:
            print("No mappings created. Exiting.")
            return
        
        # Final save (in case save_progress was False)
        if not save_progress:
            self.save_mappings(mappings, output_file)
        
        if save_to_firebase:
            self.save_mappings_to_firebase(mappings, firebase_collection)
        
        print("Done!")


def main():
    config_file = "config.json"
    if not os.path.exists(config_file):
        print(f"Error: {config_file} not found. Please create it with your API keys.")
        print("See config.example.json for an example.")
        sys.exit(1)
    
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    finnhub_api_key = config.get('finnhub_api_key')
    firebase_credentials = config.get('firebase_credentials_path')
    firebase_database_url = config.get('firebase_database_url', '')
    auto_match_threshold = config.get('auto_match_threshold', 90)
    min_prompt_confidence = config.get('min_prompt_confidence', 70)
    save_to_firebase = config.get('save_to_firebase', False)
    firebase_collection = config.get('firebase_collection', 'ticker_mappings')
    firebase_companies_collection = config.get('firebase_companies_collection', 'companies')
    firebase_name_field = config.get('firebase_name_field', 'name')
    output_file = config.get('output_file', 'ticker_mappings.json')
    resume = config.get('resume', True)
    save_progress = config.get('save_progress', True)
    match_log_file = config.get('match_log_file', 'ticker_match_log.csv')
    
    if not finnhub_api_key:
        print("Error: finnhub_api_key not found in config.json")
        sys.exit(1)
    
    if not firebase_credentials:
        print("Error: firebase_credentials_path not found in config.json")
        sys.exit(1)
    
    if not os.path.exists(firebase_credentials):
        print(f"Error: Firebase credentials file not found: {firebase_credentials}")
        sys.exit(1)
    
    skip_types = config.get('skip_symbol_types')
    if skip_types is not None:
        skip_types = frozenset(s.strip().lower() for s in skip_types if s)
    acronym_expansions = config.get('acronym_expansions')  # optional: {"BOA": "Bank of America", ...}
    if not isinstance(acronym_expansions, dict):
        acronym_expansions = None
    # Optional: known false-positive pairs (core_api, core_db). For exceptions/regression only; main logic is normalization + sector rules.
    hard_negative_pairs = config.get('hard_negative_pairs')
    if isinstance(hard_negative_pairs, list):
        hard_negative_pairs = [
            (p[0], p[1]) for p in hard_negative_pairs
            if isinstance(p, (list, tuple)) and len(p) >= 2
        ]
    else:
        hard_negative_pairs = None
    mapper = TickerMapper(
        finnhub_api_key=finnhub_api_key,
        firebase_credentials_path=firebase_credentials,
        auto_match_threshold=auto_match_threshold,
        min_prompt_confidence=min_prompt_confidence,
        firebase_collection=firebase_companies_collection,
        firebase_name_field=firebase_name_field,
        firebase_database_url=firebase_database_url,
        skip_symbol_types=skip_types,
        acronym_expansions=acronym_expansions,
        hard_negative_pairs=hard_negative_pairs
    )
    
    mapper.run(
        save_to_firebase=save_to_firebase, 
        firebase_collection=firebase_collection,
        resume=resume,
        output_file=output_file,
        save_progress=save_progress,
        match_log_path=match_log_file
    )


if __name__ == "__main__":
    main()

