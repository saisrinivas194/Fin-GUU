import os
import json
import sys
from typing import Dict, List, Tuple, Optional
from rapidfuzz import process, fuzz
import requests
import firebase_admin
from firebase_admin import credentials, db


class TickerMapper:
    def __init__(self, finnhub_api_key: str, firebase_credentials_path: str, 
                 auto_match_threshold: int = 90, firebase_collection: str = 'companies',
                 firebase_name_field: str = 'name', firebase_database_url: str = ''):
        """Initialize the TickerMapper."""
        self.finnhub_api_key = finnhub_api_key
        self.auto_match_threshold = auto_match_threshold
        self.firebase_collection = firebase_collection
        self.firebase_name_field = firebase_name_field
        self.mappings = {}
        
        # Initialize Firebase
        if not firebase_admin._apps:
            cred = credentials.Certificate(firebase_credentials_path)
            app_options = {}
            if firebase_database_url:
                app_options['databaseURL'] = firebase_database_url
            firebase_admin.initialize_app(cred, app_options)
        self.db = db.reference()
    
    def normalize_name(self, name: str) -> str:
        """Normalize company name for matching."""
        if not name:
            return ""
        normalized = name.lower().strip()
        return normalized
    
    def fetch_finnhub_companies(self) -> Dict[str, str]:
        """Fetch all publicly traded companies from Finnhub."""
        print("Fetching companies from Finnhub...")
        url = f'https://finnhub.io/api/v1/stock/symbol?exchange=US&token={self.finnhub_api_key}'
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            companies = response.json()
            
            company_map = {}
            for company in companies:
                name = company.get('description', '').strip()
                ticker = company.get('symbol', '').strip()
                if name and ticker:
                    company_map[name] = ticker
            
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
    
    def find_exact_match(self, finnhub_name: str, normalized_firebase: Dict[str, str]) -> Optional[str]:
        """Check for exact match (case-insensitive)."""
        normalized_finnhub = self.normalize_name(finnhub_name)
        return normalized_firebase.get(normalized_finnhub)
    
    def find_fuzzy_matches(self, finnhub_name: str, firebase_names: List[str], 
                          top_n: int = 5) -> List[Tuple[str, float]]:
        """Find top N fuzzy matches for a company name."""
        matches = process.extract(
            finnhub_name,
            firebase_names,
            scorer=fuzz.token_sort_ratio,
            limit=top_n
        )
        return [(match[0], match[1]) for match in matches]
    
    def prompt_user_selection(self, finnhub_name: str, ticker: str, 
                             matches: List[Tuple[str, float]]) -> Optional[str]:
        """Prompt user to select from a list of matches."""
        print(f"\n{'='*60}")
        print(f"Finnhub Company: {finnhub_name}")
        print(f"Ticker: {ticker}")
        print(f"{'='*60}")
        print("No exact match found. Please select from the following options:")
        print()
        
        for idx, (name, score) in enumerate(matches, 1):
            print(f"  {idx}. {name} (confidence: {score:.1f}%)")
        
        print(f"  {len(matches) + 1}. Skip this company")
        print()
        
        while True:
            try:
                choice = input("Enter your choice (number): ").strip()
                if not choice:
                    return None
                
                choice_num = int(choice)
                if 1 <= choice_num <= len(matches):
                    selected_name = matches[choice_num - 1][0]
                    print(f"Selected: {selected_name}\n")
                    return selected_name
                elif choice_num == len(matches) + 1:
                    print("Skipped.\n")
                    return None
                else:
                    print(f"Please enter a number between 1 and {len(matches) + 1}")
            except ValueError:
                print("Please enter a valid number")
            except KeyboardInterrupt:
                print("\n\nInterrupted by user. Saving progress...")
                return None
    
    def process_matching(self, finnhub_companies: Dict[str, str], 
                        firebase_companies: Dict[str, str]) -> Dict[str, str]:
        """Process matching between Finnhub and Firebase companies."""
        mappings = {}
        firebase_names = list(firebase_companies.keys())
        normalized_firebase = {self.normalize_name(name): name for name in firebase_names}
        total = len(finnhub_companies)
        
        print(f"\nProcessing {total} companies...")
        print(f"Auto-match threshold: {self.auto_match_threshold}%")
        print()
        
        exact_matches = 0
        auto_fuzzy_matches = 0
        manual_matches = 0
        skipped = 0
        
        for idx, (finnhub_name, ticker) in enumerate(finnhub_companies.items(), 1):
            print(f"[{idx}/{total}] Processing: {finnhub_name} ({ticker})")
            
            exact_match = self.find_exact_match(finnhub_name, normalized_firebase)
            if exact_match:
                mappings[ticker] = firebase_companies[exact_match]
                exact_matches += 1
                print(f"  Exact match: {exact_match}")
                continue
            
            fuzzy_matches = self.find_fuzzy_matches(finnhub_name, firebase_names)
            if not fuzzy_matches:
                print(f"  No matches found, skipping")
                skipped += 1
                continue
            
            best_match, best_score = fuzzy_matches[0]
            if best_score >= self.auto_match_threshold:
                mappings[ticker] = firebase_companies[best_match]
                auto_fuzzy_matches += 1
                print(f"  Auto-matched (confidence: {best_score:.1f}%): {best_match}")
                continue
            
            selected_name = self.prompt_user_selection(finnhub_name, ticker, fuzzy_matches)
            if selected_name:
                mappings[ticker] = firebase_companies[selected_name]
                manual_matches += 1
            else:
                skipped += 1
        
        print(f"\n{'='*60}")
        print("Matching Summary:")
        print(f"  Exact matches: {exact_matches}")
        print(f"  Auto fuzzy matches: {auto_fuzzy_matches}")
        print(f"  Manual matches: {manual_matches}")
        print(f"  Skipped: {skipped}")
        print(f"  Total mappings: {len(mappings)}")
        print(f"{'='*60}\n")
        
        return mappings
    
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
    
    def run(self, save_to_firebase: bool = False, firebase_collection: str = "ticker_mappings"):
        """Run the complete matching process."""
        finnhub_companies = self.fetch_finnhub_companies()
        firebase_companies = self.fetch_firebase_companies()
        
        if not finnhub_companies:
            print("No companies fetched from Finnhub. Exiting.")
            return
        
        if not firebase_companies:
            print("No companies fetched from Firebase. Exiting.")
            return
        
        mappings = self.process_matching(finnhub_companies, firebase_companies)
        
        if not mappings:
            print("No mappings created. Exiting.")
            return
        
        self.save_mappings(mappings)
        
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
    save_to_firebase = config.get('save_to_firebase', False)
    firebase_collection = config.get('firebase_collection', 'ticker_mappings')
    firebase_companies_collection = config.get('firebase_companies_collection', 'companies')
    firebase_name_field = config.get('firebase_name_field', 'name')
    
    if not finnhub_api_key:
        print("Error: finnhub_api_key not found in config.json")
        sys.exit(1)
    
    if not firebase_credentials:
        print("Error: firebase_credentials_path not found in config.json")
        sys.exit(1)
    
    if not os.path.exists(firebase_credentials):
        print(f"Error: Firebase credentials file not found: {firebase_credentials}")
        sys.exit(1)
    
    mapper = TickerMapper(
        finnhub_api_key=finnhub_api_key,
        firebase_credentials_path=firebase_credentials,
        auto_match_threshold=auto_match_threshold,
        firebase_collection=firebase_companies_collection,
        firebase_name_field=firebase_name_field,
        firebase_database_url=firebase_database_url
    )
    
    mapper.run(save_to_firebase=save_to_firebase, firebase_collection=firebase_collection)


if __name__ == "__main__":
    main()

