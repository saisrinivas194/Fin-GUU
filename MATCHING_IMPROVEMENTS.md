# Ticker Matching: Problems & Solutions (Summary for Calls)

## Problem 1: False positives from common words

**Issue:** Words like Holdings, Group, Capital, Inc. were inflating fuzzy scores so different companies matched.

**Solution (implemented):**
- **Core-name normalization:** We strip designator/suffix words (Inc, Corp, REIT, LLC, Holdings, Group, International, Industries, etc.) before fuzzy matching so the algorithm focuses on the **core** company name.
- **Conservative thresholds:** `auto_match_threshold` (default 90%) and `min_prompt_confidence` (default 70%). Anything doubtful goes to manual review instead of auto-mapping.

---

## Problem 2: Acronym mismatches

**Example:** BOA vs Bank of America.

**Solution (implemented):**
- Optional **acronym expansion** in `config.json`: `"acronym_expansions": {"BOA": "Bank of America", ...}`.
- Exact and fuzzy matching use this map so acronyms match the corresponding full name in Firebase.

---

## Problem 3: One-to-many mapping risk

**Issue:** One company ID incorrectly mapped to multiple tickers.

**Solution (implemented):**
- **One-to-many safeguard:** If the best auto-match company is already mapped to another ticker, we do **not** auto-save; we prompt for manual choice (or skip).
- Edge cases go to manual review instead of auto-saving.

---

## Problem 4: No validation benchmark

**Issue:** No way to measure accuracy before.

**Solution (implemented):**
- **Validation script:** `python3 validate_ticker_mappings.py --master master_list.csv`
- Master list CSV: `ticker`, `expected_company_id`. Script compares with `ticker_mappings.json` and reports correct / wrong / missing and **accuracy %**.
- Use this to fine-tune thresholds against the master list instead of guessing.

---

## Problem 5: Sector and fund vs company false positives

**Issue:** Fuzzy matching alone treated “INTERNATIONAL GOLD RESOURCES” (IGRU) as International Paper (shared “International”), and “BLACKROCK INCOME TRUST” as BlackRock (shared brand).

**Solution (implemented):**
- **Sector-conflict rule:** We keep a set of sector/identity words (e.g. gold, silver, mining, resources, energy, bank, paper, trust, fund). If the API name and a candidate have *different* sector words (e.g. gold/resources vs paper), the candidate is rejected and logged as `rejected_sector_conflict`.
- **Fund/product vs company:** If the API name contains fund indicators (Trust, Income, Fund, PLC) and the candidate is a shorter “parent” name (e.g. BlackRock), we treat it as fund ≠ company and reject (`rejected_fund_vs_company`).
- **Suspicious generic match:** If the ticker is short (≤4 chars) and the core name is only generic words (international, global, world) plus designators, we skip and do not auto-match (`suspicious_generic_match`).
- All such rejections are written to the match log with a clear `match_type` / `notes` for tuning and auditing.
- **Hard negative pairs** are **optional** and **config-driven** (`config.json` → `hard_negative_pairs`). They are for known exceptions / regression only. The main matching mechanism is normalization + sector groups + generic overlap rules + fuzzy scoring; we do not rely on a large hard-coded list of company names in code.

---

## Problem 6: Manual process was painful

**Goal:** Make the system conservative and stable so manual review is only for edge cases; minimal rework once finalized.

**Approach:**
- Normalization + conservative thresholds + one-to-many rule + sector/fund/generic rules + match log for auditing.
- Manual review only where confidence is in the middle band or one-to-many triggers.

---

## Short version for a call

> The main issue was false positives from common words in company names. We improved normalization so matching focuses on the core name, kept thresholds conservative, and send edge cases to manual review. We added acronym expansion for cases like BOA vs Bank of America, and a one-to-one safeguard so we don’t auto-map multiple tickers to the same company. We also added sector and fund-vs-company rules so “International Gold Resources” never matches “International Paper” and “BlackRock Income Trust” never matches “BlackRock”. We now validate everything against a master list so we can measure accuracy and tune thresholds instead of fixing things by hand later.
