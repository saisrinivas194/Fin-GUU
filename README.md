# Ticker Mapper

Semi-automated **ticker → company ID** mapping for Goods Unite Us. Uses the Finnhub API for publicly traded companies and stock tickers, and matches them to company names stored in **Firebase Realtime Database** (Goods’ list). Exact matches are saved automatically; for the rest, fuzzy matching and an optional confidence threshold reduce how often you have to choose; when needed, the script prompts you to pick the correct company, then saves the mapping so it never asks again for that ticker.

**Firebase:** This project uses **Realtime Database only** (not Firestore). Config requires `firebase_database_url` and `firebase_credentials_path` (service account JSON).

## Requirements (as specified)

| Requirement | Implementation |
|-------------|----------------|
| Use Finnhub API for list of publicly traded companies + stock tickers | ✓ `fetch_finnhub_companies()` — US exchange symbols + descriptions |
| Match with company names in Goods Unite Us (Firebase) | ✓ Companies read from Firebase Realtime DB path `companies` (configurable) |
| Stock tickers as key between Goods names and Finnhub | ✓ Output is ticker → company ID mapping (JSON + optional write to Realtime DB) |
| Fuzzy matching + confidence for non-exact | ✓ `rapidfuzz` token_sort_ratio; `auto_match_threshold` (default 90%) auto-accepts high-confidence matches |
| Exact match → save ticker/company ID mapping automatically | ✓ Exact match (case-insensitive) → save immediately, no prompt |
| No exact match → show similar Firebase names, user selects one | ✓ Top 5 fuzzy matches shown; user picks by number or skips |
| Save ticker/ID after selection so we don’t ask again | ✓ Mappings saved to JSON after each match; `resume` skips already-mapped tickers |
| Script run manually | ✓ Run `python ticker_mapper.py`; interactive prompts when below threshold |
| Firebase = Realtime Database (not Firestore) | ✓ Uses `firebase_admin` `credentials` + `db` (Realtime Database); no Firestore |

## Setup

1. Install dependencies (use `python3 -m pip` if `pip` is not found):
```bash
python3 -m pip install -r requirements.txt
```

2. Create `config.json` from `config.example.json` and add:
   - **finnhub_api_key** — from finnhub.io
   - **firebase_credentials_path** — path to your Firebase service account JSON
   - **firebase_database_url** — your Realtime Database URL (e.g. `https://PROJECT-default-rtdb.firebaseio.com`)

3. **Seed Firebase with companies** (required before first run). The mapper reads company names from the `companies` node; if it’s empty you’ll see “Fetched 0 companies from Firebase. Exiting.” Upload from a CSV:
   ```bash
   python3 seed_firebase_companies.py companies.csv
   ```
   CSV must have columns `id` and `name` (optional: `slug`). A sample is in `companies.example.csv` — copy and edit it, or use it to test:
   ```bash
   python3 seed_firebase_companies.py companies.example.csv
   ```

## What to upload where (by task)

| Task | What to upload | Where | How |
|------|----------------|-------|-----|
| **Give the mapper your company list** (Goods Unite Us / Index Align names) | Your list of companies: **id** + **name** (optional: slug) | **Firebase Realtime Database** → `companies` node | Use a CSV and run: `python3 seed_firebase_companies.py <yourfile.csv>`. Use a real filename—e.g. `companies.example.csv` to test, or `my_companies.csv` with your data. Or add records manually under `companies` in Firebase Console. |
| **Get tickers from Finnhub** | Nothing | — | The mapper calls the Finnhub API automatically (no upload). |
| **Save ticker → company ID mappings** | Nothing to upload | **Local file** `ticker_mappings.json` (and optionally Firebase) | After you run `python3 ticker_mapper.py`, mappings are saved to `ticker_mappings.json`. To also write them to Firebase, set `"save_to_firebase": true` in `config.json`. |
| **Share the completed mapping with Nik / team** | The file `ticker_mappings.json` | Email, Drive, etc. | Use the generated `ticker_mappings.json` (ticker → company ID). No upload into this repo—just share the file. |

**CSV for seeding companies** must have at least two columns (header row optional but recommended):

- **id** — your company ID (e.g. `aapl`, or your internal ID)
- **name** — company name as it appears in Goods Unite Us / Index Align (used for matching)

Example:

```csv
id,name,slug
aapl,Apple Inc.,apple-inc
msft,Microsoft Corporation,microsoft
```

## Usage

Run the ticker mapper:
```bash
python ticker_mapper.py
```

For exact matches, it saves automatically. For fuzzy matches above the threshold (default 90%), it auto-matches. Otherwise it prompts you to select from close matches.

### Features

- **Exact matching**: Automatically matches companies with identical names (case-insensitive)
- **Core-name normalization**: Strips designators (Inc, REIT, Corp, etc.) so fuzzy matching emphasizes the core company name and avoids designator-driven false matches
- **Auto fuzzy matching**: Automatically matches when confidence ≥ threshold (default 90%), unless blocked by safeguards
- **Self-match prevention**: Never saves a mapping when the company ID equals the ticker (e.g. AAPL → "appl")
- **One-to-many safeguard**: If the best match is already mapped to another ticker, the script does not auto-match; it prompts you so you can choose or skip
- **Index filtering**: Skips Finnhub symbols whose `type` is in a skip list (e.g. index, idx) when the API returns it
- **Manual selection**: Prompts when best match is between min_prompt_confidence and threshold, or when one-to-many triggers
- **Match log**: Writes each decision (ticker, names, scores, match type) to a CSV for auditing and tuning
- **Resume capability**: Skips already mapped tickers; progress saved after each match

### Configuration

Edit `config.json` to customize:
- `auto_match_threshold`: Confidence score (0-100) for auto-matching (default: 90)
- `min_prompt_confidence`: Only prompt when best match is at least this % (default: 70); below this, skip without asking
- `match_log_file`: Path for the match log CSV (default: ticker_match_log.csv); set to `""` to disable
- `skip_symbol_types`: Optional list of Finnhub symbol types to skip (e.g. `["index", "etf"]`); when the API returns `type`, matching symbols are excluded
- `acronym_expansions`: Optional map for acronym matching (e.g. `{"BOA": "Bank of America"}`) so names like BOA match the full company name in Firebase
- `resume`, `save_progress`, `output_file`, `save_to_firebase`: as before

Output is saved to `ticker_mappings.json` (or your configured output file).

### Validate against a master list

After building mappings, compare to a benchmark and measure accuracy:

```bash
python3 validate_ticker_mappings.py --mappings ticker_mappings.json --master master_list.csv
```

Master list CSV should have columns `ticker` and `expected_company_id` (or `company_id`). The script prints correct count, wrong mappings, missing tickers, and **accuracy %** so you can tune thresholds.

### Finnhub stock profile (symbol / ISIN / CUSIP)

Look up a company profile by ticker symbol, ISIN, or CUSIP (uses `config.json`):

```bash
python3 finnhub_profile.py --symbol AAPL
python3 finnhub_profile.py --symbol IBM
python3 finnhub_profile.py --isin US5949181045
python3 finnhub_profile.py --cusip 023135106
```

Add `--raw` to print only the JSON response.
