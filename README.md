# Ticker Mapper

Matches companies from Finnhub with Firebase companies using fuzzy matching. Creates ticker to company ID mappings.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create config.json from config.example.json and add your API keys

3. Get Finnhub API key from finnhub.io

4. Get Firebase credentials from Firebase Console

## Usage

Run the ticker mapper:
```bash
python ticker_mapper.py
```

For exact matches, it saves automatically. For fuzzy matches above the threshold (default 90%), it auto-matches. Otherwise it prompts you to select from close matches.

### Features

- **Exact matching**: Automatically matches companies with identical names (case-insensitive)
- **Auto fuzzy matching**: Automatically matches companies above the confidence threshold (default 90%)
- **Manual selection**: Prompts you to select from top 5 matches when confidence is below threshold
- **Resume capability**: If interrupted, you can resume by running again - it will skip already mapped tickers
- **Progress saving**: Saves mappings incrementally as you process companies (can be disabled)

### Configuration

Edit `config.json` to customize:
- `auto_match_threshold`: Confidence score (0-100) for auto-matching (default: 90)
- `resume`: Whether to resume from existing mappings file (default: true)
- `save_progress`: Whether to save mappings after each match (default: true)
- `output_file`: Where to save the mappings (default: ticker_mappings.json)
- `save_to_firebase`: Whether to also save mappings to Firebase (default: false)

Output is saved to `ticker_mappings.json` (or your configured output file)

## Crosswalk Updater

Merge new companies from Firebase into an existing crosswalk:

```bash
python crosswalk_updater.py --crosswalk old.csv --new-csv firebase_export.csv --output merged.csv
```

The Firebase export should have: name, slug, id

Only parent companies get tickers. Subsidiaries always have empty ticker field.
