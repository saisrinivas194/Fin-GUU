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

Output is saved to ticker_mappings.json

## Crosswalk Updater

Merge new companies from Firebase into an existing crosswalk:

```bash
python crosswalk_updater.py --crosswalk old.csv --new-csv firebase_export.csv --output merged.csv
```

The Firebase export should have: name, slug, id

Only parent companies get tickers. Subsidiaries always have empty ticker field.
