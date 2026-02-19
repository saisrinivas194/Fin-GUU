"""
Upload companies from a CSV to Firebase Realtime Database.
Use this to populate the 'companies' node so the ticker mapper has something to match against.

CSV should have: id, name  (and optionally slug, or other columns).
Example:
  id,name,slug
  aapl,Apple Inc.,apple-inc
  msft,Microsoft Corporation,microsoft
"""
import json
import os
import sys
import csv
from pathlib import Path

# Reuse same Firebase init as ticker_mapper
def get_db(config: dict):
    import firebase_admin
    from firebase_admin import credentials, db
    cred_path = config['firebase_credentials_path']
    db_url = (config.get('firebase_database_url') or '').strip()
    if not db_url:
        with open(cred_path, 'r') as f:
            cred_data = json.load(f)
        db_url = f"https://{cred_data.get('project_id', '')}-default-rtdb.firebaseio.com"
    if not firebase_admin._apps:
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred, {'databaseURL': db_url})
    return db.reference()


def main():
    config_path = Path('config.json')
    if not config_path.exists():
        print('config.json not found. Create it from config.example.json with your API keys.')
        sys.exit(1)
    with open(config_path, 'r') as f:
        config = json.load(f)
    if not config.get('firebase_credentials_path') or not Path(config['firebase_credentials_path']).exists():
        print('Set firebase_credentials_path in config.json to your service account JSON path.')
        sys.exit(1)

    import argparse
    p = argparse.ArgumentParser(description='Upload companies from CSV to Firebase')
    p.add_argument('csv_file', help='CSV with columns: id, name (and optionally slug, etc.)')
    p.add_argument('--collection', default=None, help='Firebase key (default: from config firebase_companies_collection)')
    p.add_argument('--id-column', default='id', help='Column to use as Firebase node key (default: id). Use slug for exports where slug = Firebase key.')
    p.add_argument('--dry-run', action='store_true', help='Print what would be uploaded, do not write')
    p.add_argument('--resume', action='store_true', help='Skip companies already in Firebase (use after interrupt)')
    p.add_argument('--clear', action='store_true', help='Remove all data in the collection before uploading (fresh seed)')
    args = p.parse_args()

    collection = args.collection or config.get('firebase_companies_collection', 'companies')
    csv_path = Path(args.csv_file)
    if not csv_path.exists():
        print(f'CSV not found: {csv_path}')
        sys.exit(1)

    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row = {k.strip(): (v.strip() if v else '') for k, v in row.items()}
            rows.append(row)

    # Normalize column names (id or ID, name or Name, etc.)
    id_col = (args.id_column or 'id').strip()
    def get_id(r):
        if id_col in r and r[id_col]:
            return r[id_col]
        for key in ('id', 'ID', 'company_id'):
            if key in r and r[key]:
                return r[key]
        return None
    def get_name(r):
        for key in ('name', 'Name', 'company_name'):
            if key in r and r[key]:
                return r[key]
        return None

    to_upload = []
    for r in rows:
        cid = get_id(r)
        name = get_name(r)
        if not cid or not name:
            continue
        to_upload.append({'id': cid, 'name': name, **{k: v for k, v in r.items() if k not in ('id', 'ID', 'company_id')}})

    if not to_upload:
        print('No rows with both id and name found in CSV.')
        sys.exit(1)
    print(f'Found {len(to_upload)} companies in CSV.')
    if args.dry_run:
        for c in to_upload[:5]:
            print(' ', c)
        if len(to_upload) > 5:
            print('  ...')
        print('Run without --dry-run to upload.')
        return

    db = get_db(config)
    ref = db.child(collection)
    if args.clear:
        ref.set({})
        print(f'Cleared Firebase path "{collection}".')
    if args.resume:
        existing = ref.get() or {}
        existing_ids = set(existing.keys()) if isinstance(existing, dict) else set()
        to_upload = [c for c in to_upload if c['id'] not in existing_ids]
        print(f'Resume: {len(existing_ids)} already in Firebase, {len(to_upload)} remaining to upload.')
        if not to_upload:
            print('Nothing left to upload.')
            return
    else:
        print(f'Uploading to Firebase path "{collection}".')

    BATCH_SIZE = 200  # batch writes to avoid slow one-by-one and request timeouts
    total = len(to_upload)
    num_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    print(f'Uploading {total} companies in {num_batches} batches (batch size {BATCH_SIZE})...')
    for i in range(0, total, BATCH_SIZE):
        batch = to_upload[i : i + BATCH_SIZE]
        updates = {}
        for c in batch:
            cid = c['id']
            # Include id, name, slug (and any other columns) in payload
            payload = dict(c)
            updates[cid] = payload
        ref.update(updates)
        done = min(i + BATCH_SIZE, total)
        print(f'  {done} / {total} uploaded')
    print(f'Done. Uploaded {total} companies to "{collection}".')


if __name__ == '__main__':
    main()
