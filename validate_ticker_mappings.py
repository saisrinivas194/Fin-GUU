#!/usr/bin/env python3
"""
Validate ticker_mappings.json against a master list of expected ticker -> company_id.
Usage:
  python3 validate_ticker_mappings.py --mappings ticker_mappings.json --master master_list.csv
Master list CSV: ticker, expected_company_id  (or company_id). Optional header.
"""
import argparse
import csv
import json
import sys
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description='Validate ticker mappings against a master list')
    p.add_argument('--mappings', default='ticker_mappings.json', help='Path to ticker_mappings.json')
    p.add_argument('--master', required=True, help='Path to master list CSV (ticker, expected_company_id)')
    args = p.parse_args()

    mappings_path = Path(args.mappings)
    master_path = Path(args.master)
    if not mappings_path.exists():
        print(f'Error: {mappings_path} not found.')
        sys.exit(1)
    if not master_path.exists():
        print(f'Error: {master_path} not found.')
        sys.exit(1)

    with open(mappings_path, 'r', encoding='utf-8') as f:
        mappings = json.load(f)
    if not isinstance(mappings, dict):
        mappings = {}

    # Load master list: ticker -> expected company_id
    expected = {}
    with open(master_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row = {k.strip().lower(): v.strip() if v else '' for k, v in row.items()}
            ticker = row.get('ticker') or row.get('symbol')
            cid = row.get('expected_company_id') or row.get('company_id') or row.get('companyid')
            if ticker and cid:
                expected[ticker.upper()] = cid

    if not expected:
        print('No rows found in master list (need columns: ticker, expected_company_id or company_id).')
        sys.exit(1)

    correct = 0
    wrong = []
    missing = []
    for ticker, exp_cid in expected.items():
        got = mappings.get(ticker) or mappings.get(ticker.upper())
        if got is None:
            missing.append(ticker)
        elif str(got).strip().lower() == str(exp_cid).strip().lower():
            correct += 1
        else:
            wrong.append((ticker, exp_cid, got))

    total = len(expected)
    accuracy = (correct / total * 100) if total else 0
    print(f'Master list: {total} tickers')
    print(f'Correct:     {correct}')
    print(f'Wrong:       {len(wrong)}')
    print(f'Missing:     {len(missing)} (in master but not in mappings)')
    print(f'Accuracy:    {accuracy:.1f}%')
    if wrong:
        print('\nWrong mappings (ticker | expected | got):')
        for t, exp, got in wrong[:20]:
            print(f'  {t}  expected {exp}  got {got}')
        if len(wrong) > 20:
            print(f'  ... and {len(wrong) - 20} more')
    if missing and len(missing) <= 30:
        print('\nMissing in mappings:', ', '.join(missing))
    elif missing:
        print(f'\nMissing in mappings: {len(missing)} tickers (first 30: {", ".join(missing[:30])} ...)')
    sys.exit(0 if not wrong else 1)


if __name__ == '__main__':
    main()
