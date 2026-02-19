#!/usr/bin/env python3
"""
Fetch company profile from Finnhub by symbol, ISIN, or CUSIP.

Usage:
  python3 finnhub_profile.py --symbol AAPL
  python3 finnhub_profile.py --symbol IBM
  python3 finnhub_profile.py --isin US5949181045
  python3 finnhub_profile.py --cusip 023135106

Uses finnhub_api_key from config.json.
"""
import json
import os
import sys
from pathlib import Path


def load_config():
    config_path = Path(__file__).parent / "config.json"
    if not config_path.exists():
        print("config.json not found. Create it from config.example.json with finnhub_api_key.", file=sys.stderr)
        sys.exit(1)
    with open(config_path, "r") as f:
        return json.load(f)


def fetch_profile(api_key: str, *, symbol: str = None, isin: str = None, cusip: str = None) -> dict:
    """GET /stock/profile with one of symbol, isin, or cusip."""
    import requests
    base = "https://finnhub.io/api/v1/stock/profile"
    params = {"token": api_key}
    if symbol:
        params["symbol"] = symbol
    elif isin:
        params["isin"] = isin
    elif cusip:
        params["cusip"] = cusip
    else:
        raise ValueError("Provide exactly one of: symbol, isin, cusip")
    resp = requests.get(base, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def main():
    import argparse
    p = argparse.ArgumentParser(description="Fetch Finnhub stock profile by symbol, ISIN, or CUSIP")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--symbol", metavar="SYMBOL", help="Stock ticker (e.g. AAPL, IBM)")
    g.add_argument("--isin", metavar="ISIN", help="ISIN (e.g. US5949181045)")
    g.add_argument("--cusip", metavar="CUSIP", help="CUSIP (e.g. 023135106)")
    p.add_argument("--raw", action="store_true", help="Print raw JSON only")
    args = p.parse_args()

    config = load_config()
    api_key = config.get("finnhub_api_key")
    if not api_key:
        print("finnhub_api_key not found in config.json", file=sys.stderr)
        sys.exit(1)

    try:
        data = fetch_profile(
            api_key,
            symbol=args.symbol,
            isin=args.isin,
            cusip=args.cusip,
        )
    except Exception as e:
        print(f"Request failed: {e}", file=sys.stderr)
        sys.exit(1)

    if args.raw:
        print(json.dumps(data, indent=2))
        return

    if not data or (isinstance(data, dict) and data.get("name") is None and not data):
        print("No profile found for the given identifier.")
        return

    # Pretty summary
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
