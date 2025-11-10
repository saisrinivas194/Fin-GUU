import csv
import json
import sys
from typing import Dict, List, Set, Tuple
from pathlib import Path


class CrosswalkUpdater:
    def __init__(self):
        pass
    
    def load_existing_crosswalk(self, file_path: str) -> List[Dict]:
        """Load existing crosswalk from CSV or JSON."""
        file_path = Path(file_path)
        
        if not file_path.exists():
            print(f"Warning: Crosswalk file not found: {file_path}")
            return []
        
        if file_path.suffix.lower() == '.json':
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            crosswalk = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    normalized_row = {}
                    for key, value in row.items():
                        key_lower = key.lower().strip()
                        if 'name' in key_lower:
                            normalized_row['name'] = value.strip()
                        elif 'slug' in key_lower:
                            normalized_row['slug'] = value.strip()
                        elif 'ticker' in key_lower:
                            normalized_row['Ticker'] = value.strip()
                        elif 'parent' in key_lower:
                            normalized_row['Parent'] = value.strip()
                        elif 'accurate' in key_lower or 'checked' in key_lower:
                            normalized_row['Accurate/Checked'] = value.strip()
                        elif key_lower == 'id':
                            normalized_row['id'] = value.strip()
                        else:
                            normalized_row[key] = value.strip()
                    crosswalk.append(normalized_row)
            return crosswalk
    
    def load_new_companies_csv(self, file_path: str) -> List[Dict]:
        """Load new companies from CSV file."""
        file_path = Path(file_path)
        
        if not file_path.exists():
            print(f"Error: CSV file not found: {file_path}")
            sys.exit(1)
        
        companies = []
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cleaned_row = {k.strip(): v.strip() if v else '' for k, v in row.items()}
                companies.append(cleaned_row)
        
        return companies
    
    def normalize_name(self, name: str) -> str:
        """Normalize company name for comparison."""
        if not name:
            return ""
        return name.lower().strip()
    
    def _build_name_index(self, crosswalk: List[Dict]) -> Tuple[Set[str], Dict[str, Dict]]:
        """Build name set and mapping in one pass."""
        names = set()
        name_map = {}
        for row in crosswalk:
            name = row.get('name', '').strip()
            if name:
                normalized = self.normalize_name(name)
                names.add(normalized)
                name_map[normalized] = row
        return names, name_map
    
    def is_parent_company(self, company: Dict) -> bool:
        """Check if a company is a parent company."""
        parent = company.get('Parent', '').strip()
        return not parent or parent.lower() in ['', 'none', 'null', 'n/a']
    
    def merge_crosswalk(self, existing_crosswalk: List[Dict], 
                       new_companies: List[Dict], preserve_ids: bool = True) -> List[Dict]:
        """Merge new companies into existing crosswalk."""
        merged = existing_crosswalk.copy()
        existing_names, existing_by_name = self._build_name_index(existing_crosswalk)
        
        added_count = 0
        updated_count = 0
        skipped_count = 0
        
        print(f"\nExisting crosswalk has {len(existing_crosswalk)} companies")
        print(f"New CSV has {len(new_companies)} companies")
        print("\nProcessing new companies...\n")
        
        for company in new_companies:
            name = company.get('name', '').strip()
            if not name:
                continue
            
            normalized_name = self.normalize_name(name)
            company_id = company.get('id', '').strip()
            company_slug = company.get('slug', '').strip()
            
            if normalized_name in existing_names:
                if preserve_ids and company_id:
                    existing_entry = existing_by_name[normalized_name]
                    if not existing_entry.get('id'):
                        existing_entry['id'] = company_id
                        updated_count += 1
                        print(f"  Updated: {name} (added id)")
                    else:
                        skipped_count += 1
                else:
                    skipped_count += 1
                continue
            
            is_parent = self.is_parent_company(company)
            new_entry = {
                'name': name,
                'slug': company_slug or self.generate_slug(name),
                'Ticker': '',
                'Parent': company.get('Parent', '').strip(),
                'Accurate/Checked': company.get('Accurate/Checked', '').strip()
            }
            
            if company_id:
                new_entry['id'] = company_id
            
            merged.append(new_entry)
            existing_names.add(normalized_name)
            existing_by_name[normalized_name] = new_entry
            added_count += 1
            print(f"  Added: {name} {'(parent)' if is_parent else '(subsidiary)'}")
        
        print(f"\n{'='*60}")
        print(f"Summary:")
        print(f"  Added: {added_count} new companies")
        print(f"  Updated: {updated_count} companies (added id)")
        print(f"  Skipped: {skipped_count} (already in crosswalk)")
        print(f"  Total in merged crosswalk: {len(merged)}")
        print(f"{'='*60}\n")
        
        return merged
    
    def generate_slug(self, name: str) -> str:
        """Generate a URL-friendly slug from company name."""
        if not name:
            return ""
        slug = ''.join(c if c.isalnum() or c == ' ' else '' for c in name.lower())
        return '-'.join(slug.split())
    
    def save_crosswalk(self, crosswalk: List[Dict], output_path: str, format: str = 'csv'):
        """Save crosswalk to file (CSV or JSON)."""
        output_path = Path(output_path)
        
        if format.lower() == 'json':
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(crosswalk, f, indent=2, ensure_ascii=False)
        else:
            if not crosswalk:
                print("Warning: No data to save")
                return
            
            fieldnames = set()
            for row in crosswalk:
                fieldnames.update(row.keys())
            
            standard_fields = ['name', 'slug', 'id', 'Ticker', 'Parent', 'Accurate/Checked']
            fieldnames = [f for f in standard_fields if f in fieldnames] + \
                        sorted([f for f in fieldnames if f not in standard_fields])
            
            with open(output_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(crosswalk)
        
        print(f"Saved {len(crosswalk)} companies to {output_path}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Merge new company data into existing crosswalk'
    )
    parser.add_argument(
        '--crosswalk',
        required=True,
        help='Path to existing crosswalk file (CSV or JSON)'
    )
    parser.add_argument(
        '--new-csv',
        required=True,
        help='Path to CSV file with new company data'
    )
    parser.add_argument(
        '--output',
        required=True,
        help='Path to output file for merged crosswalk'
    )
    parser.add_argument(
        '--format',
        choices=['csv', 'json'],
        default='csv',
        help='Output format (default: csv)'
    )
    parser.add_argument(
        '--no-preserve-ids',
        action='store_true',
        help='Do not preserve/update id fields from new companies'
    )
    
    args = parser.parse_args()
    
    updater = CrosswalkUpdater()
    
    print(f"Loading existing crosswalk from {args.crosswalk}...")
    existing_crosswalk = updater.load_existing_crosswalk(args.crosswalk)
    
    print(f"Loading new companies from {args.new_csv}...")
    new_companies = updater.load_new_companies_csv(args.new_csv)
    
    merged_crosswalk = updater.merge_crosswalk(
        existing_crosswalk, 
        new_companies, 
        preserve_ids=not args.no_preserve_ids
    )
    
    print(f"Saving merged crosswalk to {args.output}...")
    updater.save_crosswalk(merged_crosswalk, args.output, format=args.format)
    
    print("Done!")


if __name__ == "__main__":
    main()

