"""
01_fetch.py — List all MSNA datasets (2025)
============================================
Queries IMPACT/REACH for MSNA datasets across all countries,
saves catalogue to data/catalogue/msna_datasets.csv.

Usage:
    python -X utf8 01_fetch.py
    python -X utf8 01_fetch.py --year 2024-2025
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import os, argparse

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, 'scripts'))
sys.path.insert(0, os.path.join(REPO, 'scripts', 'clients'))

from impact_client import IMPACTClient
from download_catalogue import update_inventory

parser = argparse.ArgumentParser()
parser.add_argument('--year', default='2025')
args = parser.parse_args()

data_dir = os.path.join(HERE, 'data')
catalogue_dir = os.path.join(data_dir, 'catalogue')
os.makedirs(catalogue_dir, exist_ok=True)

# Year filter
years = set()
if '-' in args.year:
    y1, y2 = args.year.split('-')
    years = {str(y) for y in range(int(y1), int(y2) + 1)}
else:
    years = {args.year}

# Fetch
client = IMPACTClient()
datasets = client.search_msna_datasets()
filtered = [d for d in datasets
            if d.get('pub_date', '').split() and d['pub_date'].split()[-1] in years]

print('{} MSNA datasets match {}\n'.format(len(filtered), args.year))

# Save
if filtered:
    client.save_csv(filtered, os.path.join(catalogue_dir, 'msna_datasets.csv'))

# Summary
by_country = {}
for d in filtered:
    by_country.setdefault(d.get('country', '?'), []).append(d)
for country in sorted(by_country):
    items = by_country[country]
    print('{} ({})'.format(country, len(items)))
    for d in items:
        print('  [{}] {}'.format(d['pub_date'], d['title'][:65]))
    print()

update_inventory(data_dir)
print('Next: python -X utf8 01b_download.py')
