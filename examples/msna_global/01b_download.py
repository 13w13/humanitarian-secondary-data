"""
01b_download.py — Download MSNA datasets from catalogue
========================================================
Reads catalogue/msna_datasets.csv, downloads each file to data/raw/.

Usage:
    python -X utf8 01b_download.py              # download all
    python -X utf8 01b_download.py --dry-run    # preview
    python -X utf8 01b_download.py --scan       # show catalogue
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import os

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, 'scripts'))

from download_catalogue import download_from_catalogue, scan_catalogue, update_inventory

data_dir = os.path.join(HERE, 'data')
catalogue_dir = os.path.join(data_dir, 'catalogue')
raw_dir = os.path.join(data_dir, 'raw')

dry_run = '--dry-run' in sys.argv
scan_only = '--scan' in sys.argv

if scan_only:
    resources = scan_catalogue(catalogue_dir)
    dl = sum(1 for r in resources if r['downloadable'])
    print('{} resources ({} downloadable)'.format(len(resources), dl))
    for r in resources:
        if r['downloadable']:
            print('  {}'.format(r['url'].split('/')[-1][:60]))
else:
    counts = download_from_catalogue(
        catalogue_dir, raw_dir, dry_run=dry_run, data_dir=data_dir)
    if not dry_run:
        update_inventory(data_dir)
