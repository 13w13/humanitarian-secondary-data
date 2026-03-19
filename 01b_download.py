"""
01b_download.py — Download datasets from catalogue
====================================================
Finds {ISO3}_data/catalogue/ folders and downloads files to {ISO3}_data/raw/.

Usage:
    python -X utf8 01b_download.py                # download all from all countries
    python -X utf8 01b_download.py --select       # pick which datasets to download
    python -X utf8 01b_download.py --dry-run      # preview only
    python -X utf8 01b_download.py --scan         # list available files
    python -X utf8 01b_download.py UKR            # specific country only
"""
import sys
import os
import glob
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD = os.path.join(HERE, 'scripts', 'download_catalogue.py')

# Find country data dirs
iso3_filter = None
passthrough = []
for arg in sys.argv[1:]:
    if arg.isalpha() and len(arg) == 3 and not arg.startswith('-'):
        iso3_filter = arg.upper()
    else:
        passthrough.append(arg)

# Find *_data/catalogue/ folders
pattern = os.path.join(HERE, '*_data', 'catalogue')
catalogue_dirs = sorted(glob.glob(pattern))

if iso3_filter:
    target = os.path.join(HERE, '{}_data'.format(iso3_filter), 'catalogue')
    catalogue_dirs = [target] if os.path.isdir(target) else []

if not catalogue_dirs:
    print('No catalogue folders found. Run 01_fetch.py first.')
    sys.exit(1)

for cat_dir in catalogue_dirs:
    base = os.path.dirname(cat_dir)
    raw_dir = os.path.join(base, 'raw')
    country = os.path.basename(base).replace('_data', '')
    print('--- {} ---'.format(country))
    cmd = [sys.executable, '-X', 'utf8', DOWNLOAD,
           '--catalogue-dir', cat_dir, '--output-dir', raw_dir] + passthrough
    subprocess.run(cmd)
    print()
