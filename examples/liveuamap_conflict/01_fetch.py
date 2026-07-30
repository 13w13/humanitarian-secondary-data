"""
01_fetch.py — Fetch conflict events + MSNA datasets for a country
==================================================================
Calls fetch_country_data.py with --only impact,liveuamap.
Shows how to combine multiple sources in a single fetch.

Usage:
    python -X utf8 01_fetch.py                                # Ukraine, defaults
    python -X utf8 01_fetch.py --iso3 SDN                     # Sudan
    python -X utf8 01_fetch.py --iso3 UKR --date-from 2026-01-01 --max-pages 50
    python -X utf8 01_fetch.py --iso3 IRN --date-from 2026-03-15 --date-to 2026-03-18
"""
import sys
import os
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
FETCH = os.path.join(REPO, 'scripts', 'fetch_country_data.py')

# Defaults
iso3 = 'UKR'
date_from = None
date_to = None
max_pages = None

# Parse args
args = sys.argv[1:]
i = 0
while i < len(args):
    if args[i] == '--iso3' and i + 1 < len(args):
        iso3 = args[i + 1].upper()
        i += 2
    elif args[i] == '--date-from' and i + 1 < len(args):
        date_from = args[i + 1]
        i += 2
    elif args[i] == '--date-to' and i + 1 < len(args):
        date_to = args[i + 1]
        i += 2
    elif args[i] == '--max-pages' and i + 1 < len(args):
        max_pages = args[i + 1]
        i += 2
    else:
        i += 1

# Build command — fetch IMPACT catalogue + Liveuamap events
raw_dir = os.path.join(HERE, 'data')
cmd = [sys.executable, '-X', 'utf8', FETCH, iso3,
       '--only', 'impact,liveuamap', '--output-dir', raw_dir]

if date_from:
    cmd += ['--date-from', date_from]
if date_to:
    cmd += ['--date-to', date_to]
if max_pages:
    cmd += ['--max-pages', max_pages]

subprocess.run(cmd)
