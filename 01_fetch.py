"""
01_fetch.py — Fetch humanitarian secondary data for a country
==============================================================
Queries multiple APIs and writes CSVs to data/{ISO3}/.

Usage:
    python -X utf8 01_fetch.py UKR                                    # all sources
    python -X utf8 01_fetch.py UKR --only impact,liveuamap            # specific sources
    python -X utf8 01_fetch.py SDN --date-from 2025-01-01             # with date filter
    python -X utf8 01_fetch.py SYR --only liveuamap --max-pages 50    # limit pagination
    python -X utf8 01_fetch.py IRN --date-from 2026-03-01 --date-to 2026-03-31

Next step: python -X utf8 01b_download.py --select
"""
import sys
import os
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
FETCH = os.path.join(HERE, 'scripts', 'fetch_country_data.py')

# Pass all arguments through to fetch_country_data.py
subprocess.run([sys.executable, '-X', 'utf8', FETCH] + sys.argv[1:])
