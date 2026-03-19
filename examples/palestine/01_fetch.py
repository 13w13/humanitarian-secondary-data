"""
01_fetch.py — Fetch all sources for Palestine
==============================================
Calls fetch_country_data.py for PSE. Analytical data goes to
data/raw/, catalogue listings go to data/catalogue/.

Usage:
    python -X utf8 01_fetch.py                    # all sources
    python -X utf8 01_fetch.py --only impact      # IMPACT only
    python -X utf8 01_fetch.py --only acled,hapi  # specific sources
"""
import sys, os, subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
FETCH = os.path.join(REPO, 'scripts', 'fetch_country_data.py')

raw_dir = os.path.join(HERE, 'data', 'raw')

cmd = [sys.executable, '-X', 'utf8', FETCH, 'PSE', '--output-dir', raw_dir]

# Pass through --only if provided
if '--only' in sys.argv:
    idx = sys.argv.index('--only')
    cmd += ['--only', sys.argv[idx + 1]]

subprocess.run(cmd)
