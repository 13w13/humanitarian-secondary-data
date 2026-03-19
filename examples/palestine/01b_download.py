"""
01b_download.py — Download resources from Palestine catalogue
=============================================================
Reads data/catalogue/ and downloads files to data/raw/.

Usage:
    python -X utf8 01b_download.py              # download all
    python -X utf8 01b_download.py --dry-run    # preview
    python -X utf8 01b_download.py --scan       # show catalogue
"""
import sys, os, subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
DOWNLOAD = os.path.join(REPO, 'scripts', 'download_catalogue.py')

catalogue_dir = os.path.join(HERE, 'data', 'catalogue')
raw_dir = os.path.join(HERE, 'data', 'raw')

cmd = [sys.executable, '-X', 'utf8', DOWNLOAD,
       '--catalogue-dir', catalogue_dir,
       '--output-dir', raw_dir]

# Pass through flags
for flag in ('--dry-run', '--scan', '--source', '--filter'):
    if flag in sys.argv:
        cmd.append(flag)
        idx = sys.argv.index(flag)
        if flag in ('--source', '--filter') and idx + 1 < len(sys.argv):
            cmd.append(sys.argv[idx + 1])

subprocess.run(cmd)
