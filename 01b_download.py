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
    python -X utf8 01b_download.py --catalogue-dir SDN_data/catalogue --output-dir SDN_data/raw

--source, --filter, --select, --dry-run and --scan are passed to
scripts/download_catalogue.py. Exit status is the worst of the runs: 0 all good,
1 a download failed, 2 bad arguments or no catalogue.
"""
import argparse
import glob
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD = os.path.join(HERE, 'scripts', 'download_catalogue.py')
sys.path.insert(0, os.path.join(HERE, 'scripts', 'clients'))

parser = argparse.ArgumentParser(
    description='Download datasets listed in {ISO3}_data/catalogue/')
parser.add_argument('iso3', nargs='?', help='only this country (e.g. SDN)')
parser.add_argument('--catalogue-dir', help='one explicit catalogue folder')
parser.add_argument('--output-dir', help='download target for --catalogue-dir')
# Declared here (not guessed) so that an option's VALUE is never mistaken for a
# country: any 3-letter word used to become the country filter ("--filter idp").
parser.add_argument('--source', help='catalogue file prefix (impact, dtm...)')
parser.add_argument('--filter', dest='filter_text', help='keyword in title/URL')
parser.add_argument('--select', action='store_true', help='pick interactively')
parser.add_argument('--dry-run', action='store_true', help='list, do not download')
parser.add_argument('--scan', action='store_true', help='report what is available')
args = parser.parse_args()
passthrough = []
if args.source:
    passthrough += ['--source', args.source]
if args.filter_text:
    passthrough += ['--filter', args.filter_text]
passthrough += [flag for flag, on in (('--select', args.select),
                                      ('--dry-run', args.dry_run),
                                      ('--scan', args.scan)) if on]

if args.catalogue_dir or args.output_dir:
    # Explicit folders: exactly ONE run. They used to be passed along to every
    # {ISO3}_data folder, so one country's files landed in every other raw/.
    if not args.catalogue_dir:
        parser.error('--output-dir needs --catalogue-dir')
    out = args.output_dir or os.path.join(os.path.dirname(
        os.path.abspath(args.catalogue_dir.rstrip('/\\'))), 'raw')
    runs = [(args.catalogue_dir, out, None)]
else:
    if args.iso3:
        from config import normalize_iso3
        try:
            iso3 = normalize_iso3(args.iso3)
        except ValueError as e:
            parser.error(str(e))
        target = os.path.join(HERE, '{}_data'.format(iso3), 'catalogue')
        catalogue_dirs = [target] if os.path.isdir(target) else []
    else:
        catalogue_dirs = sorted(glob.glob(os.path.join(HERE, '*_data', 'catalogue')))
    runs = [(d, os.path.join(os.path.dirname(d), 'raw'),
             os.path.basename(os.path.dirname(d)).replace('_data', ''))
            for d in catalogue_dirs]

if not runs:
    print('No catalogue folders found. Run 01_fetch.py first.')
    sys.exit(2)

worst = 0
for cat_dir, raw_dir, country in runs:
    if country:
        print('--- {} ---'.format(country))
    cmd = [sys.executable, '-X', 'utf8', DOWNLOAD,
           '--catalogue-dir', cat_dir, '--output-dir', raw_dir] + passthrough
    worst = max(worst, subprocess.run(cmd).returncode)
    print()
sys.exit(worst)
