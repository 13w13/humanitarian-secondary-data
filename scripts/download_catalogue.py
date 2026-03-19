"""
Download Catalogue Resources
=============================
Reads CSVs from data/catalogue/, identifies downloadable URLs,
and downloads selected files to data/downloads/.

Works with catalogue files produced by fetch_country_data.py:
- impact_all_resources.csv    → direct XLSX/PDF links (repository.impact-initiatives.org)
- impact_msna_datasets.csv    → direct XLSX links
- dtm_datasets.csv            → HDX resource URLs
- dtm_portal_datasets.csv     → portal page URLs (not direct download)
- hdx_all_datasets.csv        → HDX dataset pages (use hdx_ckan_client to resolve)
- reliefweb_sitreps.csv       → report pages (not downloadable data)
- reliefweb_facets.csv         → facet counts (no URLs)

Currently supports direct-download URLs only (IMPACT, DTM resources).
HDX datasets require a separate resolution step via CKAN API (future).

Usage (standalone):
    python -X utf8 download_catalogue.py --catalogue-dir data/catalogue/ --output-dir data/downloads/
    python -X utf8 download_catalogue.py --catalogue-dir data/catalogue/ --source impact --dry-run
    python -X utf8 download_catalogue.py --catalogue-dir data/catalogue/ --filter msna --output-dir data/downloads/msna/

Usage (as library):
    from download_catalogue import download_from_catalogue
    results = download_from_catalogue('data/catalogue/', 'data/downloads/')

License: MIT
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import csv
import os
import re
import time
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# Add clients/ to path for config import
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'clients'))

try:
    from config import USER_AGENT, DEFAULT_TIMEOUT
except ImportError:
    USER_AGENT = 'humanitarian-secondary-data/1.0'
    DEFAULT_TIMEOUT = 60

# --- URL column detection ---

# Known URL columns per catalogue source
URL_COLUMNS = {
    'impact_all_resources.csv': 'url',
    'impact_msna_datasets.csv': 'url',
    'dtm_datasets.csv': 'resource_url',
    'reliefweb_disability.csv': 'url',
}

# Domains where URLs point to actual downloadable files
DOWNLOADABLE_DOMAINS = [
    'repository.impact-initiatives.org',
    'data.humdata.org/dataset/',       # HDX resource direct links
]

# Domains that are listing pages, not direct downloads
LISTING_DOMAINS = [
    'data.humdata.org/dataset',        # HDX dataset pages (need CKAN API)
    'reliefweb.int',                   # Report pages
    'dtm.iom.int',                     # Portal pages
]


def _detect_url_column(header):
    """Find the URL column in a CSV header."""
    for col in ['url', 'resource_url', 'hdx_url', 'download_url']:
        if col in header:
            return col
    return None


def _is_downloadable(url):
    """Check if a URL points to a direct file download."""
    if not url:
        return False
    for domain in DOWNLOADABLE_DOMAINS:
        if domain in url:
            return True
    # Heuristic: URL ends with a file extension
    path = url.split('?')[0].split('#')[0]
    ext = path.rsplit('.', 1)[-1].lower() if '.' in path else ''
    return ext in ('xlsx', 'xls', 'csv', 'zip', 'pdf', 'json', 'geojson')


def _safe_filename(url):
    """Extract filename from URL."""
    from urllib.parse import unquote
    fname = unquote(url.split('/')[-1].split('?')[0])
    fname = re.sub(r'[<>:"/\\|?*]', '_', fname)
    return fname[:150] if fname else 'unknown'


def _download_file(url, dest_path, timeout=DEFAULT_TIMEOUT):
    """Download a file with streaming write. Returns (success, size_or_error)."""
    import shutil
    req = Request(url, headers={'User-Agent': USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        os.makedirs(os.path.dirname(os.path.abspath(dest_path)), exist_ok=True)
        with open(dest_path, 'wb') as f:
            shutil.copyfileobj(resp, f)
        return True, os.path.getsize(dest_path)
    except (HTTPError, URLError, OSError) as e:
        return False, str(e)


def scan_catalogue(catalogue_dir):
    """Scan catalogue directory and return list of downloadable resources.

    Returns list of dicts: {file, source, title, url, downloadable, row}
    """
    resources = []
    if not os.path.isdir(catalogue_dir):
        return resources

    for fname in sorted(os.listdir(catalogue_dir)):
        if not fname.endswith('.csv') or fname == 'data_inventory.csv':
            continue

        fpath = os.path.join(catalogue_dir, fname)
        try:
            with open(fpath, encoding='utf-8') as f:
                reader = csv.DictReader(f)
                header = reader.fieldnames or []
                url_col = URL_COLUMNS.get(fname) or _detect_url_column(header)
                if not url_col or url_col not in header:
                    continue

                title_col = 'title' if 'title' in header else 'dataset_title' if 'dataset_title' in header else None

                for row in reader:
                    url = row.get(url_col, '').strip()
                    if not url:
                        continue
                    title = row.get(title_col, '') if title_col else ''
                    resources.append({
                        'catalogue_file': fname,
                        'title': title,
                        'url': url,
                        'downloadable': _is_downloadable(url),
                        'row': row,
                    })
        except Exception as e:
            print('  WARNING: could not read {}: {}'.format(fname, e))

    return resources


def _get_key():
    """Read a single keypress. Returns key name: 'up', 'down', 'space', 'enter', 'a', 'q'."""
    if sys.platform == 'win32':
        import msvcrt
        ch = msvcrt.getwch()
        if ch in ('\x00', '\xe0'):  # special key prefix
            ch2 = msvcrt.getwch()
            return {'H': 'up', 'P': 'down'}.get(ch2, '')
        return {'\r': 'enter', ' ': 'space', 'a': 'a', 'q': 'q'}.get(ch, ch)
    else:
        import tty, termios
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            ch = sys.stdin.read(1)
            if ch == '\x1b':
                ch2 = sys.stdin.read(2)
                return {'[A': 'up', '[B': 'down'}.get(ch2, '')
            return {'\r': 'enter', '\n': 'enter', ' ': 'space',
                    'a': 'a', 'q': 'q'}.get(ch, ch)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)


def select_resources(downloadable):
    """Interactive checkbox selection: arrows to move, space to toggle, enter to confirm.

    Returns filtered list of resources to download.
    """
    n = len(downloadable)
    selected = [False] * n
    cursor = 0

    def render():
        # Move cursor up to redraw (except first render)
        sys.stdout.write('\r')
        lines = []
        for i, r in enumerate(downloadable):
            fname = _safe_filename(r['url'])
            title = r.get('title', '')[:45]
            label = title or fname[:45]
            check = 'x' if selected[i] else ' '
            arrow = '>' if i == cursor else ' '
            lines.append(' {} [{}] {}'.format(arrow, check, label))
        count = sum(selected)
        lines.append('')
        lines.append(' {}/{} selected | Space=toggle  a=all  Enter=confirm  q=cancel'.format(count, n))
        return lines

    # Clear and initial render
    lines = render()
    sys.stdout.write('\n'.join(lines))
    sys.stdout.flush()
    prev_len = len(lines)

    while True:
        key = _get_key()

        if key == 'up':
            cursor = (cursor - 1) % n
        elif key == 'down':
            cursor = (cursor + 1) % n
        elif key == 'space':
            selected[cursor] = not selected[cursor]
        elif key == 'a':
            all_on = all(selected)
            selected = [not all_on] * n
        elif key == 'enter':
            # Move below the UI
            sys.stdout.write('\n')
            break
        elif key == 'q':
            sys.stdout.write('\n')
            return []
        else:
            continue

        # Redraw: move up, clear, rewrite
        sys.stdout.write('\033[{}A\r'.format(prev_len))
        lines = render()
        for line in lines:
            sys.stdout.write('\033[2K' + line + '\n')
        sys.stdout.flush()
        prev_len = len(lines)

    return [downloadable[i] for i in range(n) if selected[i]]


def _mark_downloaded(catalogue_dir, downloaded_urls):
    """Add 'downloaded' column to catalogue CSVs for downloaded URLs."""
    if not downloaded_urls:
        return

    url_set = set(downloaded_urls)
    for fname in os.listdir(catalogue_dir):
        if not fname.endswith('.csv'):
            continue
        fpath = os.path.join(catalogue_dir, fname)
        try:
            with open(fpath, encoding='utf-8') as f:
                reader = csv.DictReader(f)
                header = reader.fieldnames or []
                url_col = URL_COLUMNS.get(fname) or _detect_url_column(header)
                if not url_col or url_col not in header:
                    continue
                rows = list(reader)

            # Check if any URL in this file was downloaded
            touched = False
            for row in rows:
                url = row.get(url_col, '').strip()
                if url in url_set:
                    row['downloaded'] = 'x'
                    touched = True
                elif 'downloaded' not in row:
                    row['downloaded'] = ''

            if touched:
                out_fields = header + (['downloaded'] if 'downloaded' not in header else [])
                with open(fpath, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(rows)
        except Exception:
            pass


def download_from_catalogue(catalogue_dir, output_dir,
                            source=None, filter_text=None,
                            dry_run=False, skip_existing=True,
                            data_dir=None, interactive=False):
    """Download files from catalogue CSVs.

    Args:
        catalogue_dir: path to data/catalogue/
        output_dir: path to download destination (e.g., data/raw/downloads/)
        source: filter by catalogue file prefix (e.g., 'impact', 'dtm')
        filter_text: filter by title/URL substring (e.g., 'msna', 'disability')
        dry_run: list without downloading
        skip_existing: skip files already present in output_dir
        interactive: prompt user to select which datasets to download

    Returns:
        dict with counts: downloaded, skipped, failed, not_downloadable
    """
    all_resources = scan_catalogue(catalogue_dir)

    # Filter by source
    if source:
        source_lower = source.lower()
        all_resources = [r for r in all_resources
                         if r['catalogue_file'].lower().startswith(source_lower)]

    # Filter by text
    if filter_text:
        ft = filter_text.lower()
        all_resources = [r for r in all_resources
                         if ft in r.get('title', '').lower() or ft in r.get('url', '').lower()]

    # Separate downloadable from listings
    downloadable = [r for r in all_resources if r['downloadable']]
    listings = [r for r in all_resources if not r['downloadable']]

    print('Catalogue scan: {} resources found'.format(len(all_resources)))
    print('  Downloadable: {} (direct file URLs)'.format(len(downloadable)))
    if listings:
        print('  Listings only: {} (dataset/report pages — not direct downloads)'.format(len(listings)))
    print('  Target: {}'.format(os.path.abspath(output_dir)))
    if dry_run:
        print('  (dry run)\n')
    else:
        print()

    # Interactive selection
    if interactive and downloadable and not dry_run:
        downloadable = select_resources(downloadable)
        if not downloadable:
            return {'downloaded': 0, 'skipped': 0, 'failed': 0, 'not_downloadable': len(listings)}
        print('\n{} selected for download.\n'.format(len(downloadable)))

    os.makedirs(output_dir, exist_ok=True)

    counts = {'downloaded': 0, 'skipped': 0, 'failed': 0, 'not_downloadable': len(listings)}
    downloaded_urls = []

    for i, r in enumerate(downloadable, 1):
        fname = _safe_filename(r['url'])
        dest = os.path.join(output_dir, fname)

        if dry_run:
            print('  [{}/{}] {} | {}'.format(i, len(downloadable),
                r.get('title', '')[:35], fname[:40]))
            continue

        if skip_existing and os.path.exists(dest):
            size = os.path.getsize(dest)
            print('  [{}/{}] EXISTS ({:,} bytes) {}'.format(
                i, len(downloadable), size, fname[:45]))
            counts['skipped'] += 1
            downloaded_urls.append(r['url'])
            continue

        print('  [{}/{}] {}...'.format(i, len(downloadable), fname[:50]), end=' ')
        ok, result = _download_file(r['url'], dest)
        if ok:
            print('{:,} bytes'.format(result))
            counts['downloaded'] += 1
            downloaded_urls.append(r['url'])
        else:
            print('FAILED: {}'.format(result))
            counts['failed'] += 1

        time.sleep(0.5)

    # Mark downloaded in catalogue CSVs
    if not dry_run and downloaded_urls:
        _mark_downloaded(catalogue_dir, downloaded_urls)

    # Update inventory after download
    if not dry_run and counts['downloaded'] > 0 and data_dir:
        update_inventory(data_dir)

    return counts


def update_inventory(data_dir):
    """Rebuild data_inventory.csv by scanning all files in data/.

    Scans raw/, downloads/, catalogue/ and any other subdirs.
    Produces one row per file with: file, source, category, records, size_bytes, directory.
    """
    INVENTORY_FIELDS = [
        'file', 'source', 'category', 'records', 'size_bytes',
        'period_from', 'period_to', 'directory', 'note',
    ]

    inventory = []
    for subdir in sorted(os.listdir(data_dir)):
        dirpath = os.path.join(data_dir, subdir)
        if not os.path.isdir(dirpath) or subdir in ('processed', 'geo', '.git'):
            continue

        # Determine category from directory name
        if subdir == 'catalogue':
            category = 'catalogue'
        elif subdir == 'downloads':
            category = 'downloads'
        else:
            category = 'raw'

        # Scan files (including subdirectories)
        for root, dirs, files in os.walk(dirpath):
            for fname in sorted(files):
                if not fname.endswith(('.csv', '.xlsx', '.xls', '.zip', '.json', '.geojson', '.pdf')):
                    continue
                if fname in ('data_inventory.csv',):
                    continue
                fpath = os.path.join(root, fname)
                size = os.path.getsize(fpath)

                # Count CSV rows
                records = 0
                if fname.endswith('.csv'):
                    try:
                        with open(fpath, encoding='utf-8') as f:
                            records = max(0, sum(1 for _ in f) - 1)
                    except Exception:
                        pass

                # Build relative directory label
                rel = os.path.relpath(root, data_dir).replace('\\', '/')

                inventory.append({
                    'file': fname,
                    'source': '',
                    'category': category,
                    'records': records,
                    'size_bytes': size,
                    'period_from': '',
                    'period_to': '',
                    'directory': rel,
                    'note': '',
                })

    if inventory:
        inv_path = os.path.join(data_dir, 'data_inventory.csv')
        os.makedirs(data_dir, exist_ok=True)
        with open(inv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=INVENTORY_FIELDS)
            writer.writeheader()
            writer.writerows(inventory)
        print('  Inventory: {} files -> {}'.format(len(inventory), inv_path))

    return inventory


# --- CLI ---

def main():
    import argparse
    parser = argparse.ArgumentParser(
        description='Download resources listed in data/catalogue/ CSVs')
    parser.add_argument('--catalogue-dir', default='data/catalogue/',
                        help='Catalogue directory (default: data/catalogue/)')
    parser.add_argument('--output-dir', default='data/downloads/',
                        help='Download target (default: data/downloads/)')
    parser.add_argument('--source', help='Filter by source (e.g., impact, dtm)')
    parser.add_argument('--filter', dest='filter_text',
                        help='Filter by keyword in title/URL (e.g., msna, disability)')
    parser.add_argument('--dry-run', action='store_true',
                        help='List files without downloading')
    parser.add_argument('--scan', action='store_true',
                        help='Just scan and report what is in the catalogue')
    parser.add_argument('--select', action='store_true',
                        help='Interactive selection: pick which datasets to download')
    args = parser.parse_args()

    if args.scan:
        resources = scan_catalogue(args.catalogue_dir)
        by_file = {}
        for r in resources:
            by_file.setdefault(r['catalogue_file'], []).append(r)

        print('Catalogue scan: {}\n'.format(args.catalogue_dir))
        for fname in sorted(by_file):
            items = by_file[fname]
            dl = sum(1 for r in items if r['downloadable'])
            print('  {} — {} resources ({} downloadable)'.format(fname, len(items), dl))
        print('\nTotal: {} resources ({} downloadable)'.format(
            len(resources), sum(1 for r in resources if r['downloadable'])))
        return

    counts = download_from_catalogue(
        args.catalogue_dir, args.output_dir,
        source=args.source, filter_text=args.filter_text,
        dry_run=args.dry_run, interactive=args.select)

    print('\n---')
    if args.dry_run:
        print('Dry run complete.')
    else:
        print('Downloaded: {}, Skipped: {}, Failed: {}, Listings: {}'.format(
            counts['downloaded'], counts['skipped'],
            counts['failed'], counts['not_downloadable']))


if __name__ == '__main__':
    main()
