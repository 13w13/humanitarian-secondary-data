"""
HDX CKAN Client
================
Search and download datasets from HDX (Humanitarian Data Exchange).
Uses CKAN API v3. Optional JWT auth via keyring sds.hdx/api_key (for private datasets).

Key discovery: HDX CKAN is the richest source for MSNA datasets (64 globally),
with direct XLSX/CSV download URLs and CC BY-IGO license.

Usage:
    from hdx_ckan_client import HDXClient
    hdx = HDXClient()
    results = hdx.search_datasets('msna lebanon', format_filter='XLSX')
    hdx.download_resource(results[0]['resources'][0], 'data/LBN/')
"""
import sys
if hasattr(sys.stdout, 'reconfigure'):   # absent in Jupyter, IDLE, captured output
    sys.stdout.reconfigure(encoding='utf-8')

import json
import os
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import (HDX_CKAN_BASE, DEFAULT_TIMEOUT, RATE_LIMIT_DELAY, USER_AGENT,
                    get_credential, download_stream, safe_filename)


def _day(v):
    """First 10 characters of a date field, '' for null (never the text 'None')."""
    return str(v or '')[:10]


class HDXClient:
    """Client for HDX CKAN API v3. Uses JWT from keyring if available."""

    def __init__(self, api_key=None):
        self.base = HDX_CKAN_BASE
        if api_key:
            self.api_key = api_key
        else:
            self.api_key = get_credential('sds.hdx', 'api_key', 'HDX_API_KEY')

    def _get(self, action, params=''):
        """GET request to CKAN API. `params` is a dict (url-encoded here) or a
        ready query string."""
        if isinstance(params, dict):
            params = urlencode(params)
        url = '{}/{}?{}'.format(self.base, action, params)
        headers = {'User-Agent': USER_AGENT}
        if self.api_key:
            headers['Authorization'] = self.api_key
        req = Request(url, headers=headers)
        resp = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
        if not resp.get('success'):
            raise RuntimeError('CKAN API error: {}'.format(resp.get('error', 'unknown')))
        return resp['result']

    # ── Search ──────────────────────────────────────────────

    def search_datasets(self, query, format_filter=None, rows=20):
        """Search HDX datasets by keyword.

        Args:
            query: Search string (e.g., 'msna lebanon', 'displacement sudan')
            format_filter: Filter by resource format (e.g., 'XLSX', 'CSV')
            rows: Max results to return

        Returns list of dicts with: name, title, org, date, num_resources, resources.
        """
        # urlencode: "water & sanitation" or "côte d'ivoire" were sent raw, which
        # truncated the query at "&" or raised on non-ASCII.
        params = {'q': query, 'rows': rows}
        if format_filter:
            params['fq'] = 'res_format:{}'.format(format_filter)

        result = self._get('package_search', params)
        datasets = []
        for pkg in result.get('results', []):
            resources = []
            for res in pkg.get('resources', []):
                resources.append({
                    'id': res.get('id', ''),
                    'name': res.get('name', ''),
                    'format': res.get('format', ''),
                    'url': res.get('url', ''),
                    'size': res.get('size', 0),
                    'last_modified': _day(res.get('last_modified')),
                })
            org = pkg.get('organization', {})
            datasets.append({
                'name': pkg.get('name', ''),
                'title': pkg.get('title', ''),
                'org': org.get('title', '') if org else '',
                # `metadata_modified` = derniere MAJ des metadonnees HDX, PAS la periode
                # couverte par la donnee. Un dataset de 2021 remis a jour hier
                # affiche 2026. Le nom de colonne le dit maintenant.
                'metadata_modified': _day(pkg.get('metadata_modified')),
                'data_period_start': str(pkg.get('dataset_date', '') or '')[:40],
                'license': pkg.get('license_title', ''),
                'num_resources': len(resources),
                'resources': resources,
                'url': 'https://data.humdata.org/dataset/{}'.format(pkg.get('name', '')),
            })
        return datasets

    def search_msna(self, country_name, rows=20):
        """Shortcut: search MSNA datasets for a country.

        Returns datasets filtered to XLSX format.
        """
        return self.search_datasets(
            'msna {}'.format(country_name),
            format_filter='XLSX',
            rows=rows,
        )

    def list_all_datasets(self, iso3, theme_filter=None, rows=500, max_datasets=5000):
        """List ALL datasets for a country using CKAN group filter.

        Unlike search_datasets() which searches by text query (and misses most),
        this uses fq=groups:{iso3_lower} which returns ALL datasets tagged for
        that country. HDX uses ISO3 lowercase as group names.

        Pages with `start=` until the `count` HDX announces is reached (it used to
        stop at the first 500 rows and report them as the country's total, so a
        disability dataset at position 900 read as "0 disability-related").
        `self.last_count` keeps the announced total, and a listing cut short by
        `max_datasets` says so.

        Args:
            iso3: ISO3 country code (e.g., 'PSE', 'LBN', 'SDN')
            theme_filter: Optional list of keywords to filter by title/notes
                          (e.g., ['disability', 'needs', 'assessment', 'msna'])
            rows: page size (HDX caps it at 1000)
            max_datasets: safety cap on the whole walk

        Returns list of dataset dicts (same format as search_datasets).
        """
        iso3_lower = iso3.lower()
        pkgs, start, total_count = [], 0, None
        while True:
            result = self._get('package_search', {
                'fq': 'groups:{}'.format(iso3_lower), 'rows': rows, 'start': start,
                'sort': 'metadata_modified desc'})
            if total_count is None:
                total_count = result.get('count', 0) or 0
            batch = result.get('results', []) or []
            pkgs.extend(batch)
            start += len(batch)
            if not batch or start >= total_count or start >= max_datasets:
                break
            time.sleep(RATE_LIMIT_DELAY)
        self.last_count = total_count
        self.last_truncated = len(pkgs) < total_count
        if self.last_truncated:
            print('  HDX CKAN: only {} of {} datasets listed for {} (max_datasets={}): '
                  'a partial listing, not the total'.format(
                      len(pkgs), total_count, iso3_lower, max_datasets))

        # Post-condition: the group facet is the country scope. A row outside it
        # would be another country's dataset listed under this one.
        stray = [p.get('name') for p in pkgs
                 if iso3_lower not in [str(g.get('name', '')).lower()
                                       for g in (p.get('groups') or [])]]
        if stray:
            raise ValueError('HDX CKAN: {} dataset(s) outside group {} returned '
                             '(e.g. {}): the country filter did not apply'.format(
                                 len(stray), iso3_lower, stray[:2]))

        datasets = []
        for pkg in pkgs:
            # Optional theme filter (case-insensitive on title + notes)
            if theme_filter:
                text = '{} {}'.format(
                    pkg.get('title', ''), pkg.get('notes', '')).lower()
                if not any(kw.lower() in text for kw in theme_filter):
                    continue

            resources = []
            for res in pkg.get('resources', []):
                resources.append({
                    'id': res.get('id', ''),
                    'name': res.get('name', ''),
                    'format': res.get('format', ''),
                    'url': res.get('url', ''),
                    'size': res.get('size', 0),
                    'last_modified': _day(res.get('last_modified')),
                })
            org = pkg.get('organization', {})
            datasets.append({
                'name': pkg.get('name', ''),
                'title': pkg.get('title', ''),
                'org': org.get('title', '') if org else '',
                # `metadata_modified` = derniere MAJ des metadonnees HDX, PAS la periode
                # couverte par la donnee. Un dataset de 2021 remis a jour hier
                # affiche 2026. Le nom de colonne le dit maintenant.
                'metadata_modified': _day(pkg.get('metadata_modified')),
                'data_period_start': str(pkg.get('dataset_date', '') or '')[:40],
                'license': pkg.get('license_title', ''),
                'num_resources': len(resources),
                'resources': resources,
                'url': 'https://data.humdata.org/dataset/{}'.format(pkg.get('name', '')),
                # kept so a caller can apply more theme filters without re-querying
                'notes': str(pkg.get('notes', '') or '')[:2000],
            })

        print('  HDX CKAN: {} total datasets for {} (listed {}{})'.format(
            total_count, iso3_lower, len(datasets),
            ', filtered by {}'.format(theme_filter) if theme_filter else ''))
        return datasets

    # ── Dataset details ─────────────────────────────────────

    def get_dataset(self, dataset_name):
        """Get full details for a specific dataset by name/id."""
        result = self._get('package_show', 'id={}'.format(dataset_name))
        resources = []
        for res in result.get('resources', []):
            resources.append({
                'id': res.get('id', ''),
                'name': res.get('name', ''),
                'format': res.get('format', ''),
                'url': res.get('url', ''),
                'size': res.get('size', 0),
                'last_modified': _day(res.get('last_modified')),
            })
        org = result.get('organization', {})
        return {
            'name': result.get('name', ''),
            'title': result.get('title', ''),
            'org': org.get('title', '') if org else '',
            'notes': result.get('notes', ''),
            'license': result.get('license_title', ''),
            'metadata_modified': _day(result.get('metadata_modified')),
            'data_period_start': str(result.get('dataset_date', '') or '')[:40],
            'resources': resources,
        }

    # ── Download ────────────────────────────────────────────

    def download_resource(self, resource, output_dir, filename=None):
        """Download a resource file to output_dir.

        Args:
            resource: Resource dict (from search results) with 'url' and 'name' keys
            output_dir: Directory to save file
            filename: Override filename (default: use resource name or URL basename)

        The name and URL come from publisher metadata, so they are untrusted: the
        name is reduced to one path component (it used to accept "../../x" and
        write outside output_dir), and the download goes through
        config.download_stream (http/https only, size cap, atomic .part write).

        Returns path to downloaded file.
        """
        url = resource.get('url', '')
        if not url:
            raise ValueError('Resource has no download URL')

        if not filename:
            filename = resource.get('name', '') or url.split('/')[-1].split('?')[0]
            # Ensure extension
            fmt = resource.get('format', '').lower()
            if fmt and not filename.lower().endswith('.{}'.format(fmt)):
                filename = '{}.{}'.format(filename, fmt)
        filename = safe_filename(filename)

        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)
        root = os.path.realpath(output_dir)
        if os.path.dirname(os.path.realpath(filepath)) != root:
            raise ValueError('refusing to write outside {}: {!r}'.format(output_dir, filename))

        size = download_stream(url, filepath, timeout=60)
        print('  Downloaded: {} ({:,} bytes)'.format(filename, size))
        return filepath

    # ── Metadata CSV ────────────────────────────────────────

    def datasets_to_csv_rows(self, datasets):
        """Convert dataset list to flat CSV rows (1 row per resource).

        Returns list of dicts suitable for csv.DictWriter.
        """
        rows = []
        for ds in datasets:
            for res in ds.get('resources', []):
                rows.append({
                    'dataset_name': ds['name'],
                    'dataset_title': ds['title'],
                    'org': ds['org'],
                    # HDX's own `dataset_date` is the period the data covers;
                    # this is the last metadata edit, so it carries that name.
                    'metadata_modified': ds.get('metadata_modified', ''),
                    'license': ds.get('license', ''),
                    'resource_name': res['name'],
                    'resource_format': res['format'],
                    'resource_url': res['url'],
                    'resource_size': res.get('size', ''),
                    'resource_modified': res.get('last_modified', ''),
                    'hdx_url': ds.get('url', ''),
                })
        return rows


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    import argparse as _ap
    _parser = _ap.ArgumentParser(description='HDX CKAN Client')
    _parser.add_argument('country', nargs='?', default='lebanon',
                         help='Country name or ISO3 code')
    _parser.add_argument('--all', action='store_true',
                         help='List ALL datasets (by ISO3 group, not text search)')
    _parser.add_argument('--filter', nargs='*',
                         help='Theme keywords to filter (e.g., disability needs)')
    _pargs = _parser.parse_args()

    hdx = HDXClient()

    if _pargs.all:
        iso3 = _pargs.country.upper() if len(_pargs.country) == 3 else _pargs.country
        print('=== HDX CKAN — ALL datasets for {} ==='.format(iso3))
        datasets = hdx.list_all_datasets(iso3, theme_filter=_pargs.filter)
        print('Found {} datasets'.format(len(datasets)))
        for ds in datasets[:30]:
            print('\n  {} [{}]'.format(ds['title'][:70], ds['org']))
            print('  MAJ metadonnees: {} | Resources: {}'.format(ds.get('metadata_modified',''), ds['num_resources']))
    else:
        country = _pargs.country
        print('=== HDX CKAN — MSNA search: {} ==='.format(country))
        datasets = hdx.search_msna(country)
        print('Found {} datasets'.format(len(datasets)))
        for ds in datasets[:10]:
            print('\n  {} [{}]'.format(ds['title'][:70], ds['org']))
            print('  Date: {} | License: {} | Resources: {}'.format(
                ds.get('metadata_modified',''), ds.get('license', 'N/A'), ds['num_resources']))
            for res in ds['resources'][:3]:
                print('    - {} ({}) {}'.format(res['name'][:50], res['format'], res['url'][:80]))
