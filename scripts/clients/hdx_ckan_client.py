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
    results = hdx.search_datasets('msna lebanon', format='XLSX')
    hdx.download_resource(results[0]['resources'][0], 'data/LBN/')
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
import os
import time
from urllib.request import Request, urlopen

from config import HDX_CKAN_BASE, DEFAULT_TIMEOUT, RATE_LIMIT_DELAY, USER_AGENT


class HDXClient:
    """Client for HDX CKAN API v3. Uses JWT from keyring if available."""

    def __init__(self, api_key=None):
        self.base = HDX_CKAN_BASE
        if api_key:
            self.api_key = api_key
        else:
            import keyring
            self.api_key = keyring.get_password('sds.hdx', 'api_key') or ''

    def _get(self, action, params=''):
        """GET request to CKAN API."""
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
        fq = 'res_format:{}'.format(format_filter) if format_filter else ''
        params = 'q={}&rows={}'.format(query.replace(' ', '+'), rows)
        if fq:
            params += '&fq={}'.format(fq)

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
                    'last_modified': str(res.get('last_modified', ''))[:10],
                })
            org = pkg.get('organization', {})
            datasets.append({
                'name': pkg.get('name', ''),
                'title': pkg.get('title', ''),
                'org': org.get('title', '') if org else '',
                # `metadata_modified` = derniere MAJ des metadonnees HDX, PAS la periode
                # couverte par la donnee. Un dataset de 2021 remis a jour hier
                # affiche 2026. Le nom de colonne le dit maintenant.
                'metadata_modified': str(pkg.get('metadata_modified', ''))[:10],
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

    def list_all_datasets(self, iso3, theme_filter=None, rows=500):
        """List ALL datasets for a country using CKAN group filter.

        Unlike search_datasets() which searches by text query (and misses most),
        this uses fq=groups:{iso3_lower} which returns ALL datasets tagged for
        that country. HDX uses ISO3 lowercase as group names.

        Args:
            iso3: ISO3 country code (e.g., 'PSE', 'LBN', 'SDN')
            theme_filter: Optional list of keywords to filter by title/notes
                          (e.g., ['disability', 'needs', 'assessment', 'msna'])
            rows: Max datasets to retrieve (default 500, HDX max ~1000)

        Returns list of dataset dicts (same format as search_datasets).
        """
        iso3_lower = iso3.lower()
        params = 'fq=groups:{}&rows={}&sort=metadata_modified+desc'.format(iso3_lower, rows)

        result = self._get('package_search', params)
        total_count = result.get('count', 0)
        datasets = []

        for pkg in result.get('results', []):
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
                    'last_modified': str(res.get('last_modified', ''))[:10],
                })
            org = pkg.get('organization', {})
            datasets.append({
                'name': pkg.get('name', ''),
                'title': pkg.get('title', ''),
                'org': org.get('title', '') if org else '',
                # `metadata_modified` = derniere MAJ des metadonnees HDX, PAS la periode
                # couverte par la donnee. Un dataset de 2021 remis a jour hier
                # affiche 2026. Le nom de colonne le dit maintenant.
                'metadata_modified': str(pkg.get('metadata_modified', ''))[:10],
                'data_period_start': str(pkg.get('dataset_date', '') or '')[:40],
                'license': pkg.get('license_title', ''),
                'num_resources': len(resources),
                'resources': resources,
                'url': 'https://data.humdata.org/dataset/{}'.format(pkg.get('name', '')),
            })

        print('  HDX CKAN: {} total datasets for {} (returned {}{})'.format(
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
                'last_modified': str(res.get('last_modified', ''))[:10],
            })
        org = result.get('organization', {})
        return {
            'name': result.get('name', ''),
            'title': result.get('title', ''),
            'org': org.get('title', '') if org else '',
            'notes': result.get('notes', ''),
            'license': result.get('license_title', ''),
            'metadata_modified': str(result.get('metadata_modified', ''))[:10],
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

        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)

        req = Request(url, headers={'User-Agent': USER_AGENT})
        with urlopen(req, timeout=60) as resp:
            with open(filepath, 'wb') as f:
                while True:
                    chunk = resp.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)

        size = os.path.getsize(filepath)
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
                    'dataset_date': ds.get('metadata_modified', ''),
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
