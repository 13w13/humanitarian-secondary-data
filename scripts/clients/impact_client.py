"""
IMPACT Initiatives Resource Centre Client
==========================================
Search and list MSNA datasets from IMPACT/REACH Resource Centre.
Uses undocumented WordPress AJAX API (screen_resources action).

The Resource Centre has 21,791+ resources. This client focuses on
MSNA datasets (programme=756, type=777) which are ~28 datasets.

Usage:
    from impact_client import IMPACTClient
    client = IMPACTClient()

    # List all MSNA datasets
    datasets = client.search_msna_datasets()

    # Filter by country
    datasets = client.search_msna_datasets(country='Palestine')

    # Search by keywords
    results = client.search(keywords='disability palestine', limit=50)

    # Get latest resources for a country
    resources = client.get_country_resources('PS')

Note: This scrapes HTML responses — fragile if IMPACT changes their site.
Always check gotchas.md for current status.

License: MIT
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import csv
import json
import os
import re
import time
from urllib.request import Request, urlopen
from urllib.parse import urlencode

# Works standalone (no dependencies) or as part of the SDS toolkit
try:
    from config import DEFAULT_TIMEOUT, USER_AGENT, save_csv
except ImportError:
    DEFAULT_TIMEOUT = 30
    USER_AGENT = 'impact-client/1.0'

    def save_csv(records, filepath):
        if not records:
            return
        os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(records[0].keys()))
            writer.writeheader()
            writer.writerows(records)
        print('Saved {} rows -> {}'.format(len(records), filepath))

AJAX_URL = 'https://www.impact-initiatives.org/wp-admin/admin-ajax.php'
COUNTRIES_JSON = 'https://www.impact-initiatives.org/wp-content/uploads/repository/countries.json'

# --- Filter IDs (discovered 2026-03-18) ---

PROGRAMME_IDS = {
    'msna': 756,
    'hsm': 754,          # Humanitarian Situation Monitoring
    'rna': 755,           # Rapid Needs Assessment
    'migration': 753,
    'cash_markets': 742,
}

TYPE_IDS = {
    'dataset': 777,
    'analysis_output': 775,
    'qualitative_grid': 776,
    'factsheet': 280,
    'report': 286,
}

# Country name → IMPACT location ID
LOCATION_IDS = {
    'AFG': 12, 'BGD': 31, 'CAF': 56, 'COD': 609, 'HTI': 111,
    'IRQ': 119, 'JOR': 127, 'LBN': 136, 'LBY': 139, 'MLI': 149,
    'NER': 173, 'NGA': 174, 'PSE': 183, 'SOM': 211, 'SSD': 215,
    'SYR': 231, 'UKR': 250, 'YEM': 262,
}

# ISO2 → ISO3 for countries.json (uses ISO2)
ISO2_TO_ISO3 = {
    'AF': 'AFG', 'BD': 'BGD', 'CF': 'CAF', 'CD': 'COD', 'HT': 'HTI',
    'IQ': 'IRQ', 'JO': 'JOR', 'LB': 'LBN', 'LY': 'LBY', 'ML': 'MLI',
    'NE': 'NER', 'NG': 'NGA', 'PS': 'PSE', 'SO': 'SOM', 'SS': 'SSD',
    'SY': 'SYR', 'UA': 'UKR', 'YE': 'YEM', 'SD': 'SDN', 'ET': 'ETH',
    'KE': 'KEN', 'MZ': 'MOZ', 'TD': 'TCD', 'MM': 'MMR', 'PK': 'PAK',
    'CM': 'CMR', 'BF': 'BFA', 'GH': 'GHA', 'SN': 'SEN',
}

ISO3_TO_ISO2 = {v: k for k, v in ISO2_TO_ISO3.items()}


class IMPACTClient:
    """Client for IMPACT Initiatives Resource Centre."""

    def __init__(self):
        self.ajax_url = AJAX_URL

    def _ajax_post(self, action, args=None):
        """POST to WordPress admin-ajax.php."""
        params = [('action', action)]
        if args:
            for key, value in args.items():
                if isinstance(value, list):
                    for v in value:
                        params.append(('args[{}][]'.format(key), str(v)))
                else:
                    params.append(('args[{}]'.format(key), str(value)))

        payload = urlencode(params).encode('utf-8')
        req = Request(self.ajax_url, data=payload, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Requested-With': 'XMLHttpRequest',
        })
        resp = urlopen(req, timeout=DEFAULT_TIMEOUT)
        return json.loads(resp.read().decode('utf-8', errors='replace'))

    def _parse_resources_html(self, html):
        """Parse resource HTML blocks into structured dicts."""
        resources = []
        blocks = re.findall(
            r'<div class="resources_result">(.*?)</div>\s*</div>\s*</div>',
            html, re.S)

        if not blocks:
            # Fallback: extract individual fields
            blocks = html.split('<div class="resources_result">')

        for block in blocks:
            if not block.strip():
                continue

            # Title + URL
            title_match = re.search(
                r'<h3><a href="([^"]+)"[^>]*>([^<]+)</a></h3>', block)
            if not title_match:
                continue

            url = title_match.group(1)
            title = title_match.group(2).strip()

            # Country
            country_match = re.search(r'<h4>([^<]*)</h4>', block)
            country = country_match.group(1).strip() if country_match else ''

            # Document type + Published date
            type_match = re.search(r'<span>([^<]+)</span>\s*<span>Published:', block)
            doc_type = type_match.group(1).strip() if type_match else ''

            date_match = re.search(r'Published:\s*([^<]+)</span>', block)
            pub_date = date_match.group(1).strip() if date_match else ''

            # Programme
            prog_match = re.search(r'Programme:</strong>\s*([^<]+)<', block)
            programme = prog_match.group(1).strip() if prog_match else ''

            # Sector
            sector_match = re.search(r'Sector/cluster:</strong>\s*([^<]+)<', block)
            sector = sector_match.group(1).strip() if sector_match else ''

            # Collection date
            coll_match = re.search(r'Data collection date:</strong>\s*([^<]+)<', block)
            collection_date = coll_match.group(1).strip() if coll_match else ''

            # File format (from label class)
            fmt_match = re.search(r'<label class="(\w+)">', block)
            file_format = fmt_match.group(1).upper() if fmt_match else ''

            resources.append({
                'title': title,
                'url': url,
                'country': country,
                'doc_type': doc_type,
                'programme': programme,
                'sector': sector,
                'pub_date': pub_date,
                'collection_date': collection_date,
                'file_format': file_format,
            })

        return resources

    def _get_total_results(self, pagination_html):
        """Extract total result count from pagination HTML."""
        match = re.search(r'([\d,]+)\s*Results', pagination_html)
        if match:
            return int(match.group(1).replace(',', ''))
        return 0

    # --- Public methods ---

    def search(self, keywords='', programme=None, doc_type=None,
               location_iso3=None, limit=50, page=1, order='latest'):
        """Search IMPACT Resource Centre.

        Args:
            keywords: Search text
            programme: Programme name key (msna, hsm, rna, etc.) or numeric ID
            doc_type: Type key (dataset, report, factsheet, etc.) or numeric ID
            location_iso3: ISO3 country code (e.g., 'PSE')
            limit: Results per page (10, 25, 50)
            page: Page number
            order: latest, oldest, relevance, downloads

        Returns:
            dict with: total, page, resources (list of dicts)
        """
        args = {'page': page, 'limit': limit, 'order': order}

        if keywords:
            args['keywords'] = keywords

        if programme:
            pid = PROGRAMME_IDS.get(programme, programme)
            args['programme'] = [pid]

        if doc_type:
            tid = TYPE_IDS.get(doc_type, doc_type)
            args['type'] = [tid]

        if location_iso3:
            lid = LOCATION_IDS.get(location_iso3.upper())
            if lid:
                args['location'] = [lid]

        data = self._ajax_post('screen_resources', args)
        html = data.get('html', '')
        pagination = data.get('pagination', '')

        resources = self._parse_resources_html(html)
        total = self._get_total_results(pagination)

        return {
            'total': total,
            'page': page,
            'resources': resources,
        }

    def search_all_pages(self, max_pages=50, **kwargs):
        """Search and paginate through all results.

        Same args as search(). Returns flat list of all resources.
        """
        kwargs.setdefault('limit', 50)
        all_resources = []
        page = 1

        while page <= max_pages:
            result = self.search(page=page, **kwargs)
            resources = result['resources']
            if not resources:
                break
            all_resources.extend(resources)
            total = result['total']
            print('  IMPACT: page {}/{} ({} resources so far)'.format(
                page, (total // kwargs['limit']) + 1, len(all_resources)))
            if len(all_resources) >= total:
                break
            page += 1
            time.sleep(0.5)  # Be polite

        return all_resources

    def search_msna_datasets(self, country_iso3=None):
        """Shortcut: list all MSNA datasets (programme=756, type=777).

        Args:
            country_iso3: Optional ISO3 filter (e.g., 'PSE')

        Returns list of resource dicts.
        """
        kwargs = {
            'programme': 'msna',
            'doc_type': 'dataset',
            'order': 'latest',
        }
        if country_iso3:
            kwargs['location_iso3'] = country_iso3

        return self.search_all_pages(**kwargs)

    def get_country_resources(self, iso2_or_iso3):
        """Get latest resources for a country from countries.json (fast, cached).

        Args:
            iso2_or_iso3: ISO2 ('PS') or ISO3 ('PSE') code

        Returns list of resource dicts (from static JSON, limited to ~2 per country).
        """
        req = Request(COUNTRIES_JSON, headers={'User-Agent': USER_AGENT})
        data = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())

        # Normalize to ISO2
        iso2 = iso2_or_iso3.upper()
        if len(iso2) == 3:
            iso2 = ISO3_TO_ISO2.get(iso2, iso2)

        country = next((c for c in data if c.get('id', '').upper() == iso2), None)
        if not country:
            return []

        # Parse repository URLs from HTML content
        content = country.get('content', '')
        urls = re.findall(
            r'href="(https://repository\.impact-initiatives\.org/[^"]+)"[^>]*>([^<]+)',
            content)

        return [{
            'title': title.strip(),
            'url': url,
            'country': country.get('title', ''),
            'file_format': url.split('.')[-1].upper(),
        } for url, title in urls]

    @staticmethod
    def save_csv(records, filepath):
        """Save records to CSV."""
        save_csv(records, filepath)


# --- CLI ---
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Search IMPACT/REACH Resource Centre (21,000+ humanitarian resources)')
    parser.add_argument('--country', help='ISO3 country code (e.g., PSE, LBN, SDN)')
    parser.add_argument('--search', help='Free-text keyword search')
    parser.add_argument('--msna', action='store_true', help='List MSNA datasets only')
    parser.add_argument('--programme', choices=list(PROGRAMME_IDS.keys()),
                        help='Filter by programme (msna, hsm, rna, migration, cash_markets)')
    parser.add_argument('--type', choices=list(TYPE_IDS.keys()),
                        help='Filter by resource type', dest='doc_type')
    parser.add_argument('--all-pages', action='store_true', help='Fetch all pages')
    parser.add_argument('--output', '-o', help='Save results to CSV file')
    args = parser.parse_args()

    client = IMPACTClient()
    resources = []

    if args.msna:
        print('IMPACT Resource Centre — MSNA Datasets {}'.format(
            args.country or '(all countries)'))
        resources = client.search_msna_datasets(args.country)
        print('Found {} MSNA datasets\n'.format(len(resources)))

    elif args.search or args.programme or args.doc_type:
        kwargs = {}
        if args.search:
            kwargs['keywords'] = args.search
        if args.programme:
            kwargs['programme'] = args.programme
        if args.doc_type:
            kwargs['doc_type'] = args.doc_type
        if args.country:
            kwargs['location_iso3'] = args.country

        if args.all_pages:
            resources = client.search_all_pages(**kwargs)
        else:
            result = client.search(**kwargs)
            resources = result['resources']
            print('{} total results (showing page 1)\n'.format(result['total']))

    elif args.country:
        result = client.search(location_iso3=args.country, limit=50)
        resources = result['resources']
        print('IMPACT — {} : {} total resources\n'.format(args.country, result['total']))

    else:
        parser.print_help()
        sys.exit(0)

    for r in resources:
        print('  [{}] {} | {} | {}'.format(
            r.get('pub_date', ''), r.get('country', '')[:20],
            r.get('title', '')[:55], r.get('file_format', '')))

    if args.output and resources:
        client.save_csv(resources, args.output)

    print('\n{} resources'.format(len(resources)))
