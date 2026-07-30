"""
ReliefWeb API Client
====================
Reusable module for querying ReliefWeb reports.
Uses POST queries (recommended over GET for complex filters).

Usage:
    from reliefweb_client import ReliefWebClient
    rw = ReliefWebClient()
    facets = rw.get_facets('LBN', '2026-03-01', '2026-03-31')
    sitreps = rw.get_sitreps('LBN', '2026-03-01', '2026-03-31')
    disability = rw.search_disability('LBN')
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
import csv
import os
import time
from urllib.request import Request, urlopen

from config import (
    RELIEFWEB_BASE, RELIEFWEB_APPNAME,
    DEFAULT_TIMEOUT, RATE_LIMIT_DELAY, USER_AGENT
)


class ReliefWebClient:
    """Client for ReliefWeb API v1 (POST queries)."""

    def __init__(self, appname=None):
        self.base = RELIEFWEB_BASE
        # Pre-approved appname (required since 2025-11-01): keyring, then env,
        # then the generic config fallback. Personal appnames stay out of the
        # published config.py.
        import keyring
        self.appname = (appname
                        or keyring.get_password('sds.reliefweb', 'appname')
                        or os.environ.get('RELIEFWEB_APPNAME')
                        or RELIEFWEB_APPNAME)

    def _post(self, endpoint, payload):
        """POST query to ReliefWeb API."""
        url = '{}/{}?appname={}'.format(self.base, endpoint, self.appname)
        data = json.dumps(payload).encode('utf-8')
        req = Request(url, data=data, headers={
            'Content-Type': 'application/json',
            'User-Agent': USER_AGENT,
        })
        resp = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
        return resp

    def _date_filter(self, iso3, date_from=None, date_to=None):
        """Build standard country + date filter."""
        conditions = [{'field': 'primary_country.iso3', 'value': iso3}]
        if date_from:
            date_val = {'from': '{}T00:00:00+00:00'.format(date_from)}
            if date_to:
                date_val['to'] = '{}T23:59:59+00:00'.format(date_to)
            conditions.append({'field': 'date.created', 'value': date_val})
        if len(conditions) == 1:
            return conditions[0]
        return {'operator': 'AND', 'conditions': conditions}

    # ── Facets ──────────────────────────────────────────────

    def get_facets(self, iso3, date_from=None, date_to=None):
        """Get report facets (format, source, theme, disaster_type) for a country/period.

        Returns dict with keys: total, facets (dict of field -> list of {value, count}).
        """
        resp = self._post('reports', {
            'filter': self._date_filter(iso3, date_from, date_to),
            'facets': [
                {'field': 'format.name', 'limit': 20},
                {'field': 'source.name', 'limit': 25},
                {'field': 'theme.name', 'limit': 30},
                {'field': 'disaster_type.name', 'limit': 10},
                {'field': 'language.name', 'limit': 10},
            ],
            'limit': 0,
        })
        total = resp.get('totalCount', 0)
        raw_facets = resp.get('embedded', {}).get('facets', {})
        facets = {}
        for fname, fdata in raw_facets.items():
            facets[fname] = [
                {'value': item['value'], 'count': item['count']}
                for item in fdata.get('data', [])
            ]
        return {'total': total, 'facets': facets}

    # ── Situation Reports ───────────────────────────────────

    def get_sitreps(self, iso3, date_from=None, date_to=None, limit=50):
        """Fetch Situation Reports for a country/period.

        Returns list of dicts with: id, title, source, date, url.
        """
        filt = self._date_filter(iso3, date_from, date_to)
        # Add format filter
        if isinstance(filt, dict) and filt.get('operator') == 'AND':
            filt['conditions'].append({'field': 'format.name', 'value': 'Situation Report'})
        else:
            filt = {'operator': 'AND', 'conditions': [filt, {'field': 'format.name', 'value': 'Situation Report'}]}

        reports = []
        offset = 0
        while True:
            resp = self._post('reports', {
                'filter': filt,
                'fields': {'include': ['title', 'source.name', 'date.created', 'url']},
                'sort': ['date.created:desc'],
                'limit': limit,
                'offset': offset,
            })
            data = resp.get('data', [])
            for r in data:
                f = r.get('fields', {})
                sources = f.get('source', [])
                reports.append({
                    'id': r.get('id', ''),
                    'title': f.get('title', ''),
                    'source': ', '.join(s.get('name', '') for s in sources),
                    'date': f.get('date', {}).get('created', '')[:10],
                    'url': f.get('url', ''),
                })
            if len(data) < limit:
                break
            offset += limit
            time.sleep(RATE_LIMIT_DELAY)
        return reports

    # ── Disability search ───────────────────────────────────

    def search_disability(self, iso3, date_from=None, date_to=None, limit=50):
        """Search reports mentioning disability (full-text query).

        Returns dict with: total_reports, disability_reports, reports (list).
        """
        # Total reports for period
        facets = self.get_facets(iso3, date_from, date_to)
        total = facets['total']

        # Disability query
        payload = {
            'query': {'value': 'disability OR "persons with disabilities" OR handicap'},
            'filter': self._date_filter(iso3, date_from, date_to),
            'fields': {'include': ['title', 'source.name', 'date.created', 'format.name', 'url']},
            'sort': ['date.created:desc'],
            'limit': limit,
        }
        resp = self._post('reports', payload)
        disability_total = resp.get('totalCount', 0)

        reports = []
        for r in resp.get('data', []):
            f = r.get('fields', {})
            sources = f.get('source', [])
            formats = f.get('format', [])
            reports.append({
                'title': f.get('title', ''),
                'source': sources[0].get('name', '') if sources else '',
                'format': formats[0].get('name', '') if formats else '',
                'date': f.get('date', {}).get('created', '')[:10],
                'url': f.get('url', ''),
            })

        return {
            'total_reports': total,
            'disability_reports': disability_total,
            'disability_pct': round(disability_total / total * 100, 1) if total else 0,
            'reports': reports,
        }

    # ── All reports (generic) ──────────────────────────────

    def get_reports(self, iso3, date_from=None, date_to=None, limit=50, format_name=None):
        """Fetch reports with optional format filter. Returns list of report dicts."""
        filt = self._date_filter(iso3, date_from, date_to)
        if format_name:
            if isinstance(filt, dict) and filt.get('operator') == 'AND':
                filt['conditions'].append({'field': 'format.name', 'value': format_name})
            else:
                filt = {'operator': 'AND', 'conditions': [filt, {'field': 'format.name', 'value': format_name}]}

        reports = []
        offset = 0
        while True:
            resp = self._post('reports', {
                'filter': filt,
                'fields': {'include': ['title', 'source.name', 'date.created', 'format.name', 'url']},
                'sort': ['date.created:desc'],
                'limit': limit,
                'offset': offset,
            })
            data = resp.get('data', [])
            for r in data:
                f = r.get('fields', {})
                sources = f.get('source', [])
                formats = f.get('format', [])
                reports.append({
                    'id': r.get('id', ''),
                    'title': f.get('title', ''),
                    'source': ', '.join(s.get('name', '') for s in sources),
                    'format': formats[0].get('name', '') if formats else '',
                    'date': f.get('date', {}).get('created', '')[:10],
                    'url': f.get('url', ''),
                })
            if len(data) < limit:
                break
            offset += limit
            time.sleep(RATE_LIMIT_DELAY)
        return reports


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    iso3 = sys.argv[1] if len(sys.argv) > 1 else 'LBN'
    rw = ReliefWebClient()

    print('=== ReliefWeb — {} ==='.format(iso3))
    facets = rw.get_facets(iso3)
    print('Total reports: {}'.format(facets['total']))
    for fname, items in facets['facets'].items():
        print('\n  {}:'.format(fname))
        for item in items[:10]:
            print('    {} ({})'.format(item['value'], item['count']))

    print('\n--- Disability search ---')
    dis = rw.search_disability(iso3)
    print('Disability reports: {} / {} ({:.1f}%)'.format(
        dis['disability_reports'], dis['total_reports'], dis['disability_pct']))
    for r in dis['reports'][:10]:
        print('  [{}] {} — {}'.format(r['date'], r['title'][:70], r['source']))
