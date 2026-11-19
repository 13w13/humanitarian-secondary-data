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
if hasattr(sys.stdout, 'reconfigure'):   # absent in Jupyter, IDLE, captured output
    sys.stdout.reconfigure(encoding='utf-8')

import json
import re
import time
from datetime import date
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import (
    RELIEFWEB_BASE, RELIEFWEB_APPNAME, RELIEFWEB_APPNAME_HELP, MissingCredential,
    DEFAULT_TIMEOUT, RATE_LIMIT_DELAY, USER_AGENT, get_credential, raise_unavailable)


MAX_PAGES = 40      # per listing: 40 x limit rows at most

# A ReliefWeb epidemic still "ongoing" this long after it started is flagged: the
# editors do not always close disaster pages ("DR Congo: Cholera and Measles
# Outbreaks - 2013-2026" was ongoing on 2026-11-19, as were Somalia 2016 pages).
STALE_EPIDEMIC_DAYS = 730


def parse_disaster_name(name):
    """Disease part of a ReliefWeb disaster name. No network: testable.

    'Sudan: Cholera Outbreak - Aug 2024' -> 'Cholera'; 'DR Congo: Cholera and Measles
    Outbreaks - 2013-2026' -> 'Cholera and Measles'; 'South Sudan: Measles - Dec 2022'
    -> 'Measles'. The wording is ReliefWeb's, not a harmonised disease name.
    """
    text = str(name or '').strip()
    if ': ' in text:
        text = text.split(': ', 1)[1]
    if ' - ' in text:
        text = text.rsplit(' - ', 1)[0]
    return re.sub(r'\s+outbreaks?$', '', text.strip(), flags=re.I).strip()


class ReliefWebClient:
    """Client for ReliefWeb API v2 (POST queries)."""

    def __init__(self, appname=None):
        self.base = RELIEFWEB_BASE
        # Pre-approved appname (required since 2025-11-01): keyring, then env,
        # then the generic config fallback. Personal appnames stay out of the
        # published config.py.
        self.appname = (appname
                        or get_credential('sds.reliefweb', 'appname',
                                          'RELIEFWEB_APPNAME')
                        or RELIEFWEB_APPNAME)

    def _post(self, endpoint, payload):
        """POST query to ReliefWeb API."""
        # urlencode: an appname with "&" or a space injected a parameter or broke
        # the URL.
        url = '{}/{}?{}'.format(self.base, endpoint, urlencode({'appname': self.appname}))
        data = json.dumps(payload).encode('utf-8')
        req = Request(url, data=data, headers={
            'Content-Type': 'application/json',
            'User-Agent': USER_AGENT,
        })
        try:
            return json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
        except HTTPError as e:
            if e.code == 403:   # an unapproved appname: a missing credential
                raise MissingCredential(RELIEFWEB_APPNAME_HELP) from e
            raise

    def _date_filter(self, iso3, date_from=None, date_to=None):
        """Build standard country + date filter."""
        conditions = [{'field': 'primary_country.iso3', 'value': iso3}]
        # Each bound on its own: date_to used to be dropped unless date_from was
        # also given, and the whole history came back instead.
        date_val = {}
        if date_from:
            date_val['from'] = '{}T00:00:00+00:00'.format(date_from)
        if date_to:
            date_val['to'] = '{}T23:59:59+00:00'.format(date_to)
        if date_val:
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
            offset += limit
            # Stop on the announced total, and after MAX_PAGES whatever happens: an
            # ignored offset used to loop forever on ReliefWeb's daily quota.
            if len(data) < limit or offset >= resp.get('totalCount', 0) \
                    or offset >= limit * MAX_PAGES:
                break
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
            offset += limit
            # Stop on the announced total, and after MAX_PAGES whatever happens: an
            # ignored offset used to loop forever on ReliefWeb's daily quota.
            if len(data) < limit or offset >= resp.get('totalCount', 0) \
                    or offset >= limit * MAX_PAGES:
                break
            time.sleep(RATE_LIMIT_DELAY)
        return reports

    # ── Epidemics (disasters of type EP) ───────────────────

    def ongoing_epidemics(self, iso3=None, statuses=('ongoing', 'alert'), limit=200):
        """Epidemic disaster pages (type EP), newest start first.

        `iso3` filters on `country.iso3`, which is inclusive: a multi-country page
        ('Central/Eastern Africa: Ebola Outbreak - May 2026', primary COD) is listed
        for KEN, NGA and UGA too. Pass statuses=('ongoing', 'alert', 'past') for the
        history. `stale` flags an ongoing page opened more than STALE_EPIDEMIC_DAYS
        ago: ReliefWeb does not always close its pages, so "ongoing" is the
        editors' last word, not a confirmation that the outbreak is active.
        """
        conditions = [{'field': 'type.code', 'value': 'EP'},
                      {'field': 'status', 'value': list(statuses), 'operator': 'OR'}]
        if iso3:
            conditions.append({'field': 'country.iso3', 'value': str(iso3).lower()})
        rows, offset, today = [], 0, date.today()
        while True:
            try:
                resp = self._post('disasters', {
                    'filter': {'operator': 'AND', 'conditions': conditions},
                    'fields': {'include': ['name', 'status', 'date.event', 'date.created',
                                           'country.iso3', 'primary_country.iso3',
                                           'glide', 'url']},
                    'sort': ['date.event:desc'], 'limit': limit, 'offset': offset})
            except MissingCredential:
                raise
            except Exception as e:  # noqa: BLE001 - outage -> SourceUnavailable
                raise_unavailable('ReliefWeb disasters', e)
            data = resp.get('data', [])
            for item in data:
                f = item.get('fields', {})
                start = str((f.get('date') or {}).get('event') or '')[:10]
                status = f.get('status', '')
                try:
                    open_days = (today - date(*map(int, start.split('-')))).days
                except (TypeError, ValueError):
                    open_days = None
                live = status in ('ongoing', 'alert')
                rows.append({
                    'disease': parse_disaster_name(f.get('name')),
                    'name': f.get('name', ''),
                    'iso3': ';'.join(str(c.get('iso3') or '').upper()
                                     for c in f.get('country') or []),
                    'primary_iso3': str((f.get('primary_country') or {}).get('iso3')
                                        or '').upper(),
                    'date_start': start,
                    'date_created': str((f.get('date') or {}).get('created') or '')[:10],
                    'status': status,
                    'open_days': open_days if live else None,
                    'stale': bool(live and open_days is not None
                                  and open_days > STALE_EPIDEMIC_DAYS),
                    'glide': f.get('glide', ''),
                    'url': f.get('url', ''),
                })
            offset += limit
            if len(data) < limit or offset >= resp.get('totalCount', 0) \
                    or offset >= limit * MAX_PAGES:
                break
            time.sleep(RATE_LIMIT_DELAY)
        return rows

    def get_source_reports(self, source_shortname, iso3=None, date_from=None, limit=50):
        """Reports of one source (e.g. 'Africa CDC'), newest first, one page.

        `iso3` filters on `country.iso3` (inclusive: a weekly report tags every
        country it covers). Africa CDC has no API of its own: its Epidemic
        Intelligence Weekly Report reaches machines through this listing, a few
        days after africacdc.org.
        """
        conditions = [{'field': 'source.shortname', 'value': source_shortname}]
        if iso3:
            conditions.append({'field': 'country.iso3', 'value': str(iso3).lower()})
        if date_from:
            conditions.append({'field': 'date.created',
                               'value': {'from': '{}T00:00:00+00:00'.format(date_from)}})
        try:
            resp = self._post('reports', {
                'filter': {'operator': 'AND', 'conditions': conditions},
                'fields': {'include': ['title', 'date.created', 'format.name',
                                       'country.iso3', 'url']},
                'sort': ['date.created:desc'], 'limit': limit})
        except MissingCredential:
            raise
        except Exception as e:  # noqa: BLE001 - outage -> SourceUnavailable
            raise_unavailable('ReliefWeb reports ({})'.format(source_shortname), e)
        out = []
        for item in resp.get('data', []):
            f = item.get('fields', {})
            formats = f.get('format') or []
            out.append({
                'title': f.get('title', ''),
                'date': str((f.get('date') or {}).get('created') or '')[:10],
                'format': formats[0].get('name', '') if formats else '',
                'iso3': ';'.join(str(c.get('iso3') or '').upper()
                                 for c in f.get('country') or []),
                'url': f.get('url', ''),
            })
        return out


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
