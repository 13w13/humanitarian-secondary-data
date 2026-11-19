"""
WHO weekly cholera client
=========================
Weekly cholera cases and deaths by country, as WHO's global cholera and acute watery
diarrhoea (AWD) dashboard shows them: a public ArcGIS FeatureServer, no key.

What it is: cases REPORTED to WHO by the countries (suspected and confirmed, as each
country reports), since 2023, about one month behind (week of 2026-10-05 was the
newest on 2026-11-19), 43 countries on that date. Negative weekly values have been
removed by WHO. What it is not: a complete count (under-reporting, access), nor a
series for countries that reported no cholera since 2023.

Why it matters: it is the "since when, how many" that the alert feeds lack. On
2026-11-19 WHO had published no Disease Outbreak News tagged Sudan since January 2023,
while this series showed Sudan's cumulative cases at 138,260.

Usage:
    from who_cholera_client import WHOCholeraClient
    ch = WHOCholeraClient()
    ch.weekly('SDN', since='2026-01-01')     # one row per week, oldest first
    ch.weekly(since='2026-09-01')            # every country
"""
import sys
if hasattr(sys.stdout, 'reconfigure'):   # absent in Jupyter, IDLE, captured output
    sys.stdout.reconfigure(encoding='utf-8')

import json
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import (DEFAULT_TIMEOUT, USER_AGENT, NotCovered, SourceUnavailable,
                    normalize_iso3, raise_unavailable, save_csv,
                    WHO_CHOLERA_FEATURESERVER)

CAVEATS = [
    'Cases reported to WHO by countries (suspected and confirmed), not a complete '
    'count; about one month behind.',
    'Only countries that reported cholera since 2023 have rows.',
    'A week at 0 often means no report that week, not zero cases: Sudan\'s cases '
    'arrive in batches (0, 0, then 545 in August 2026).',
]
_FIELDS = 'iso_3_code,adm0_name,who_region,date_wk,epiyr,epiwk,cases,deaths,cum_case,cum_death'
PAGE = 1000          # the service's maxRecordCount


def _week(ms):
    """ArcGIS date (epoch milliseconds, UTC) -> 'YYYY-MM-DD'."""
    try:
        return datetime.fromtimestamp(int(ms) / 1000, timezone.utc).strftime('%Y-%m-%d')
    except (TypeError, ValueError, OverflowError):
        return ''


def _since_clause(since):
    if not since:
        return ''
    day = datetime.strptime(str(since).strip(), '%Y-%m-%d')   # ValueError if malformed
    return " AND date_wk >= DATE '{}'".format(day.strftime('%Y-%m-%d'))


class WHOCholeraClient:
    """WHO weekly cholera FeatureServer (public, no auth). No request at construction."""

    def __init__(self, base=None, timeout=DEFAULT_TIMEOUT):
        self.base = base or WHO_CHOLERA_FEATURESERVER
        self.timeout = timeout
        self.page = PAGE

    def _query(self, params):
        url = '{}/query?{}'.format(self.base, urlencode(dict(params, f='json')))
        req = Request(url, headers={'User-Agent': USER_AGENT, 'Accept': 'application/json'})
        try:
            with urlopen(req, timeout=self.timeout) as resp:
                body = json.loads(resp.read())
        except Exception as e:  # noqa: BLE001 - outage -> SourceUnavailable, else re-raised
            raise_unavailable('WHO cholera FeatureServer', e)
        err = body.get('error') if isinstance(body, dict) else None
        if err:
            # ArcGIS answers HTTP 200 with the error inside: read its code.
            code = int(err.get('code') or 0)
            msg = 'WHO cholera FeatureServer error {}: {}'.format(code, err.get('message'))
            if code >= 500 or code == 429:
                raise SourceUnavailable(msg)
            raise ValueError(msg)
        return body

    def weekly(self, iso3=None, since=None):
        """One row per country-week, oldest first.

        `since` ('YYYY-MM-DD') keeps the weeks starting on or after it. For a
        country absent from the dataset, NotCovered (it reported no cholera since
        2023); for a country present but quiet in the window, an empty list.
        """
        where = '1=1'
        if iso3:
            iso3 = normalize_iso3(iso3)
            where = "iso_3_code='{}'".format(iso3)
        where_win = where + _since_clause(since)
        rows, offset = [], 0
        while True:
            body = self._query({'where': where_win, 'outFields': _FIELDS,
                                'orderByFields': 'date_wk ASC,iso_3_code ASC',
                                'returnGeometry': 'false', 'resultOffset': offset,
                                'resultRecordCount': self.page})
            feats = body.get('features')
            if not isinstance(feats, list):
                raise ValueError('WHO cholera FeatureServer: no "features" list')
            for ft in feats:
                a = ft.get('attributes') or {}
                rows.append({
                    'iso3': str(a.get('iso_3_code') or '').upper(),
                    'country': a.get('adm0_name') or '',
                    'who_region': a.get('who_region') or '',
                    'week_start': _week(a.get('date_wk')),
                    'epiyr': a.get('epiyr'), 'epiwk': a.get('epiwk'),
                    'cases': a.get('cases'), 'deaths': a.get('deaths'),
                    'cum_cases': a.get('cum_case'), 'cum_deaths': a.get('cum_death'),
                })
            if not (body.get('exceededTransferLimit') or len(feats) >= self.page):
                break
            offset += len(feats)
            if not feats:
                break
        if iso3 and not rows:
            count = self._query({'where': where, 'returnCountOnly': 'true'}).get('count')
            if not count:
                raise NotCovered(
                    'WHO weekly cholera dataset has no row for {}: it lists only the '
                    'countries that reported cholera since 2023.'.format(iso3))
        return rows

    @staticmethod
    def save_csv(records, filepath):
        save_csv(records, filepath)


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    code = sys.argv[1] if len(sys.argv) > 1 else 'SDN'
    since = sys.argv[2] if len(sys.argv) > 2 else None
    weeks = WHOCholeraClient().weekly(code, since=since)
    print('=== WHO weekly cholera - {} : {} weeks ==='.format(code.upper(), len(weeks)))
    for w in weeks[-12:]:
        print('  {} cases {:>6} deaths {:>4} cumulative {}'.format(
            w['week_start'], w['cases'] or 0, w['deaths'] or 0, w['cum_cases']))
    for c in CAVEATS:
        print('  ! ' + c)
