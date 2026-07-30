"""
UNHCR Population API Client
=============================
Query UNHCR Refugee Data Finder for population statistics.
More granular than HAPI: time series, origin breakdowns, demographics.
Public API — no authentication required.

Usage:
    from unhcr_client import UNHCRClient
    unhcr = UNHCRClient()
    pop = unhcr.get_population('LBN', year_from=2020)
    demographics = unhcr.get_demographics('LBN', year=2024)
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
import time
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.parse import urlencode

from config import (
    UNHCR_BASE, DEFAULT_TIMEOUT, RATE_LIMIT_DELAY, USER_AGENT, save_csv
)


class WrongScopeError(Exception):
    """Raised when the API ignored our country filter and returned other data.

    The UNHCR API accepts unknown query parameters SILENTLY (no 4xx) and then
    returns GLOBAL AGGREGATES. Before 2026-07-25 this client sent four
    undocumented aliases (`country_of_asylum`, `country_of_origin`,
    `year_from`, `year_to`), so every CSV it ever wrote held world totals with
    empty country columns (verified: data/LBN/unhcr_population.csv showed
    30,490,994 refugees / 63,852,580 IDPs for "Lebanon"). This exception makes
    that failure mode impossible to ship again.
    """


def _num(v):
    """UNHCR measures: '-' means NOT AVAILABLE (distinct from 0), and types are
    mixed (refugees=2116011 int, asylum_seekers='0' str). Returns int or None."""
    if v is None:
        return None
    s = str(v).strip()
    if s in ('', '-'):
        return None
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return None


class UNHCRClient:
    """Client for UNHCR Population Statistics API (public, no auth).

    Parameter contract (verified against api.unhcr.org docs, 2026-07-25):
    `coa` = country of asylum, `coo` = country of origin, `yearFrom`/`yearTo`,
    and `cf_type='ISO'` to make the country filters speak ISO3 instead of
    internal UNHCR codes. Response carries coa/coa_iso/coa_name and
    coo/coo_iso/coo_name. Never send `coa_all`/`coo_all` alongside a specific
    country: they OVERRIDE the selection.
    """

    # Population groups (kept for reference; not a valid query param)
    GROUPS = ['REF', 'ASY', 'IDP', 'STA', 'OIP', 'OOC', 'HST']

    def __init__(self):
        self.base = UNHCR_BASE

    def _get(self, endpoint, params=None):
        """GET request to UNHCR API."""
        url = '{}/{}'.format(self.base, endpoint)
        if params:
            url += '?{}'.format(urlencode(params, doseq=True))
        req = Request(url, headers={
            'User-Agent': USER_AGENT,
            'Accept': 'application/json',
        })
        resp = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
        return resp

    @staticmethod
    def _assert_scope(records, iso3, key, endpoint):
        """Post-condition: the response must actually describe the country asked.

        This is the guard that the four dead parameter aliases defeated.
        """
        if not iso3 or not records:
            return
        want = iso3.upper()
        seen = {str(r.get(key, '')).upper() for r in records}
        if want not in seen:
            raise WrongScopeError(
                'UNHCR {}: asked for {}={} but the response contains only {} '
                '-> the filter was IGNORED (global aggregates). Refusing to '
                'return foreign data.'.format(
                    endpoint, key, want,
                    sorted(s for s in seen if s)[:5] or ['no country at all']))

    def _paginate(self, endpoint, params):
        """Page through an endpoint. Uses the envelope's `maxPages`.

        Never read `total` from the envelope: it is an empty list, not a count.
        """
        all_items = []
        page = 1
        while True:
            params['page'] = page
            params['limit'] = 1000
            resp = self._get(endpoint, params)
            items = resp.get('items', [])
            all_items.extend(items)
            max_pages = resp.get('maxPages') or 1
            if page >= max_pages or not items:
                break
            page += 1
            time.sleep(RATE_LIMIT_DELAY)
        return all_items

    def get_population(self, country_asylum=None, country_origin=None,
                       year_from=None, year_to=None, population_group=None):
        """Fetch population statistics by country of asylum OR origin.

        `country_asylum` = people hosted IN that country.
        `country_origin` = nationals of that country displaced ANYWHERE.
        These are different questions: pass exactly one.

        Returns list of dicts using UNHCR's own column names (coa/coo family),
        so outputs join with the official `refugees` R package and Refugee Data
        Finder exports. Measures are int or None ('-' = not available, NOT 0).
        Population groups are NOT mutually exclusive (`hst` = host community,
        `oip` overlaps): never sum them into a total.
        """
        if country_asylum and country_origin:
            raise ValueError('Pass either country_asylum or country_origin, '
                             'not both: they answer different questions.')
        params = {'cf_type': 'ISO'}
        if country_asylum:
            params['coa'] = country_asylum.upper()
        if country_origin:
            params['coo'] = country_origin.upper()
        # yearFrom ALONE is silently ignored (observed 2026-07-25: yearFrom=2022
        # returned 1964..2025 unfiltered, while yearFrom+yearTo returned exactly
        # the requested range). The range only applies when BOTH bounds are sent,
        # so default the upper bound rather than shipping an unfiltered series.
        if year_from:
            params['yearFrom'] = int(year_from)
            params['yearTo'] = int(year_to) if year_to else datetime.now().year
        elif year_to:
            params['yearTo'] = int(year_to)
        if population_group:
            # Not a documented filter: keep the argument for API compatibility
            # but do not send an unknown param (it would be silently ignored).
            print('  UNHCR: population_group filter is not supported by the API '
                  '- ignoring it (filter client-side on the returned columns).')

        items = self._paginate('population/', params)

        all_records = []
        for item in items:
            all_records.append({
                'year': item.get('year', ''),
                'coa': item.get('coa', ''),
                'coa_iso': item.get('coa_iso', ''),
                'coa_name': item.get('coa_name', ''),
                'coo': item.get('coo', ''),
                'coo_iso': item.get('coo_iso', ''),
                'coo_name': item.get('coo_name', ''),
                'refugees': _num(item.get('refugees')),
                'asylum_seekers': _num(item.get('asylum_seekers')),
                'returned_refugees': _num(item.get('returned_refugees')),
                'idps': _num(item.get('idps')),
                'returned_idps': _num(item.get('returned_idps')),
                'stateless': _num(item.get('stateless')),
                'oip': _num(item.get('oip')),
                'ooc': _num(item.get('ooc')),
                'hst': _num(item.get('hst')),
            })

        if country_asylum:
            self._assert_scope(all_records, country_asylum, 'coa_iso', 'population')
        if country_origin:
            self._assert_scope(all_records, country_origin, 'coo_iso', 'population')
        return all_records

    def get_demographics(self, country_asylum, year=None):
        """Fetch demographic breakdown (age, sex) for refugees in a country.

        This is UNHCR's ONLY age/sex disaggregation. There is NO disability
        breakdown anywhere in this API (verified 2026-07-25 against the docs and
        the official `refugees` R package): for disability, use HAPI
        humanitarian-needs, the World Bank Disability Data Hub, or MSNA WG-SS
        microdata.
        """
        params = {'cf_type': 'ISO', 'coa': country_asylum.upper()}
        if year:
            params['yearFrom'] = int(year)
            params['yearTo'] = int(year)

        items = self._paginate('demographics/', params)

        all_records = []
        for item in items:
            all_records.append({
                'year': item.get('year', ''),
                'coa': item.get('coa', ''),
                'coa_iso': item.get('coa_iso', ''),
                'coa_name': item.get('coa_name', ''),
                'coo': item.get('coo', ''),
                'coo_iso': item.get('coo_iso', ''),
                'female_0_4': _num(item.get('f_0_4')),
                'female_5_11': _num(item.get('f_5_11')),
                'female_12_17': _num(item.get('f_12_17')),
                'female_18_59': _num(item.get('f_18_59')),
                'female_60_plus': _num(item.get('f_60')),
                'male_0_4': _num(item.get('m_0_4')),
                'male_5_11': _num(item.get('m_5_11')),
                'male_12_17': _num(item.get('m_12_17')),
                'male_18_59': _num(item.get('m_18_59')),
                'male_60_plus': _num(item.get('m_60')),
                'total': _num(item.get('total')),
            })

        self._assert_scope(all_records, country_asylum, 'coa_iso', 'demographics')
        return all_records

    def get_solutions(self, country_asylum=None, country_origin=None,
                      year_from=None, year_to=None):
        """Fetch durable solutions data (returns, resettlement, naturalisation)."""
        if country_asylum and country_origin:
            raise ValueError('Pass either country_asylum or country_origin, not both.')
        params = {'cf_type': 'ISO'}
        if country_asylum:
            params['coa'] = country_asylum.upper()
        if country_origin:
            params['coo'] = country_origin.upper()
        if year_from:
            params['yearFrom'] = int(year_from)
        if year_to:
            params['yearTo'] = int(year_to)

        items = self._paginate('solutions/', params)

        all_records = []
        for item in items:
            all_records.append({
                'year': item.get('year', ''),
                'coa': item.get('coa', ''),
                'coa_iso': item.get('coa_iso', ''),
                'coo': item.get('coo', ''),
                'coo_iso': item.get('coo_iso', ''),
                'returned_refugees': _num(item.get('returned_refugees')),
                'resettlement': _num(item.get('resettlement')),
                'naturalisation': _num(item.get('naturalisation')),
                'complementary_pathways': _num(item.get('complementary_pathways')),
            })

        if country_asylum:
            self._assert_scope(all_records, country_asylum, 'coa_iso', 'solutions')
        if country_origin:
            self._assert_scope(all_records, country_origin, 'coo_iso', 'solutions')
        return all_records

    @staticmethod
    def save_csv(records, filepath):
        """Save records to CSV."""
        save_csv(records, filepath)


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    iso3 = sys.argv[1] if len(sys.argv) > 1 else 'LBN'
    unhcr = UNHCRClient()

    def _f(v):
        return 'n/a' if v is None else '{:,}'.format(v)

    print('=== UNHCR Population - {} (country of asylum) ==='.format(iso3))
    pop = unhcr.get_population(country_asylum=iso3, year_from=2018)
    print('\nPopulation by year (NO total: population groups are not mutually '
          'exclusive - hst is the host community, oip overlaps others):')
    for p in pop[:25]:
        print('  {} [{}] REF {} | ASY {} | IDP {} | STA {} [origin {}]'.format(
            p['year'], p['coa_iso'], _f(p['refugees']), _f(p['asylum_seekers']),
            _f(p['idps']), _f(p['stateless']), p['coo_name'] or p['coo_iso'] or '-'))
    print('  ({} rows total)'.format(len(pop)))

    solutions = unhcr.get_solutions(country_asylum=iso3, year_from=2020)
    print('\nSolutions:')
    for s in solutions[:15]:
        print('  {} [{}] Returned {} | Resettled {} | Naturalised {}'.format(
            s['year'], s['coa_iso'], _f(s['returned_refugees']),
            _f(s['resettlement']), _f(s['naturalisation'])))
