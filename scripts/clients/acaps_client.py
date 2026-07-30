"""
ACAPS Crisis API Client
========================
Query ACAPS for crisis overviews, INFORM severity, access constraints.
Requires free API key - register at https://api.acaps.org/

Usage:
    from acaps_client import ACAPSClient
    acaps = ACAPSClient()
    crises = acaps.get_crisis_list()
    info = acaps.get_crisis_info('lebanon')
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
import os
import time
from urllib.request import Request, urlopen
from urllib.parse import urlencode

from config import (
    ACAPS_BASE, DEFAULT_TIMEOUT, RATE_LIMIT_DELAY, USER_AGENT, save_csv,
    get_credential
)


def _first(v):
    """ACAPS returns iso3/country as lists; take the first (or the scalar)."""
    return v[0] if isinstance(v, list) and v else (v or '')


class ACAPSClient:
    """Client for ACAPS Crisis API (requires free API key)."""

    def __init__(self, api_key=None):
        self.base = ACAPS_BASE
        # OS keychain first, then env var. No hard keyring dependency.
        self.api_key = api_key or get_credential(
            'sds.acaps', 'api_key', 'ACAPS_API_KEY')

    def _get(self, endpoint, params=None):
        """GET request to ACAPS API."""
        url = '{}/{}'.format(self.base, endpoint)
        if params:
            url += '?{}'.format(urlencode(params, doseq=True))
        headers = {
            'User-Agent': USER_AGENT,
            'Accept': 'application/json',
        }
        if self.api_key:
            headers['Authorization'] = 'Token {}'.format(self.api_key)
        req = Request(url, headers=headers)
        resp = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
        return resp

    def get_crisis_list(self, status='ongoing'):
        """Get list of crises.

        The API no longer accepts a 'status' query filter (2026-07-24:
        "Field status does not exist"); we fetch all and filter client-side
        on the 'active' flag. status='ongoing' -> active, 'past' -> inactive,
        None/other -> all.
        """
        params = {}
        all_records = []
        url_next = True
        while url_next:
            try:
                resp = self._get('crises/', params)
            except Exception as e:
                print('  ACAPS crises: {}'.format(e))
                break
            results = resp.get('results', resp) if isinstance(resp, dict) else resp
            if isinstance(results, list):
                for c in results:
                    drivers = c.get('drivers', '')
                    all_records.append({
                        'id': c.get('crisis_id', ''),
                        'name': c.get('crisis_name', ''),
                        'country': _first(c.get('country', '')),
                        'iso3': _first(c.get('iso3', '')),
                        'active': c.get('active', ''),
                        'drivers': ', '.join(drivers) if isinstance(drivers, list) else drivers,
                        'date_created': c.get('date_created', ''),
                    })
            url_next = resp.get('next') if isinstance(resp, dict) else None
            if url_next:
                params['page'] = params.get('page', 1) + 1
                time.sleep(RATE_LIMIT_DELAY)
            else:
                break

        if status == 'ongoing':
            all_records = [c for c in all_records if c.get('active') in (True, 'Yes', 'yes', 1)]
        elif status == 'past':
            all_records = [c for c in all_records if c.get('active') in (False, 'No', 'no', 0)]
        return all_records

    def get_inform_severity(self, iso3=None):
        """Get INFORM Severity Index data.

        Endpoint renamed to `inform-severity-index/` and fields to Title Case
        (verified 2026-07-24). iso3/country come back as lists.
        """
        params = {}
        if iso3:
            params['iso3'] = iso3.upper()

        all_records = []
        try:
            resp = self._get('inform-severity-index/', params)
        except Exception as e:
            print('  ACAPS inform-severity: {}'.format(e))
            return []
        results = resp.get('results', resp) if isinstance(resp, dict) else resp
        if isinstance(results, list):
            for item in results:
                all_records.append({
                    'country': _first(item.get('country', '')),
                    'iso3': _first(item.get('iso3', '')),
                    'crisis_name': item.get('crisis_name', ''),
                    'severity_score': item.get('INFORM Severity Index', ''),
                    'severity_class': item.get('INFORM Severity category', ''),
                    'severity_numeric': item.get('INFORM Severity category (numeric)', ''),
                    'impact': item.get('Impact of the crisis', ''),
                    'humanitarian_conditions': item.get('Conditions of affected people', ''),
                    'complexity': item.get('Complexity', ''),
                    # ⚠ C'est un SCORE INFORM Severity de 0 a 10, PAS un effectif.
                    # Nomme 'People in need' par l'API, il a ete lu comme un nombre de
                    # personnes (un 9.5 soudanais publie comme "10 M de personnes").
                    # Le nom de colonne porte desormais l'echelle.
                    'pin_score_0_10': item.get('People in need', ''),
                    'pin_score_scale': '0-10 (INFORM Severity), PAS un effectif',
                    'last_updated': item.get('Last updated', ''),
                })

        return all_records

    def get_access_constraints(self, iso3=None):
        """Get humanitarian access scores (ACAPS Humanitarian Access dataset).

        Endpoint renamed to `humanitarian-access/` (verified 2026-07-24).
        New shape: one row per crisis with a 0-5 composite `ACCESS` score,
        3 pillar scores (P1-P3) and 9 indicator scores (I1-I9) - see the
        ACAPS Humanitarian Access methodology for label definitions.
        """
        params = {}
        if iso3:
            params['iso3'] = iso3.upper()

        all_records = []
        try:
            resp = self._get('humanitarian-access/', params)
        except Exception as e:
            print('  ACAPS humanitarian-access unavailable: {}'.format(e))
            return []
        results = resp.get('results', resp) if isinstance(resp, dict) else resp
        if isinstance(results, list):
            for item in results:
                rec = {
                    'crisis_id': item.get('crisis_id', ''),
                    'crisis_name': item.get('crisis_name', ''),
                    'country': _first(item.get('country', '')),
                    'iso3': _first(item.get('iso3', '')),
                    'access_score': item.get('ACCESS', ''),
                    'pillar1': item.get('P1', ''),
                    'pillar2': item.get('P2', ''),
                    'pillar3': item.get('P3', ''),
                    'info_gap': item.get('Infogap', ''),
                }
                for i in range(1, 10):
                    rec['i{}'.format(i)] = item.get('I{}'.format(i), '')
                all_records.append(rec)

        return all_records

    @staticmethod
    def save_csv(records, filepath):
        """Save records to CSV."""
        save_csv(records, filepath)


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    iso3 = sys.argv[1] if len(sys.argv) > 1 else 'LBN'
    acaps = ACAPSClient()

    print('=== ACAPS - {} ==='.format(iso3))

    severity = acaps.get_inform_severity(iso3)
    if severity:
        for s in severity:
            print('Severity: {} ({}) - Impact {}, Conditions {}, Complexity {}'.format(
                s['severity_score'], s['severity_class'],
                s['impact'], s['humanitarian_conditions'], s['complexity']))

    access = acaps.get_access_constraints(iso3)
    print('\nAccess constraints: {}'.format(len(access)))
    for a in access[:5]:
        print('  [{}] {} - {}'.format(
            a['severity'], a['constraint_type'], a['description'][:80]))
