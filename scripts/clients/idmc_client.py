"""
IDMC REST Client
=================
Query Internal Displacement Monitoring Centre for historical displacement data.
Uses the new REST API (Helix Tools) — replaces the deprecated GraphQL endpoint.

Auth: client_id required (free, email ch.datainfo@idmc.ch to request).
Set env var IDMC_CLIENT_ID.

Endpoints:
  - GIDD: validated annual displacement data
  - IDU: Internal Displacement Updates (near-real-time events)

Usage:
    from idmc_client import IDMCClient
    idmc = IDMCClient()  # reads IDMC_CLIENT_ID from env
    data = idmc.get_displacement('LBN')
    events = idmc.get_displacement_events('SDN', year_from=2023)
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import json
from urllib.request import Request, urlopen
from urllib.parse import urlencode

from config import DEFAULT_TIMEOUT, USER_AGENT, save_csv, get_credential

IDMC_REST_BASE = 'https://helix-tools-api.idmcdb.org/external-api'


class IDMCClient:
    """Client for IDMC REST API (Helix Tools).

    Requires IDMC_CLIENT_ID env var. If not set, methods return empty
    results with a warning (graceful degradation).
    """

    def __init__(self, client_id=None):
        self.base = IDMC_REST_BASE
        # OS keychain first, then env var. No hard keyring dependency.
        self.client_id = client_id or get_credential(
            'sds.idmc', 'client_id', 'IDMC_CLIENT_ID')
        if not self.client_id:
            print('  IDMC: No credentials — IDMC queries will be skipped.')
            print('  Set via: keyring sds.idmc/client_id or env var IDMC_CLIENT_ID')
            print('  Register free at: email ch.datainfo@idmc.ch')

    def _get(self, endpoint, params=None, timeout=None):
        """GET sur l'API REST IDMC, avec `client_id` et decompression gzip.

        ⚠ `idus/all/` renvoie du **gzip** meme sans `Accept-Encoding` : 9,2 Mo
        compresses pour 97,9 Mo de JSON (62 502 enregistrements). Sans decompression
        explicite, `json.loads` echoue sur des octets binaires et l'appel se termine
        en `except -> []`, ce qui se lit comme "pas de donnee".
        """
        if not self.client_id:
            return {}
        if params is None:
            params = {}
        params['client_id'] = self.client_id
        url = '{}/{}?{}'.format(self.base, endpoint, urlencode(params, doseq=True))
        req = Request(url, headers={
            'User-Agent': USER_AGENT,
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip',
        })
        resp = urlopen(req, timeout=timeout or DEFAULT_TIMEOUT)
        raw = resp.read()
        if raw[:2] == b'\x1f\x8b' or resp.headers.get('content-encoding') == 'gzip':
            import gzip
            raw = gzip.decompress(raw)
        return json.loads(raw)

    def get_idus_events(self, iso3, timeout=180):
        """Evenements de deplacement IDUS pour un pays, filtres COTE CLIENT.

        `idus/all/` ne prend pas de filtre pays : il rend le flux mondial (62 502
        lignes, 9,2 Mo gzip). Le filtrage se fait donc en local sur le champ `iso3`,
        **qui est present dans la reponse** : ne PAS le fabriquer depuis l'argument
        d'entree, sinon toute ligne du flux mondial serait etiquetee au pays demande.

        Verifie 2026-07-25 : 1 066 lignes SDN dans le flux mondial.
        """
        if not self.client_id:
            return []
        try:
            data = self._get('idus/all/', timeout=timeout)
        except Exception as e:
            print('  IDMC idus/all: {}'.format(str(e)[:110]))
            return []
        rows = data if isinstance(data, list) else (data.get('results') or [])
        want = iso3.upper()
        out = [r for r in rows if str(r.get('iso3', '')).upper() == want]
        # Post-condition : le flux est mondial, l'assertion porte donc sur le filtre.
        wrong = sorted({str(r.get('iso3')) for r in out
                        if str(r.get('iso3', '')).upper() != want})
        if wrong:
            raise ValueError('IDMC: filtrage iso3 casse, pays parasites {}'.format(wrong[:4]))
        print('  IDMC idus: {} evenements {} sur {} lignes mondiales'.format(
            len(out), want, len(rows)))
        return out

    def get_displacement(self, iso3, year_from=None, year_to=None, cause=None):
        """Get annual displacement figures (GIDD validated data).

        Args:
            iso3: Country ISO3 code
            year_from: Start year (e.g. 2018)
            year_to: End year (e.g. 2025)
            cause: CONFLICT or DISASTER (or None for both)

        Returns:
            List of dicts: year, conflict_new_displacements, disaster_new_displacements,
            conflict_stock, disaster_stock.
        """
        if not self.client_id:
            return []

        params = {'iso3__in': iso3.upper()}
        if year_from:
            params['start_year'] = str(int(year_from))
        if year_to:
            params['end_year'] = str(int(year_to))
        if cause:
            params['cause'] = cause.upper()

        try:
            data = self._get('gidd/displacements/', params)
        except Exception as e:
            print('  IDMC displacements: {}'.format(e))
            return []

        results = data.get('results', []) if isinstance(data, dict) else data if isinstance(data, list) else []

        # GIDD schema (verified 2026-07-24): one row per (country, year) with
        # conflict + disaster columns side by side (NOT separate cause rows).
        def _num(v):
            try:
                return int(v) if v is not None else 0
            except (TypeError, ValueError):
                return 0

        by_year = {}
        for r in results:
            year = r.get('year', 0)
            by_year[year] = {
                # Lu de la REPONSE quand il y est ; l'argument n'est qu'un repli.
                # Fabriquer l'iso3 depuis l'entree etiquette au pays demande
                # n'importe quelle ligne qui aurait echappe au filtre.
                'iso3': str(r.get('iso3') or iso3).upper(),
                'year': year,
                'conflict_new_displacements': _num(r.get('conflict_new_displacement')),
                'disaster_new_displacements': _num(r.get('disaster_new_displacement')),
                'conflict_stock': _num(r.get('conflict_total_displacement')),
                'disaster_stock': _num(r.get('disaster_total_displacement')),
            }

        return sorted(by_year.values(), key=lambda r: r['year'])

    def get_displacement_events(self, iso3, year_from=None, year_to=None, cause=None):
        """Get individual displacement events (IDU — near-real-time).

        Args:
            iso3: Country ISO3 code
            year_from: Start year
            year_to: End year
            cause: CONFLICT or DISASTER

        Returns:
            List of event dicts: event_name, year, displacement_type,
            new_displacements, cause, start_date, end_date.
        """
        if not self.client_id:
            return []

        params = {'iso3__in': iso3.upper()}
        if year_from:
            params['start_year'] = str(int(year_from))
        if year_to:
            params['end_year'] = str(int(year_to))
        if cause:
            params['cause'] = cause.upper()

        try:
            data = self._get('idu/all/', params)
        except Exception as e:
            print('  IDMC events: {}'.format(e))
            return []

        results = data.get('results', []) if isinstance(data, dict) else data if isinstance(data, list) else []

        records = []
        for e in results:
            records.append({
                # Lu de la REPONSE quand il y est ; l'argument n'est qu'un repli.
                # Fabriquer l'iso3 depuis l'entree etiquette au pays demande
                # n'importe quelle ligne qui aurait echappe au filtre.
                'iso3': str(e.get('iso3') or iso3).upper(),
                'event_id': e.get('id', ''),
                'event_name': e.get('event_name', '') or e.get('name', ''),
                'year': e.get('year', ''),
                'displacement_type': e.get('displacement_type', ''),
                'new_displacements': e.get('figure', 0) or e.get('new_displacements', 0) or 0,
                'cause': e.get('cause', ''),
                'start_date': e.get('start_date', '') or e.get('date', ''),
                'end_date': e.get('end_date', ''),
            })

        return records

    def get_disasters(self, iso3, year_from=None, year_to=None):
        """Get disaster displacement data from GIDD.

        Returns list of disaster event records.
        """
        if not self.client_id:
            return []

        params = {'iso3__in': iso3.upper()}
        if year_from:
            params['start_year'] = str(int(year_from))
        if year_to:
            params['end_year'] = str(int(year_to))

        try:
            data = self._get('gidd/disasters/', params)
        except Exception as e:
            print('  IDMC disasters: {}'.format(e))
            return []

        results = data.get('results', []) if isinstance(data, dict) else data if isinstance(data, list) else []

        records = []
        for d in results:
            records.append({
                # Lu de la REPONSE quand il y est ; l'argument n'est qu'un repli.
                # Fabriquer l'iso3 depuis l'entree etiquette au pays demande
                # n'importe quelle ligne qui aurait echappe au filtre.
                'iso3': str(d.get('iso3') or iso3).upper(),
                'event_id': d.get('id', ''),
                'event_name': d.get('event_name', '') or d.get('name', ''),
                'year': d.get('year', ''),
                'hazard_type': d.get('hazard_type', ''),
                'new_displacements': d.get('new_displacements', 0) or d.get('new_displacement', 0) or 0,
                'start_date': d.get('start_date', ''),
                'end_date': d.get('end_date', ''),
            })
        return records

    def get_country_overview(self, iso3):
        """Get country-level overview (latest stock figures).

        Uses GIDD displacements for the most recent year available.
        Returns dict with total stock and latest figures.
        """
        data = self.get_displacement(iso3, year_from=2020)
        if not data:
            return {}

        latest = data[-1]  # Most recent year
        return {
            # Lu de la REPONSE quand il y est ; l'argument n'est qu'un repli.
                # Fabriquer l'iso3 depuis l'entree etiquette au pays demande
                # n'importe quelle ligne qui aurait echappe au filtre.
            'iso3': iso3.upper(),   # agregat construit localement, pas une ligne d'API
            'name': '',  # REST API doesn't return country name in displacement endpoint
            'latest_year': latest['year'],
            'total_stock': latest['conflict_stock'] + latest['disaster_stock'],
            'conflict_stock': latest['conflict_stock'],
            'disaster_stock': latest['disaster_stock'],
            'total_new': latest['conflict_new_displacements'] + latest['disaster_new_displacements'],
        }

    @staticmethod
    def save_csv(records, filepath):
        """Save records to CSV."""
        save_csv(records, filepath)


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    iso3 = sys.argv[1] if len(sys.argv) > 1 else 'LBN'
    idmc = IDMCClient()

    print('=== IDMC (REST) — {} ==='.format(iso3))

    if not idmc.client_id:
        print('Set IDMC_CLIENT_ID env var to test. Exiting.')
        sys.exit(0)

    overview = idmc.get_country_overview(iso3)
    if overview:
        print('Latest year: {} — Stock: {:,} IDPs'.format(
            overview['latest_year'], overview['total_stock']))

    data = idmc.get_displacement(iso3, year_from=2020)
    print('\nAnnual displacement (since 2020): {} years'.format(len(data)))
    for d in data:
        print('  {} — Conflict: {:,} new / {:,} stock | Disaster: {:,} new / {:,} stock'.format(
            d['year'], d['conflict_new_displacements'], d['conflict_stock'],
            d['disaster_new_displacements'], d['disaster_stock']))

    events = idmc.get_displacement_events(iso3, year_from=2023)
    print('\nIDU events (since 2023): {}'.format(len(events)))
    for e in events[:10]:
        print('  [{}] {} — {:,} displaced ({})'.format(
            e['year'], e['event_name'][:50], e['new_displacements'], e['cause']))
