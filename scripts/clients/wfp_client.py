"""
WFP HungerMap LIVE Client
===========================
Query WFP HungerMap for near-real-time food security indicators.
Public API - no authentication required.
Note: API uses numeric country IDs, not ISO3 codes.

Usage:
    from wfp_client import WFPClient
    wfp = WFPClient()
    data = wfp.get_country_data('LBN')
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
from urllib.request import Request, urlopen

from config import (
    WFP_HUNGERMAP_BASE as WFP_BASE, DEFAULT_TIMEOUT, USER_AGENT, save_csv
)

# WFP HungerMap numeric country IDs (GAUL-style adm0 codes).
# RE-DERIVED + VERIFIED 2026-07-24: brute-force scan of /v2/adm0/{id}/adm1data.json
# (ids 1-1000, multi-round - the API drops ~60% of requests randomly), each id
# matched against cdn.hungermapdata.org/hungermap/adm1_labels.geojson admin1
# names (match score 1.00 for every entry below). The pre-2026 map had several
# WRONG ids (e.g. SDN=276 is Fiji - 4 divisions Central/Eastern/Northern/Western,
# pop 0.88M). Do NOT re-add ids without fingerprint verification.
WFP_COUNTRY_IDS = {
    'AFG': 1, 'BDI': 43, 'COD': 68, 'HTI': 108, 'IRQ': 118,
    'KEN': 133, 'LBN': 141, 'LBY': 145, 'MLI': 155, 'MMR': 273,
    'MOZ': 170, 'NER': 181, 'NGA': 182, 'SOM': 226, 'SYR': 238,
    'TCD': 50, 'UKR': 254, 'YEM': 269,
}

# Countries HungerMap does NOT serve publicly (found 2026-07-24 in the app
# bundle's special-display geojson layer: India, Pakistan, Ethiopia, Sudan,
# Palestine, DPRK, Turkey, Venezuela + small islands). Their countryData.json
# returns errors even with the right id - this is data policy, not a bug.
# For these, use HAPI food-security (IPC) instead.
WFP_NOT_PUBLIC = {'ETH': 79, 'IND': 115, 'PAK': 188, 'PRK': 67,
                  'PSE': 1011, 'SDN': 40764, 'SSD': 74, 'TUR': 249, 'VEN': 263}


# IPC global : le SEUL canal WFP qui sert les pays absents de HungerMap.
# Verifie 2026-07-25 : HTTP 200, 56 pays, dont SDN et SSD (que countryData.json refuse).
WFP_IPC_GLOBAL = ('https://ew-tool-api.hungermapdata.org'
                  '/ew/v1/ipc/food/insecurity/global/recent')


class WFPClient:
    """Client for WFP HungerMap LIVE API (public, no auth)."""

    def __init__(self):
        self.base = WFP_BASE

    # ── IPC global (comble le trou HungerMap : SDN, SSD…) ────

    def get_ipc(self, iso3=None):
        """Derniere analyse IPC par pays (56 pays, sans cle).

        Debloque les pays que HungerMap ne sert pas publiquement (SDN, SSD…).

        ⚠ DEUX PIEGES, mesures le 2026-07-25 sur le Soudan.

        1. **`phase35` = IPC 3+**, pas `phase3`. `phase3Population` (3 126 748) est la
           phase 3 SEULE ; l'agregat "IPC 3+" est `phase35Population` (5 574 076).
           Servir phase3 sous-estime de 44 %.

        2. **Le denominateur est la population ANALYSEE, pas la population du pays**,
           et l'enregistrement "recent" est souvent une PROJECTION dont le perimetre
           geographique est bien plus etroit que l'analyse courante. Confronte a HAPI
           food-security (meme source IPC, filtre 1 periode + 1 type + admin0) :

               Soudan, current (fev-mai 2026)          : 3+ = 19 466 533 / 47 535 794 (41 %)
               Soudan, first projection (juin-sept 26) : 3+ =  5 574 078 /  8 289 603 (67 %)
               ce endpoint, referencePeriod "Jun 2026 - Sep 2026 (Projection)" : 5 574 076

           Les deux sont vrais et repondent a des questions differentes. Presenter les
           5,57 M comme "l'insecurite alimentaire au Soudan" sous-estime de 3,5x le
           chiffre national courant. TOUJOURS citer `reference_period` avec le chiffre,
           et pour un headline national courant passer par HAPI food-security
           (`ipc_type='current'`, derniere periode, admin0).

        `analysed_population_implied` est derive (population / pourcentage) pour rendre
        ce perimetre VISIBLE : l'API ne l'expose pas.

        Args:
            iso3: filtre optionnel. Si le pays n'est pas servi, renvoie [] et le dit
                  (56 pays seulement) — pas de silence.

        Returns: liste de dicts a plat, prets pour CSV.
        """
        req = Request(WFP_IPC_GLOBAL, headers={
            'User-Agent': USER_AGENT, 'Accept': 'application/json'})
        try:
            raw = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
        except Exception as e:
            print('  WFP IPC global: {}'.format(str(e)[:90]))
            return []
        rows = raw if isinstance(raw, list) else (raw.get('data') or [])

        def pop(rec, key):
            v = rec.get(key + 'Population')
            return int(v) if isinstance(v, (int, float)) else None

        def pct(rec, key):
            v = rec.get(key + 'Percentage')
            return float(v) if isinstance(v, (int, float)) else None

        out = []
        for rec in rows:
            code = str(rec.get('iso3Alpha3', '')).upper()
            p35, f35 = pop(rec, 'phase35'), pct(rec, 'phase35')
            # Perimetre implicite : l'API donne un effectif et une part, jamais le
            # denominateur. On le derive pour que le perimetre soit lisible.
            analysed = int(round(p35 / f35)) if (p35 and f35) else None
            period = str(rec.get('referencePeriod', ''))
            out.append({
                'iso3': code,
                'reference_period': period,
                'is_projection': 'projection' in period.lower(),
                'data_source': rec.get('dataSource', ''),
                'analysis_date': str(rec.get('analysisDate') or
                                     rec.get('dateOfAnalysis') or '')[:10],
                'ipc3plus_population': p35,
                'ipc3plus_fraction': f35,
                'phase3_only_population': pop(rec, 'phase3'),
                'phase4_population': pop(rec, 'phase4'),
                'phase45_population': pop(rec, 'phase45'),
                'phase5_population': pop(rec, 'phase5'),
                'analysed_population_implied': analysed,
                'scope_note': ('denominateur = population ANALYSEE ({}), pas la '
                               'population du pays'.format(analysed) if analysed
                               else 'perimetre indeterminable'),
            })
        if iso3:
            code = iso3.upper()
            hit = [r for r in out if r['iso3'] == code]
            if not hit:
                print('  WFP IPC global: {} absent de cet endpoint ({} pays servis)'
                      .format(code, len(out)))
            return hit
        return out

    def _get(self, endpoint):
        """GET request to WFP HungerMap API."""
        url = '{}/{}'.format(self.base, endpoint)
        req = Request(url, headers={
            'User-Agent': USER_AGENT,
            'Accept': 'application/json',
        })
        resp = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
        return resp

    def _resolve_id(self, iso3):
        """Convert ISO3 to WFP numeric country ID.

        Order: (1) NOT_PUBLIC list -> explicit refusal (HungerMap withholds
        these countries; not a client bug); (2) fingerprint-VERIFIED map;
        (3) legacy dynamic resolver (dead since the 2026 restructuring, kept
        as a cheap future-proofing attempt); (4) raise.
        """
        iso3 = iso3.upper()
        if iso3 in WFP_NOT_PUBLIC:
            raise ValueError(
                'HungerMap does not publicly serve {} (restricted-display list '
                'in the WFP app itself). Use HAPI food-security (IPC).'.format(iso3))
        cid = WFP_COUNTRY_IDS.get(iso3)
        if cid:
            return cid
        cid = self._resolve_id_dynamic(iso3)
        if cid:
            return cid
        raise ValueError(
            'No verified WFP id for {} - derive it via adm1-name fingerprint '
            'matching before adding to WFP_COUNTRY_IDS (see 2026-07-24 note).'.format(iso3))

    def _resolve_id_dynamic(self, iso3):
        """Resolve ISO3 to WFP ID dynamically via v1 API.

        The v1 API accepts ISO3 and returns the numeric country ID in the response.
        Returns int ID or None if resolution fails.
        """
        try:
            url = 'https://api.hungermapdata.org/v1/foodsecurity/country/{}/region?date_start=2026-01-01'.format(iso3)
            req = Request(url, headers={
                'User-Agent': USER_AGENT,
                'Accept': 'application/json',
            })
            resp = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
            # Response includes country object with id
            country = resp.get('country', {})
            cid = country.get('id')
            if cid:
                # Cache for future use
                WFP_COUNTRY_IDS[iso3] = cid
                return cid
        except Exception:
            pass
        return None

    def get_country_data(self, iso3):
        """Get food security overview for a country.

        Returns dict with: fcs, rcsi, population data.
        """
        try:
            cid = self._resolve_id(iso3)
            data = self._get('adm0/{}/countryData.json'.format(cid))
        except Exception as e:
            print('  WFP: {}'.format(e))
            return {}

        if not data or not isinstance(data, dict):
            return {}

        # Check for API error response
        if 'error' in data:
            print('  WFP error: {}'.format(data['error']))
            return {}

        result = {
            'iso3': iso3.upper(),
            'country_name': data.get('country', {}).get('name', ''),
        }

        # Population - legacy dict {number: N} or v2 float
        pop = data.get('population')
        if isinstance(pop, dict):
            result['population'] = pop.get('number', 0) or 0
        elif isinstance(pop, (int, float)):
            result['population'] = pop
        result['population_source'] = data.get('populationSource', '')

        # FCS - v2 schema (2026): top-level 'fcs' is a FLOAT in MILLIONS of
        # people (verified: id 276 fcs=0.1941 == fcsGraph 194,745 people;
        # AFG fcs=23.07 == ~23M). The fcsGraph carries the absolute daily
        # people series. Legacy schema: dict {people, prevalence}.
        pop_m = result.get('population')  # millions in the v2 float schema
        fcs = data.get('fcs')
        if isinstance(fcs, dict):
            result['fcs_people_insufficient'] = fcs.get('people', 0) or 0
            result['fcs_prevalence_insufficient'] = fcs.get('prevalence', 0) or 0
        elif isinstance(fcs, (int, float)):
            graph = data.get('fcsGraph') or []
            last = graph[-1] if graph and isinstance(graph[-1], dict) else {}
            result['fcs_people_insufficient'] = last.get('fcs') or int(fcs * 1_000_000)
            if pop_m:
                result['fcs_prevalence_insufficient'] = round(fcs / pop_m, 4)

        # rCSI - same dict-or-float(millions) handling
        rcsi = data.get('rcsi')
        if isinstance(rcsi, dict):
            result['rcsi_people_crisis'] = rcsi.get('people', 0) or 0
            result['rcsi_prevalence_crisis'] = rcsi.get('prevalence', 0) or 0
        elif isinstance(rcsi, (int, float)):
            graph = data.get('rcsiGraph') or []
            last = graph[-1] if graph and isinstance(graph[-1], dict) else {}
            result['rcsi_people_crisis'] = last.get('rcsi') or int(rcsi * 1_000_000)
            if pop_m:
                result['rcsi_prevalence_crisis'] = round(rcsi / pop_m, 4)

        return result

    def get_subnational(self, iso3):
        """Get subnational (admin1) food security data.

        v2 schema (2026, verified): `adm1data.json` - LOWERCASE, the endpoint
        is case-sensitive since the restructuring (`adm1Data.json` -> 404) -
        returns geojson-like {features: [{properties: {Code, Name,
        fcs: {ratio(%), people, ...}, rcsi: {...}, centroid, fcsGraph}}]}.
        """
        try:
            cid = self._resolve_id(iso3)
            data = self._get('adm0/{}/adm1data.json'.format(cid))
        except Exception as e:
            print('  WFP subnational: {}'.format(e))
            return []

        feats = data.get('features', []) if isinstance(data, dict) else []
        records = []
        for f in feats:
            p = f.get('properties') or {}
            fcs = p.get('fcs') if isinstance(p.get('fcs'), dict) else {}
            rcsi = p.get('rcsi') if isinstance(p.get('rcsi'), dict) else {}
            records.append({
                'iso3': iso3.upper(),
                'admin1_name': p.get('Name', ''),
                'admin1_code': p.get('Code', ''),
                'fcs_prevalence': round((fcs.get('ratio', 0) or 0) / 100.0, 4),
                'fcs_people': fcs.get('people', 0) or 0,
                'rcsi_prevalence': round((rcsi.get('ratio', 0) or 0) / 100.0, 4),
                'rcsi_people': rcsi.get('people', 0) or 0,
            })
        return records

    @staticmethod
    def save_csv(records, filepath):
        """Save records to CSV."""
        save_csv(records, filepath)


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    iso3 = sys.argv[1] if len(sys.argv) > 1 else 'LBN'
    wfp = WFPClient()

    print('=== WFP HungerMap - {} ==='.format(iso3))
    data = wfp.get_country_data(iso3)
    if data:
        print('Country: {}'.format(data.get('country_name', '')))
        print('  Population: {}'.format(data.get('population', 0)))
        print('  FCS insufficient: {} people ({}%)'.format(
            data.get('fcs_people_insufficient', 0),
            data.get('fcs_prevalence_insufficient', 0)))
        print('  rCSI crisis: {} people ({}%)'.format(
            data.get('rcsi_people_crisis', 0),
            data.get('rcsi_prevalence_crisis', 0)))
    else:
        print('  No data available')
