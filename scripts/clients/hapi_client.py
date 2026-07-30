"""
HDX HAPI Client
================
Reusable module for querying HDX Humanitarian API (HAPI v2).
Handles pagination automatically. Covers all 13 data endpoints + 8 metadata endpoints.

Usage:
    from hapi_client import HAPIClient
    hapi = HAPIClient()
    avail = hapi.get_data_availability('LBN')  # always check first
    idps = hapi.get_idps('LBN')
    funding = hapi.get_funding('LBN')
    conflict = hapi.get_conflict_events('LBN')
    refugees = hapi.get_refugees('LBN')
    humanitarian_needs = hapi.get_humanitarian_needs('SDN')  # has disabled_marker!
    returnees = hapi.get_returnees('SDN')
    food_sec = hapi.get_food_security('SDN')
    prices = hapi.get_food_prices('YEM')
    rainfall = hapi.get_rainfall('LBN')
    population = hapi.get_baseline_population('SDN')
    poverty = hapi.get_poverty_rate('ETH')
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
import time
from urllib.request import Request, urlopen

from config import (
    HAPI_BASE, HAPI_APP_ID,
    DEFAULT_TIMEOUT, DEFAULT_PAGE_SIZE, RATE_LIMIT_DELAY, USER_AGENT
)


DEDUPE_KEYS = ('location_code', 'admin1_name', 'admin2_name', 'sector_name',
               'category', 'population_status', 'date_start', 'date_end',
               'ipc_phase', 'ipc_type', 'population', 'population_in_phase')


def dedupe_rows(records, keys=None):
    """Garde GENERIQUE : retire les lignes strictement identiques.

    ⚠ Ne resout PAS le double-comptage du total national HAPI. Mesure le
    2026-07-25 sur SDN humanitarian-needs : 0 ligne retiree sur 84 032, parce que
    les deux lignes du total ne sont pas identiques — elles diffèrent par
    `category` (`''` contre `'total'`). Pour ce cas, voir la regle dans le
    docstring de `get_humanitarian_needs` : choisir UNE etiquette de total et
    verifier que le compte vaut 1.

    Utile malgre tout comme filet sur les endpoints qui repetent vraiment des
    lignes. Retourne (lignes_dedupliquees, nombre_retire).
    """
    keys = keys or DEDUPE_KEYS
    seen, out, dropped = set(), [], 0
    for r in records:
        sig = tuple(str(r.get(k, '')) for k in keys)
        if sig in seen:
            dropped += 1
            continue
        seen.add(sig)
        out.append(r)
    return out, dropped


def national_total(records, sector='Intersectoral', status='INN', period=None):
    """Total NATIONAL d'un endpoint HNO, sans double-comptage.

    Applique la regle du double etiquetage : prend les lignes `category='total'`
    et ne retombe sur `category=''` que si aucune ligne `'total'` n'existe pour
    la periode. Puis VERIFIE que le compte vaut 1 : au-dela, on ne somme pas en
    silence, on le signale.

    Retourne {'value', 'period', 'rows', 'label_used', 'warning'} ou None.
    """
    rows = [r for r in records
            if str(r.get('admin_level', '0')) == '0'
            and str(r.get('sector_name', '')).lower().startswith(sector.lower()[:9])
            and str(r.get('population_status', '')) == status]
    if not rows:
        return None
    periods = sorted({str(r.get('date_start'))[:10] for r in rows if r.get('date_start')})
    period = period or (periods[-1] if periods else '')
    rows = [r for r in rows if str(r.get('date_start'))[:10] == period]

    for label in ('total', ''):
        sel = [r for r in rows if str(r.get('category', '')).lower() == label]
        if sel:
            vals = []
            for r in sel:
                try:
                    vals.append(float(r.get('population') or 0))
                except (TypeError, ValueError):
                    pass
            warning = ''
            if len(sel) > 1:
                warning = ('{} lignes pour un total national : valeurs {}. '
                           'Une seule est retenue (pas de somme).'.format(
                               len(sel), sorted(set(vals))))
            return {'value': vals[0] if vals else None, 'period': period,
                    'rows': len(sel), 'label_used': label or "(vide)",
                    'warning': warning}
    return None

class HAPIClient:
    """Client for HDX HAPI v2 with automatic pagination."""

    def __init__(self, app_id=None):
        self.base = HAPI_BASE
        self.app_id = app_id or HAPI_APP_ID

    def _get(self, endpoint, params='', limit=None):
        """GET request to HAPI with app_identifier."""
        limit = limit or DEFAULT_PAGE_SIZE
        url = '{}/{}?app_identifier={}&{}&limit={}'.format(
            self.base, endpoint, self.app_id, params, limit)
        req = Request(url, headers={'User-Agent': USER_AGENT})
        return json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())

    def _fetch_all(self, endpoint, location_code, extra_params='', loc_param='location_code'):
        """Fetch all pages for an endpoint + country.

        Args:
            loc_param: Parameter name for location. Most endpoints use 'location_code',
                       but refugees uses 'asylum_location_code'.
        """
        all_data = []
        offset = 0
        while True:
            params = '{}={}&offset={}'.format(loc_param, location_code, offset)
            if extra_params:
                params += '&' + extra_params
            resp = self._get(endpoint, params)
            data = resp.get('data', [])
            all_data.extend(data)
            if len(data) < DEFAULT_PAGE_SIZE:
                break
            offset += DEFAULT_PAGE_SIZE
            time.sleep(RATE_LIMIT_DELAY)
        return all_data

    # ── Data Availability (always call first) ──────────────

    def get_data_availability(self, iso3):
        """Check which endpoints have data for a country.

        Returns sorted list of unique subcategory names (e.g., 'idps', 'conflict-events').
        """
        raw = self._fetch_all('metadata/data-availability', iso3)
        return sorted(set(r.get('subcategory', '') for r in raw))

    # ── IDPs ────────────────────────────────────────────────

    def get_idps(self, iso3, admin_level=None):
        """Fetch IDP data. Returns list of records.

        Each record has: location_code, admin1_name, admin2_name,
        reference_period_start, reference_period_end, population.
        """
        extra = 'admin_level={}'.format(admin_level) if admin_level is not None else ''
        raw = self._fetch_all('affected-people/idps', iso3, extra)
        records = []
        for r in raw:
            records.append({
                'location_code': r.get('location_code', ''),
                'admin1_name': r.get('admin1_name', ''),
                'admin2_name': r.get('admin2_name', ''),
                'date_start': str(r.get('reference_period_start', ''))[:10],
                'date_end': str(r.get('reference_period_end', ''))[:10],
                'population': r.get('population', ''),
            })
        return records

    # ── Operational Presence ────────────────────────────────

    def get_op_presence(self, iso3, org_acronym=None):
        """Fetch operational presence. Returns list of records.

        Each record has: org_acronym, org_name, sector_name,
        admin1_name, admin2_name, date_start, date_end.
        """
        extra = 'org_acronym={}'.format(org_acronym) if org_acronym else ''
        raw = self._fetch_all('coordination-context/operational-presence', iso3, extra)
        records = []
        for r in raw:
            records.append({
                'org_acronym': r.get('org_acronym', ''),
                'org_name': r.get('org_name', ''),
                'sector_name': r.get('sector_name', ''),
                'admin1_name': r.get('admin1_name', ''),
                'admin2_name': r.get('admin2_name', ''),
                'date_start': str(r.get('reference_period_start', ''))[:10],
                'date_end': str(r.get('reference_period_end', ''))[:10],
            })
        return records

    # ── Funding ─────────────────────────────────────────────

    def get_funding(self, iso3):
        """Fetch funding/appeal data. Returns list of records.

        Each record has: appeal_name, appeal_code, appeal_type, year,
        requirements_usd, funding_usd, funding_pct.
        """
        raw = self._fetch_all('coordination-context/funding', iso3)
        records = []
        for r in raw:
            req_usd = float(r.get('requirements_usd', 0) or 0)
            fund_usd = float(r.get('funding_usd', 0) or 0)
            records.append({
                'appeal_name': r.get('appeal_name', ''),
                'appeal_code': r.get('appeal_code', ''),
                'appeal_type': r.get('appeal_type', ''),
                'year': str(r.get('reference_period_start', ''))[:4],
                'requirements_usd': req_usd,
                'funding_usd': fund_usd,
                'funding_pct': round(fund_usd / req_usd * 100, 1) if req_usd else 0,
            })
        return records

    # ── National Risk ───────────────────────────────────────

    def get_national_risk(self, iso3):
        """Fetch INFORM national risk data. Returns list of records.

        Each record has: risk_class, global_rank, overall_risk,
        hazard_exposure, vulnerability, coping_capacity, date_start, date_end.
        """
        raw = self._fetch_all('coordination-context/national-risk', iso3)
        records = []
        for r in raw:
            records.append({
                'location_code': r.get('location_code', ''),
                'risk_class': r.get('risk_class', ''),
                'global_rank': r.get('global_rank', ''),
                'overall_risk': r.get('overall_risk', ''),
                'hazard_exposure': r.get('hazard_exposure_risk', ''),
                'vulnerability': r.get('vulnerability_risk', ''),
                'coping_capacity': r.get('coping_capacity_risk', ''),
                'date_start': str(r.get('reference_period_start', ''))[:10],
                'date_end': str(r.get('reference_period_end', ''))[:10],
            })
        return records

    # ── Returnees ───────────────────────────────────────────

    def get_returnees(self, iso3, direction='origin'):
        """Fetch returnee data. Returns list of records.

        WARNING (fixed 2026-07-25): returnees is a FLOW between an origin and an
        asylum country, NOT a country stock. `location_code` is NOT a parameter
        of this endpoint, and HAPI (FastAPI) ignores unknown query parameters
        SILENTLY, so the previous implementation collected the returnees of the
        ENTIRE WORLD and wrote three permanently empty geography columns, which
        the orchestrator then summed.

        direction='origin' -> people returning TO {iso3} (the usual question).
        direction='asylum' -> people returning FROM {iso3}.

        The endpoint carries no location_code/admin1_name/admin2_name at all;
        geography is origin_location_* / asylum_location_*.
        """
        loc_param = ('origin_location_code' if direction == 'origin'
                     else 'asylum_location_code')
        raw = self._fetch_all('affected-people/returnees', iso3,
                              loc_param=loc_param)
        records = []
        for r in raw:
            records.append({
                'origin_location_code': r.get('origin_location_code', ''),
                'origin_location_name': r.get('origin_location_name', ''),
                'asylum_location_code': r.get('asylum_location_code', ''),
                'asylum_location_name': r.get('asylum_location_name', ''),
                'population_group': r.get('population_group', ''),
                'gender': r.get('gender', ''),
                'age_range': r.get('age_range', ''),
                'min_age': r.get('min_age', ''),
                'max_age': r.get('max_age', ''),
                'population': r.get('population', ''),
                'date_start': str(r.get('reference_period_start', ''))[:10],
                'date_end': str(r.get('reference_period_end', ''))[:10],
                'resource_hdx_id': r.get('resource_hdx_id', ''),
            })
        return records

    # ── Refugees / Persons of Concern ──────────────────────

    def get_refugees(self, iso3):
        """Fetch UNHCR refugees/PoC data. Uses asylum_location_code (NOT location_code).

        Each record has: population_group, gender, age_range, population,
        origin_location_code, origin_location_name.
        """
        raw = self._fetch_all(
            'affected-people/refugees-persons-of-concern', iso3,
            loc_param='asylum_location_code')
        records = []
        for r in raw:
            records.append({
                'asylum_location': r.get('asylum_location_code', ''),
                'origin_location': r.get('origin_location_code', ''),
                'origin_name': r.get('origin_location_name', ''),
                'population_group': r.get('population_group', ''),
                'gender': r.get('gender', ''),
                'age_range': r.get('age_range', ''),
                'population': r.get('population', ''),
                'date_start': str(r.get('reference_period_start', ''))[:10],
                'date_end': str(r.get('reference_period_end', ''))[:10],
            })
        return records

    # ── Conflict Events (ACLED via HAPI) ───────────────────

    def get_conflict_events(self, iso3):
        """Fetch ACLED conflict events. Returns list of records.

        Each record has: event_type, events (count), fatalities, admin1/2, dates.
        """
        raw = self._fetch_all('coordination-context/conflict-events', iso3)
        records = []
        for r in raw:
            records.append({
                'location_code': r.get('location_code', ''),
                'admin1_name': r.get('admin1_name', ''),
                'admin2_name': r.get('admin2_name', ''),
                'event_type': r.get('event_type', ''),
                'events': r.get('events', 0),
                'fatalities': r.get('fatalities', 0),
                'date_start': str(r.get('reference_period_start', ''))[:10],
                'date_end': str(r.get('reference_period_end', ''))[:10],
            })
        return records

    # ── Food Security (IPC phases) ─────────────────────────

    def get_food_security(self, iso3):
        """Fetch IPC food security data. Returns list of records.

        Each record has: ipc_phase, ipc_type, population_in_phase, admin1/2.
        Available for: SDN, ETH, SOM, MOZ, PSE, LBN and others.
        """
        raw = self._fetch_all('food-security-nutrition-poverty/food-security', iso3)
        records = []
        for r in raw:
            records.append({
                'location_code': r.get('location_code', ''),
                'admin1_name': r.get('admin1_name', ''),
                'admin2_name': r.get('admin2_name', ''),
                'ipc_phase': r.get('ipc_phase', ''),
                'ipc_type': r.get('ipc_type', ''),
                'population_in_phase': r.get('population_in_phase', ''),
                'population_fraction': r.get('population_fraction_in_phase', ''),
                'date_start': str(r.get('reference_period_start', ''))[:10],
                'date_end': str(r.get('reference_period_end', ''))[:10],
            })
        return records

    # ── Food Prices (WFP market data) ──────────────────────

    def get_food_prices(self, iso3, limit=None):
        """Fetch WFP food prices/market monitor data. Can be very large.

        Each record has: commodity_name, unit, price, currency_code, market_name, lat, lon.
        """
        raw = self._fetch_all('food-security-nutrition-poverty/food-prices-market-monitor', iso3)
        records = []
        for r in raw:
            records.append({
                'location_code': r.get('location_code', ''),
                'admin1_name': r.get('admin1_name', ''),
                'market_name': r.get('market_name', ''),
                'commodity_name': r.get('commodity_name', ''),
                'commodity_category': r.get('commodity_category', ''),
                'unit': r.get('unit', ''),
                'price': r.get('price', ''),
                'currency_code': r.get('currency_code', ''),
                'price_type': r.get('price_type', ''),
                'lat': r.get('lat', ''),
                'lon': r.get('lon', ''),
                'date_start': str(r.get('reference_period_start', ''))[:10],
            })
        return records

    # ── Poverty Rate (MPI) ─────────────────────────────────

    def get_poverty_rate(self, iso3):
        """Fetch MPI poverty rate data. Returns list of records.

        Each record has: mpi, headcount_ratio, intensity_of_deprivation, admin1.
        Available for: ETH, MOZ, PSE and a few others.
        """
        raw = self._fetch_all('food-security-nutrition-poverty/poverty-rate', iso3)
        records = []
        for r in raw:
            records.append({
                'location_code': r.get('location_code', ''),
                'admin1_name': r.get('admin1_name', ''),
                'mpi': r.get('mpi', ''),
                'headcount_ratio': r.get('headcount_ratio', ''),
                'intensity_of_deprivation': r.get('intensity_of_deprivation', ''),
                'vulnerable_to_poverty': r.get('vulnerable_to_poverty', ''),
                'in_severe_poverty': r.get('in_severe_poverty', ''),
                'date_start': str(r.get('reference_period_start', ''))[:10],
                'date_end': str(r.get('reference_period_end', ''))[:10],
            })
        return records

    # ── Baseline Population ────────────────────────────────

    def get_baseline_population(self, iso3):
        """Fetch baseline population estimates. Returns list of records.

        Each record has: gender, age_range, population, admin1/2.
        Available for: SDN, ETH, SOM, MOZ, PSE.
        """
        raw = self._fetch_all('geography-infrastructure/baseline-population', iso3)
        records = []
        for r in raw:
            records.append({
                'location_code': r.get('location_code', ''),
                'admin1_name': r.get('admin1_name', ''),
                'admin2_name': r.get('admin2_name', ''),
                'gender': r.get('gender', ''),
                'age_range': r.get('age_range', ''),
                'population': r.get('population', ''),
                'date_start': str(r.get('reference_period_start', ''))[:10],
            })
        return records

    # ── Rainfall (CHIRPS climate) ──────────────────────────

    def get_rainfall(self, iso3):
        """Fetch CHIRPS rainfall data with anomalies. Returns list of records.

        Each record has: rainfall, rainfall_anomaly_pct, rainfall_long_term_average, admin1/2.
        """
        raw = self._fetch_all('climate/rainfall', iso3)
        records = []
        for r in raw:
            records.append({
                'location_code': r.get('location_code', ''),
                'admin1_name': r.get('admin1_name', ''),
                'admin2_name': r.get('admin2_name', ''),
                'rainfall': r.get('rainfall', ''),
                'rainfall_anomaly_pct': r.get('rainfall_anomaly_pct', ''),
                'rainfall_long_term_avg': r.get('rainfall_long_term_average', ''),
                'date_start': str(r.get('reference_period_start', ''))[:10],
                'date_end': str(r.get('reference_period_end', ''))[:10],
            })
        return records

    # ── Humanitarian Needs ─────────────────────────────────

    def get_humanitarian_needs(self, iso3):
        """Fetch HNO humanitarian needs data. Returns list of records.

        IMPORTANT FOR HI: v2 consolidated ALL disaggregation into a single
        `category` field. The labels VARY PER HNO VINTAGE (SDN observed
        2026-07-24): 2024 uses 'Disability', 'Hostcommunities', 'IDPs';
        2025 uses 'People with disability', 'Host Communities', 'IDP';
        totals = 'total' and/or ''. Match disability rows with
        `'disab' in category.lower()`, never with an exact label.
        (The old v1 fields gender/age_range/disabled_marker/population_group
        NO LONGER EXIST - the pre-2026-07 client read them and silently
        returned '' for everything, which masked the disability data as a
        "gap". It is NOT a gap for SDN: HNO 2025 has 4.57M people with
        disabilities in need, down to admin2.)

        ANALYSIS RULES:
        - Never sum rows naively: rows repeat per category AND per HNO
          reference period (2024, 2025, 2026...). Filter ONE period + ONE
          category (or category-total) first.
        - ⚠ **LE TOTAL EST PUBLIE SOUS DEUX ETIQUETTES : `''` ET `'total'`.**
          Trouve le 2026-07-25 (SDN). Les deux lignes portent la MEME valeur, le
          meme `resource_hdx_id` et les memes dates : elles ne diffèrent QUE par
          le champ `category`, vide pour l'une, `'total'` pour l'autre. Un filtre
          `category in ('total', '')` accepte donc les DEUX et double le chiffre :
              2024 : 2 lignes a 24 786 370  -> 49 572 740 (DOUBLE)
              2025 : 2 lignes a 30 440 770  -> 60 881 540 (DOUBLE)
              2026 : 1 ligne  a 33 699 770  -> correct (ce millesime n'utilise
                                               qu'une seule des deux etiquettes)
          **Choisir UNE etiquette** (`category == 'total'`, avec repli sur `''`
          seulement si aucune ligne `'total'` n'existe pour la periode), et
          **verifier que le compte vaut 1** pour un headline national. Ne PAS
          compter sur `dedupe_rows()` ici : les lignes ne sont pas identiques,
          il les laisse toutes les deux passer (0 retiree sur 84 032).
          Consequence vecue : selon le millesime, le meme code rendait le bon
          chiffre (2026) ou le double (2025) — une erreur qui varie par annee,
          donc invisible a la relecture.
        - National headline PiN = admin_level 0 + sector Intersectoral +
          population_status INN + the total category, latest period.
        - population_status: INN (in need), TGT (targeted), REA (reached),
          AFF (affected), all.

        Each record: location_code, admin1_name, admin2_name, admin_level,
        sector_code, sector_name, category, population_status, population,
        date_start, date_end, resource_hdx_id.
        """
        raw = self._fetch_all('affected-people/humanitarian-needs', iso3)
        records = []
        for r in raw:
            records.append({
                'location_code': r.get('location_code', ''),
                'admin1_name': r.get('admin1_name', ''),
                'admin2_name': r.get('admin2_name', ''),
                'admin_level': r.get('admin_level', ''),
                'sector_code': r.get('sector_code', ''),
                'sector_name': r.get('sector_name', ''),
                'category': r.get('category', ''),
                'population_status': r.get('population_status', ''),
                'population': r.get('population', ''),
                'date_start': str(r.get('reference_period_start', ''))[:10],
                'date_end': str(r.get('reference_period_end', ''))[:10],
                'resource_hdx_id': r.get('resource_hdx_id', ''),
            })
        return records

    # ── Metadata helpers ────────────────────────────────────

    def get_admin_units(self, iso3, level=1):
        """Fetch admin units (admin1 or admin2) for a country."""
        raw = self._fetch_all('metadata/admin{}'.format(level), iso3)
        return [{'code': r.get('code', ''), 'name': r.get('name', '')} for r in raw]

    def get_sectors(self):
        """Fetch available sectors."""
        resp = self._get('metadata/sector', limit=100)
        return [{'code': r.get('code', ''), 'name': r.get('name', '')} for r in resp.get('data', [])]

    # ── Additional metadata (from hdx-mcp audit) ────────────

    def _fetch_all_generic(self, endpoint, extra_params=''):
        """Fetch all pages for a metadata endpoint (no location required)."""
        all_data = []
        offset = 0
        while True:
            params = 'offset={}'.format(offset)
            if extra_params:
                params += '&' + extra_params
            resp = self._get(endpoint, params)
            data = resp.get('data', [])
            all_data.extend(data)
            if len(data) < DEFAULT_PAGE_SIZE:
                break
            offset += DEFAULT_PAGE_SIZE
            time.sleep(RATE_LIMIT_DELAY)
        return all_data

    def get_locations(self, name=None, has_hrp=None):
        """Fetch all HAPI locations (countries).

        Args:
            name: Filter by country name (partial match)
            has_hrp: True = only HRP countries, False = only non-HRP, None = all

        Returns list of dicts: code (ISO3), name, has_hrp.
        Useful for discovering which countries have data + active humanitarian response.
        """
        params = ''
        if name:
            params += 'name={}&'.format(name)
        if has_hrp is not None:
            params += 'has_hrp={}&'.format('true' if has_hrp else 'false')
        raw = self._fetch_all_generic('metadata/location', params.rstrip('&'))
        return [{
            'code': r.get('code', ''),
            'name': r.get('name', ''),
            'has_hrp': r.get('has_hrp', False),
        } for r in raw]

    def get_datasets(self, location_code=None):
        """Fetch dataset metadata — provenance info for citations.

        Returns list of dicts: hdx_id, hdx_stub, title, provider_code, provider_name.
        """
        params = 'location_code={}'.format(location_code) if location_code else ''
        raw = self._fetch_all_generic('metadata/dataset', params)
        return [{
            'hdx_id': r.get('hdx_id', ''),
            'hdx_stub': r.get('hdx_stub', ''),
            'title': r.get('title', ''),
            'provider_code': r.get('hdx_provider_stub', ''),
            'provider_name': r.get('hdx_provider_name', ''),
        } for r in raw]

    def get_resources(self, dataset_hdx_id=None):
        """Fetch individual resources (files) within datasets.

        Returns list of dicts: resource_hdx_id, name, format, update_date, download_url.
        """
        params = 'dataset_hdx_id={}'.format(dataset_hdx_id) if dataset_hdx_id else ''
        raw = self._fetch_all_generic('metadata/resource', params)
        return [{
            'resource_hdx_id': r.get('resource_hdx_id', ''),
            'name': r.get('name', ''),
            'format': r.get('format', ''),
            'update_date': str(r.get('update_date', ''))[:10],
            'download_url': r.get('download_url', ''),
            'dataset_hdx_id': r.get('dataset_hdx_id', ''),
        } for r in raw]

    def get_orgs(self, org_type_code=None):
        """Fetch organizations in HAPI.

        Args:
            org_type_code: Filter by org type (e.g., 433 = INGO, 447 = UN)

        Returns list of dicts: acronym, name, org_type_code, org_type_description.
        """
        params = 'org_type_code={}'.format(org_type_code) if org_type_code else ''
        raw = self._fetch_all_generic('metadata/org', params)
        return [{
            'acronym': r.get('acronym', ''),
            'name': r.get('name', ''),
            'org_type_code': r.get('org_type_code', ''),
            'org_type_description': r.get('org_type_description', ''),
        } for r in raw]

    def get_org_types(self):
        """Fetch organization type taxonomy (INGO, NNGO, UN, etc.)."""
        resp = self._get('metadata/org-type', limit=100)
        return [{
            'code': r.get('code', ''),
            'description': r.get('description', ''),
        } for r in resp.get('data', [])]

    def get_currencies(self):
        """Fetch currency codes and names."""
        resp = self._get('metadata/currency', limit=200)
        return [{
            'code': r.get('code', ''),
            'name': r.get('name', ''),
        } for r in resp.get('data', [])]

    def get_wfp_commodities(self):
        """Fetch WFP commodity list (food items tracked in price monitoring)."""
        raw = self._fetch_all_generic('metadata/wfp-commodity')
        return [{
            'code': r.get('code', ''),
            'name': r.get('name', ''),
            'category': r.get('category', ''),
        } for r in raw]

    def get_wfp_markets(self, location_code=None):
        """Fetch WFP market locations (name, lat/lon).

        Useful for mapping food price monitoring points.
        """
        params = 'location_code={}'.format(location_code) if location_code else ''
        raw = self._fetch_all_generic('metadata/wfp-market', params)
        return [{
            'code': r.get('code', ''),
            'name': r.get('name', ''),
            'lat': r.get('lat', ''),
            'lon': r.get('lon', ''),
            'location_code': r.get('location_code', ''),
            'admin1_name': r.get('admin1_name', ''),
            'admin2_name': r.get('admin2_name', ''),
        } for r in raw]


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    iso3 = sys.argv[1] if len(sys.argv) > 1 else 'LBN'
    hapi = HAPIClient()

    print('=== HDX HAPI v2 — {} ==='.format(iso3))

    # Check data availability first
    avail = hapi.get_data_availability(iso3)
    print('\nData available ({}/13): {}'.format(len(avail), ', '.join(avail)))

    idps = hapi.get_idps(iso3)
    print('\nIDPs: {} records'.format(len(idps)))
    if idps:
        dates = sorted(set(r['date_start'] for r in idps if r['date_start']))
        print('  Period: {} -> {}'.format(dates[0] if dates else 'N/A', dates[-1] if dates else 'N/A'))

    ops = hapi.get_op_presence(iso3)
    print('\nOp Presence: {} records'.format(len(ops)))
    orgs = set(r['org_acronym'] for r in ops if r['org_acronym'])
    print('  Unique orgs: {}'.format(len(orgs)))

    funding = hapi.get_funding(iso3)
    print('\nFunding: {} records'.format(len(funding)))
    for f in sorted(funding, key=lambda x: x['year'], reverse=True)[:5]:
        print('  {} ({}) — req ${:.1f}M, funded ${:.1f}M ({:.0f}%)'.format(
            f['appeal_name'][:40], f['year'],
            f['requirements_usd'] / 1e6, f['funding_usd'] / 1e6, f['funding_pct']))

    risk = hapi.get_national_risk(iso3)
    print('\nNational Risk: {} records'.format(len(risk)))
    for r in risk:
        print('  Class {}, Rank {}, Overall {}, Vulnerability {}'.format(
            r['risk_class'], r['global_rank'], r['overall_risk'], r['vulnerability']))

    conflict = hapi.get_conflict_events(iso3)
    print('\nConflict Events: {} records'.format(len(conflict)))

    refugees = hapi.get_refugees(iso3)
    print('\nRefugees/PoC: {} records'.format(len(refugees)))

    # Humanitarian Needs - key for HI (disability data)
    # v2: ALL disaggregation lives in `category`; the v1 fields gender/age_range/
    # disabled_marker/population_group are DEAD. This self-test used to read
    # disabled_marker and therefore printed "0 records marked disabled" for a
    # country with 4,566,110 - the exact false negative that hid the data for
    # three months. Labels vary per HNO vintage, so match on 'disab'.
    hum_needs = hapi.get_humanitarian_needs(iso3)
    print('\nHumanitarian Needs: {} records'.format(len(hum_needs)))
    if hum_needs:
        cats = sorted({(r.get('category') or '(empty)') for r in hum_needs})
        print('  categories observed ({}): {}'.format(len(cats), ', '.join(cats)))
        periods = sorted({'{}->{}'.format(r.get('date_start'), r.get('date_end'))
                          for r in hum_needs})
        print('  reference periods: {}'.format(', '.join(periods)))
        dis = [r for r in hum_needs if 'disab' in (r.get('category') or '').lower()]
        if dis:
            # One period + one sector + one status, else rows multiply (156M trap)
            latest = max(str(r.get('date_start') or '') for r in dis)
            inn = [r for r in dis
                   if str(r.get('date_start') or '') == latest
                   and r.get('population_status') == 'INN'
                   and r.get('sector_name') == 'Intersectoral'
                   and r.get('admin_level') == 2]
            pop = sum(int(r.get('population') or 0) for r in inn)
            print('  ** DISABILITY: {} rows total; period {} INN Intersectoral '
                  'admin2 -> {:,} persons'.format(len(dis), latest, pop))
        else:
            print('  ** No disability category found (check the category list above)')

    returnees = hapi.get_returnees(iso3)
    print('\nReturnees TO {} : {} records'.format(iso3, len(returnees)))
    if returnees:
        asylums = sorted({r['asylum_location_name'] for r in returnees
                          if r['asylum_location_name']})
        print('  returning from: {}'.format(', '.join(asylums[:10]) or 'n/a'))
        print('  (no total printed: rows repeat per group/gender/age/period - '
              'filter one combination before summing)')

    rainfall = hapi.get_rainfall(iso3)
    print('\nRainfall: {} records'.format(len(rainfall)))
