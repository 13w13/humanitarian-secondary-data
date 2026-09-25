"""
Unified Country Data Fetcher
=============================
Fetch all secondary data sources for a given country (ISO3 code).
Writes CSVs to {ISO3}_data/raw/ and {ISO3}_data/catalogue/, plus two indexes:
fetch_summary.csv (one row per source with its status) and data_inventory.csv
(one row per file).

Sources (17): ReliefWeb, HDX HAPI, HDX CKAN, IDMC, UNHCR, INFORM, WFP HungerMap,
         World Bank, ACLED, ACAPS, DTM (HDX), GDACS, HPC/FTS, IFRC GO,
         IMPACT/REACH, DTM portal, Liveuamap. ACLED, ACAPS and IDMC need a free key.

Every source ends in one status: ok, empty, partial, unavailable, error, skipped
or not_covered. A failed source is never reported as zero records of data.
Exit status: 0 when every source answered, 1 when one is partial, unavailable
or in error, 2 on invalid arguments.

Usage:
    python -X utf8 fetch_country_data.py LBN
    python -X utf8 fetch_country_data.py SDN --date-from 2026-01-01 --date-to 2026-03-31
    python -X utf8 fetch_country_data.py LBN --skip-hdx --skip-worldbank
    python -X utf8 fetch_country_data.py LBN --only reliefweb,hapi,idmc

Objectives: O4 (innovation), O5 (evidence-based), O2 (pack IM urgence)
"""
import sys
if hasattr(sys.stdout, 'reconfigure'):   # absent in Jupyter, IDLE, captured output
    sys.stdout.reconfigure(encoding='utf-8')

import os
import csv
import argparse
from datetime import datetime

# Add clients/ to path so fetch functions can import *_client modules
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'clients'))

# Country name mapping for HDX search (ISO3 -> name)
COUNTRY_NAMES = {
    'AFG': 'afghanistan', 'BDI': 'burundi', 'COD': 'congo', 'ETH': 'ethiopia',
    'HTI': 'haiti', 'IRN': 'iran', 'IRQ': 'iraq', 'KEN': 'kenya', 'LBN': 'lebanon',
    'LBY': 'libya', 'MLI': 'mali', 'MMR': 'myanmar', 'MOZ': 'mozambique',
    'NER': 'niger', 'NGA': 'nigeria', 'PAK': 'pakistan', 'PSE': 'palestine',
    'SDN': 'sudan', 'SOM': 'somalia', 'SSD': 'south sudan', 'SYR': 'syria',
    'TCD': 'chad', 'UKR': 'ukraine', 'YEM': 'yemen',
}

ALL_SOURCES = [
    'reliefweb', 'hapi', 'hdx', 'idmc', 'unhcr', 'inform',
    'wfp', 'worldbank', 'acled', 'acaps', 'dtm',
    'gdacs', 'hpc', 'ifrcgo', 'impact', 'dtm_portal',
    'liveuamap',
]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

# Source categories: catalogue (dataset listings with URLs) vs raw (exploitable data)
SOURCE_CATEGORIES = {
    'reliefweb': 'catalogue',   # Listings of reports (not the content)
    'hapi': 'raw',
    'hdx': 'catalogue',
    'idmc': 'raw',
    'unhcr': 'raw',
    'inform': 'raw',     # Risk scores = exploitable data
    'wfp': 'raw',
    'worldbank': 'raw',  # Indicators = exploitable data
    'acled': 'raw',
    'acaps': 'raw',
    'dtm': 'catalogue',
    'gdacs': 'raw',
    'hpc': 'raw',
    'ifrcgo': 'raw',
    'impact': 'catalogue',
    'dtm_portal': 'catalogue',
    'liveuamap': 'raw',
}

CATALOGUE_SOURCES = {k for k, v in SOURCE_CATEGORIES.items() if v == 'catalogue'}

INVENTORY_FIELDS = [
    'file', 'source', 'category', 'records', 'size_bytes',
    'period_from', 'period_to', 'directory', 'note',
]

# Map dispatch keys to display names (used in error fallback for consistent summary)
SOURCE_DISPLAY_NAMES = {
    'reliefweb': 'ReliefWeb', 'hapi': 'HDX HAPI', 'hdx': 'HDX CKAN',
    'idmc': 'IDMC', 'unhcr': 'UNHCR', 'inform': 'INFORM',
    'wfp': 'WFP HungerMap', 'worldbank': 'World Bank', 'acled': 'ACLED',
    'acaps': 'ACAPS', 'dtm': 'DTM/IOM', 'gdacs': 'GDACS',
    'hpc': 'HPC/FTS', 'ifrcgo': 'IFRC Go', 'impact': 'IMPACT/REACH',
    'dtm_portal': 'DTM Portal',
    'liveuamap': 'Liveuamap',
}



def _extract_date_range(records, date_field='date_start'):
    """Extract min/max dates from a list of record dicts."""
    dates = []
    for r in records:
        d = r.get(date_field, '') or ''
        if d:
            dates.append(str(d)[:10])
    if not dates:
        return '', ''
    return min(dates), max(dates)


# ─── Per-source outcome ─────────────────────────────────────
# A source ends in exactly one of these states, written to fetch_summary.csv.
# Only `ok` and `empty` are answers from the provider; every other state means
# the files on disk say nothing about the country for that source.
#   ok           the source answered with data
#   empty        the source answered, with nothing for this country and period
#   partial      some requests answered, others failed (see note): not a total
#   unavailable  the provider could not be reached (outage, retry later)
#   error        our side: a bug, or the provider changed its response
#   skipped      not queried: missing free key, or excluded by a flag
#   not_covered  the source has no coverage for this country
FAILED_STATES = ('partial', 'unavailable', 'error')


def _failure_state(exc):
    """Which state an exception puts a source in."""
    from config import MissingCredential, NotCovered, is_outage
    if isinstance(exc, MissingCredential):
        return 'skipped'
    if isinstance(exc, NotCovered):
        return 'not_covered'
    return 'unavailable' if is_outage(exc) else 'error'


def _short(exc):
    return '{}: {}'.format(type(exc).__name__, str(exc).replace('\n', ' ')[:110])


def _attempt(errors, label, fn, *args, **kwargs):
    """Run one request of a source; on failure record it and return None.

    A source is often several requests (HPC: plans, then flows). One failing
    must not discard the others, and must not read as zero either: its result
    is None (printed "n/a"), the failure is kept in `errors`, and the source can
    no longer end `ok`.
    """
    try:
        return fn(*args, **kwargs)
    except Exception as e:  # noqa: BLE001 - recorded, never swallowed
        errors.append((label, e))
        print('  {} -> {} ({})'.format(label, _failure_state(e).upper(), _short(e)))
        return None


def _n(rows):
    """Count for a note: 'n/a' when the request failed, never a fake 0."""
    return 'n/a' if rows is None else len(rows)


def _state(total_records, errors):
    if not errors:
        return 'ok' if total_records else 'empty'
    if total_records:
        return 'partial'
    states = {_failure_state(e) for _, e in errors}
    for s in ('error', 'unavailable', 'skipped'):
        if s in states:
            return s
    return 'not_covered'


def _make_result(source, category, total_records, disability_records, note,
                 period_from='', period_to='', last_update='', files=None,
                 errors=None, status=None):
    """Build a standardized result dict for fetch_summary.csv.

    `errors` is the list filled by `_attempt`; `status` forces a state (e.g.
    'skipped') when no request was made at all.
    """
    errors = errors or []
    if errors:
        failed = '; '.join('{} {}: {}'.format(label, _failure_state(e), _short(e))
                           for label, e in errors)
        note = '{} | {}'.format(note, failed) if note else failed
    return {
        'source': source,
        'status': status or _state(total_records, errors),
        'category': category,
        'total_records': total_records,
        'disability_records': disability_records,
        'period_from': period_from or '',
        'period_to': period_to or '',
        'last_update': last_update or '',
        'files': ', '.join(files) if files else '',
        'note': note,
    }


def save_csv(rows, filepath, fieldnames, audit=True):
    """Save list of dicts to CSV. Retries on PermissionError (OneDrive lock).

    Delegates the empty/phantom-column audit to `config.audit_columns` so that
    BOTH write paths of this repo (this one, 43 call sites, and the clients'
    `config.save_csv` used by ACLED/ACAPS) share one gate. Uses
    extrasaction='ignore' so a client can return extra keys without raising
    against the frozen header below (review 2026-07-25).

    `audit=False` for the run's own indexes (fetch_summary, data_inventory):
    their empty columns are facts about the run, not a renamed API field.
    """
    from config import audit_columns
    parent = os.path.dirname(os.path.abspath(filepath))
    if parent:
        os.makedirs(parent, exist_ok=True)
    if audit:
        audit_columns(rows, filepath)
    for attempt in range(3):
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames,
                                        extrasaction='ignore')
                writer.writeheader()
                writer.writerows(rows)
            print('  Saved {} rows -> {}'.format(len(rows), os.path.basename(filepath)))
            return
        except PermissionError:
            if attempt < 2:
                import time
                time.sleep(1)
            else:
                print('  WARNING: Could not write {} (OneDrive lock)'.format(
                    os.path.basename(filepath)))


# The files each source writes, by exact name. Before a source runs, its files
# from an earlier run are removed: a source that fails or finds nothing this time
# must not leave last month's CSV on disk looking current, and listed as such in
# data_inventory.csv. Only these exact names are ever deleted (never downloads).
SOURCE_FILES = {
    'reliefweb': ['reliefweb_facets.csv', 'reliefweb_sitreps.csv',
                  'reliefweb_disability.csv'],
    'hapi': ['hapi_idps.csv', 'hapi_op_presence.csv', 'hapi_funding.csv',
             'hapi_risk.csv', 'hapi_conflict_events.csv', 'hapi_refugees.csv',
             'hapi_food_security.csv', 'hapi_food_prices.csv', 'hapi_population.csv',
             'hapi_humanitarian_needs.csv', 'hapi_returnees.csv', 'hapi_rainfall.csv'],
    'hdx': ['hdx_all_datasets.csv'],
    'idmc': ['idmc_displacement.csv', 'idmc_events.csv'],
    'unhcr': ['unhcr_population.csv', 'unhcr_population_origin.csv',
              'unhcr_demographics.csv', 'unhcr_solutions.csv'],
    'inform': ['inform_risk.csv', 'inform_subnational.csv'],
    'wfp': ['wfp_hungermap.csv', 'wfp_subnational.csv'],
    'worldbank': ['worldbank_profile.csv'],
    'acled': ['acled_events.csv', 'acled_cast_forecasts.csv'],
    'acaps': ['acaps_severity.csv', 'acaps_access.csv'],
    'dtm': ['dtm_datasets.csv'],
    'gdacs': ['gdacs_alerts.csv'],
    'hpc': ['hpc_plans.csv', 'hpc_funding_flows.csv'],
    'ifrcgo': ['ifrcgo_emergencies.csv', 'ifrcgo_appeals.csv', 'ifrcgo_projects.csv'],
    'impact': ['impact_all_resources.csv', 'impact_msna_datasets.csv'],
    'dtm_portal': ['dtm_portal_datasets.csv'],
    'liveuamap': ['liveuamap_events.csv'],
}
FILE_OWNER = {f: src for src, names in SOURCE_FILES.items() for f in names}


def clear_previous_outputs(sources, dirs):
    """Delete, for the sources about to run, the files an earlier run left."""
    removed = []
    for src in sources:
        for fname in SOURCE_FILES.get(src, []):
            for d in dirs:
                path = os.path.join(d, fname)
                if os.path.isfile(path):
                    os.remove(path)
                    removed.append(fname)
    return removed


def _count_rows(path):
    """Data rows of a CSV, read as CSV (a quoted field may span several lines).

    Returns (count, None) or (None, reason): an unreadable file is not 0 rows.
    """
    try:
        with open(path, newline='', encoding='utf-8-sig') as f:
            return max(0, sum(1 for _ in csv.reader(f)) - 1), None
    except (OSError, UnicodeDecodeError, csv.Error) as e:
        return None, '{}: {}'.format(type(e).__name__, str(e)[:60])


def build_inventory(summaries, raw_dir, catalogue_dir=None, ran=None):
    """Build data_inventory.csv rows: one per CSV file on disk.

    A file is attributed to a source by its exact name (it used to be a substring
    match, so a downloaded `events.csv` was credited to ACLED). A pipeline file
    not refreshed by this run says so, and a file no source owns (a download)
    is listed as such.
    """
    by_name = {}
    for s in summaries:
        for f in (s.get('files') or '').split(','):
            if f.strip():
                by_name[f.strip()] = s
    ran = set(ran or [])
    inventory = []
    for directory, dir_label in [(raw_dir, 'raw'), (catalogue_dir, 'catalogue')]:
        if not directory or not os.path.isdir(directory):
            continue
        for fname in sorted(os.listdir(directory)):
            if not fname.endswith('.csv') or fname in ('data_inventory.csv',
                                                       'fetch_summary.csv'):
                continue
            fpath = os.path.join(directory, fname)
            records, unreadable = _count_rows(fpath)
            matched = by_name.get(fname)
            owner = FILE_OWNER.get(fname)
            if matched:
                note = matched.get('note', '')
            elif owner and owner not in ran:
                note = 'from an earlier run: not refreshed by this one'
            else:
                note = 'not written by the pipeline (download or manual file)'
            if unreadable:
                note = 'unreadable ({}) | {}'.format(unreadable, note)
            inventory.append({
                'file': fname,
                'source': (matched['source'] if matched else
                           SOURCE_DISPLAY_NAMES.get(owner, '') if owner else ''),
                'category': matched['category'] if matched else dir_label,
                'records': '' if records is None else records,
                'size_bytes': os.path.getsize(fpath),
                'period_from': matched.get('period_from', '') if matched else '',
                'period_to': matched.get('period_to', '') if matched else '',
                'directory': dir_label,
                'note': note,
            })
    return inventory

def fetch_reliefweb(iso3, output_dir, date_from=None, date_to=None):
    """Fetch ReliefWeb data: facets, sitreps, disability search."""
    from reliefweb_client import ReliefWebClient
    rw = ReliefWebClient()
    files = []

    print('\n--- ReliefWeb ---')

    facets = rw.get_facets(iso3, date_from, date_to)
    print('  Total reports: {}'.format(facets['total']))
    facet_rows = []
    for fname, items in facets['facets'].items():
        for item in items:
            facet_rows.append({
                'facet_field': fname, 'value': item['value'], 'count': item['count'],
            })
    if facet_rows:
        save_csv(facet_rows, os.path.join(output_dir, 'reliefweb_facets.csv'),
                 ['facet_field', 'value', 'count'])
        files.append('reliefweb_facets.csv')

    sitreps = rw.get_sitreps(iso3, date_from, date_to)
    print('  SitReps: {}'.format(len(sitreps)))
    if sitreps:
        save_csv(sitreps, os.path.join(output_dir, 'reliefweb_sitreps.csv'),
                 ['id', 'title', 'source', 'date', 'url'])
        files.append('reliefweb_sitreps.csv')

    dis = rw.search_disability(iso3, date_from, date_to)
    print('  Disability reports: {} / {} ({:.1f}%)'.format(
        dis['disability_reports'], dis['total_reports'], dis['disability_pct']))
    if dis['reports']:
        save_csv(dis['reports'], os.path.join(output_dir, 'reliefweb_disability.csv'),
                 ['title', 'source', 'format', 'date', 'url'])
        files.append('reliefweb_disability.csv')

    p_from, p_to = _extract_date_range(sitreps, 'date') if sitreps else ('', '')

    return _make_result(
        source='ReliefWeb', category='catalogue',
        total_records=facets['total'],
        disability_records=dis['disability_reports'],
        period_from=p_from, period_to=p_to,
        files=files,
        note='{} sitreps, {:.1f}% mention disability'.format(len(sitreps), dis['disability_pct']),
    )


def fetch_hapi(iso3, output_dir, date_from=None, date_to=None):
    """Fetch HDX HAPI data: all available endpoints for the country.

    date_from/date_to filter analytical endpoints (conflict, IDPs, food, returnees).
    Reference endpoints (risk, population, funding) are NOT filtered.
    """
    from hapi_client import HAPIClient
    hapi = HAPIClient()
    files = []
    all_dates = []  # Track dates for period_from/period_to

    print('\n--- HDX HAPI ---')
    if date_from:
        print('  Period filter: {} -> {}'.format(date_from, date_to or 'now'))

    def _filter_by_period(records, date_field='date_start'):
        """Post-fetch filter for analytical data within the period."""
        if not date_from:
            return records
        filtered = []
        for r in records:
            d = str(r.get(date_field, '') or '')[:10]
            if not d:
                filtered.append(r)  # Keep records with no date
                continue
            if date_from and d < date_from:
                continue
            if date_to and d > date_to:
                continue
            filtered.append(r)
        return filtered

    avail = hapi.get_data_availability(iso3)
    print('  Data available ({}/13): {}'.format(len(avail), ', '.join(avail)))
    summary_parts = []
    total = 0

    # --- ANALYTICAL endpoints (filtered by period) ---

    idps = _filter_by_period(hapi.get_idps(iso3))
    print('  IDPs: {} records'.format(len(idps)))
    if idps:
        save_csv(idps, os.path.join(output_dir, 'hapi_idps.csv'),
                 ['location_code', 'admin1_name', 'admin2_name', 'date_start', 'date_end', 'population'])
        files.append('hapi_idps.csv')
        summary_parts.append('{} IDPs'.format(len(idps)))
        total += len(idps)
        all_dates.extend(r.get('date_start', '') for r in idps)

    # --- REFERENCE endpoints (NOT filtered) ---

    ops = hapi.get_op_presence(iso3)
    print('  Op Presence: {} records'.format(len(ops)))
    if ops:
        save_csv(ops, os.path.join(output_dir, 'hapi_op_presence.csv'),
                 ['org_acronym', 'org_name', 'sector_name', 'admin1_name', 'admin2_name',
                  'date_start', 'date_end'])
        files.append('hapi_op_presence.csv')
        orgs = set(r['org_acronym'] for r in ops if r['org_acronym'])
        summary_parts.append('{} orgs'.format(len(orgs)))
        total += len(ops)

    funding = hapi.get_funding(iso3)
    print('  Funding: {} records'.format(len(funding)))
    if funding:
        save_csv(funding, os.path.join(output_dir, 'hapi_funding.csv'),
                 ['appeal_name', 'appeal_code', 'appeal_type', 'year',
                  'requirements_usd', 'funding_usd', 'funding_pct'])
        files.append('hapi_funding.csv')
        summary_parts.append('{} funding'.format(len(funding)))
        total += len(funding)

    risk = hapi.get_national_risk(iso3)
    print('  National Risk: {} records'.format(len(risk)))
    if risk:
        save_csv(risk, os.path.join(output_dir, 'hapi_risk.csv'),
                 ['location_code', 'risk_class', 'global_rank', 'overall_risk',
                  'hazard_exposure', 'vulnerability', 'coping_capacity',
                  'date_start', 'date_end'])
        files.append('hapi_risk.csv')
        summary_parts.append('risk class {}'.format(risk[0].get('risk_class', '?')))
        total += len(risk)

    # --- ANALYTICAL endpoints (filtered by period) ---

    if 'conflict-events' in avail:
        conflict = _filter_by_period(hapi.get_conflict_events(iso3))
        print('  Conflict Events: {} records'.format(len(conflict)))
        if conflict:
            save_csv(conflict, os.path.join(output_dir, 'hapi_conflict_events.csv'),
                     ['location_code', 'admin1_name', 'admin2_name', 'event_type',
                      'events', 'fatalities', 'date_start', 'date_end'])
            files.append('hapi_conflict_events.csv')
            summary_parts.append('{} conflict events'.format(len(conflict)))
            total += len(conflict)
            all_dates.extend(r.get('date_start', '') for r in conflict)

    if 'refugees-persons-of-concern' in avail:
        refugees = _filter_by_period(hapi.get_refugees(iso3))
        print('  Refugees/PoC: {} records'.format(len(refugees)))
        if refugees:
            save_csv(refugees, os.path.join(output_dir, 'hapi_refugees.csv'),
                     ['asylum_location', 'origin_location', 'origin_name',
                      'population_group', 'gender', 'age_range', 'population',
                      'date_start', 'date_end'])
            files.append('hapi_refugees.csv')
            summary_parts.append('{} refugees/PoC'.format(len(refugees)))
            total += len(refugees)
            all_dates.extend(r.get('date_start', '') for r in refugees)

    if 'food-security' in avail:
        food_sec = _filter_by_period(hapi.get_food_security(iso3))
        print('  Food Security: {} records'.format(len(food_sec)))
        if food_sec:
            save_csv(food_sec, os.path.join(output_dir, 'hapi_food_security.csv'),
                     ['location_code', 'admin1_name', 'admin2_name', 'ipc_phase',
                      'ipc_type', 'population_in_phase', 'population_fraction',
                      'date_start', 'date_end'])
            files.append('hapi_food_security.csv')
            summary_parts.append('{} food security'.format(len(food_sec)))
            total += len(food_sec)
            all_dates.extend(r.get('date_start', '') for r in food_sec)

    if 'food-prices-market-monitor' in avail:
        prices = _filter_by_period(hapi.get_food_prices(iso3))
        print('  Food Prices: {} records'.format(len(prices)))
        if prices:
            save_csv(prices, os.path.join(output_dir, 'hapi_food_prices.csv'),
                     ['location_code', 'admin1_name', 'market_name', 'commodity_name',
                      'commodity_category', 'unit', 'price', 'currency_code',
                      'price_type', 'lat', 'lon', 'date_start'])
            files.append('hapi_food_prices.csv')
            summary_parts.append('{} food prices'.format(len(prices)))
            total += len(prices)
            all_dates.extend(r.get('date_start', '') for r in prices)

    if 'baseline-population' in avail:
        pop = hapi.get_baseline_population(iso3)  # Reference - NOT filtered
        print('  Baseline Population: {} records'.format(len(pop)))
        if pop:
            save_csv(pop, os.path.join(output_dir, 'hapi_population.csv'),
                     ['location_code', 'admin1_name', 'admin2_name', 'gender',
                      'age_range', 'population', 'date_start'])
            files.append('hapi_population.csv')
            summary_parts.append('{} population'.format(len(pop)))
            total += len(pop)

    # Humanitarian Needs - KEY FOR HI: v2 `category` field carries ALL
    # disaggregation, including category='Disability' (see hapi_client docstring)
    disability_records = 0
    if 'humanitarian-needs' in avail:
        hum_needs = _filter_by_period(hapi.get_humanitarian_needs(iso3))
        print('  Humanitarian Needs: {} records'.format(len(hum_needs)))
        if hum_needs:
            save_csv(hum_needs, os.path.join(output_dir, 'hapi_humanitarian_needs.csv'),
                     ['location_code', 'admin1_name', 'admin2_name', 'admin_level',
                      'sector_code', 'sector_name', 'category', 'population_status',
                      'population', 'date_start', 'date_end', 'resource_hdx_id'])
            files.append('hapi_humanitarian_needs.csv')
            summary_parts.append('{} humanitarian needs'.format(len(hum_needs)))
            total += len(hum_needs)
            all_dates.extend(r.get('date_start', '') for r in hum_needs)
            # Disability disaggregation (v2: category label VARIES per HNO
            # vintage: 'Disability' in 2024, 'People with disability' in 2025)
            disabled_rows = [r for r in hum_needs
                             if 'disab' in (r.get('category') or '').lower()]
            disabled_inn = sum(int(r.get('population', 0) or 0) for r in disabled_rows
                               if r.get('population_status') == 'INN')
            if disabled_rows:
                disability_records = len(disabled_rows)
                print('  ** DISABILITY: {} rows with category=Disability '
                      '(in-need pop {:,} across periods - dedupe by period before use)'.format(
                          len(disabled_rows), disabled_inn))
            else:
                print('  ** No disability-disaggregated records found')

    if 'returnees' in avail:
        # returnees = a FLOW origin x asylum, not a country stock. We ask for
        # people returning TO iso3 (origin_location_code). The previous code
        # filtered on location_code, which is not a parameter of this endpoint,
        # and therefore collected the whole world and summed it.
        returnees = _filter_by_period(hapi.get_returnees(iso3, direction='origin'))
        print('  Returnees (to {}): {} records'.format(iso3, len(returnees)))
        if returnees:
            save_csv(returnees, os.path.join(output_dir, 'hapi_returnees.csv'),
                     ['origin_location_code', 'origin_location_name',
                      'asylum_location_code', 'asylum_location_name',
                      'population_group', 'gender', 'age_range',
                      'min_age', 'max_age', 'population',
                      'date_start', 'date_end', 'resource_hdx_id'])
            files.append('hapi_returnees.csv')
            # No summed population: rows repeat per group/gender/age/period.
            summary_parts.append('{} returnee rows'.format(len(returnees)))
            total += len(returnees)
            all_dates.extend(r.get('date_start', '') for r in returnees)

    if 'rainfall' in avail:
        rainfall = _filter_by_period(hapi.get_rainfall(iso3))
        print('  Rainfall: {} records'.format(len(rainfall)))
        if rainfall:
            save_csv(rainfall, os.path.join(output_dir, 'hapi_rainfall.csv'),
                     ['location_code', 'admin1_name', 'admin2_name',
                      'rainfall', 'rainfall_anomaly_pct', 'rainfall_long_term_avg',
                      'date_start', 'date_end'])
            files.append('hapi_rainfall.csv')
            summary_parts.append('{} rainfall'.format(len(rainfall)))
            total += len(rainfall)
            all_dates.extend(r.get('date_start', '') for r in rainfall)

    disability_note = ''
    if disability_records:
        disability_note = ' | ** {} disability-disaggregated records **'.format(disability_records)
    elif 'humanitarian-needs' not in avail:
        disability_note = ' | humanitarian-needs not available'
    else:
        disability_note = ' | humanitarian-needs available but no disability disaggregation'

    # Compute period from all analytical dates
    clean_dates = [str(d)[:10] for d in all_dates if d]
    p_from = min(clean_dates) if clean_dates else ''
    p_to = max(clean_dates) if clean_dates else ''

    return _make_result(
        source='HDX HAPI', category='raw',
        total_records=total,
        disability_records=disability_records,
        period_from=p_from, period_to=p_to,
        files=files,
        note=', '.join(summary_parts) + disability_note,
    )


def fetch_hdx_ckan(iso3, output_dir):
    """List ALL HDX CKAN datasets for a country + MSNA-specific search."""
    from config import SourceUnavailable
    from hdx_ckan_client import HDXClient
    hdx = HDXClient()
    files = []
    errors = []

    print('\n--- HDX CKAN ---')

    # 1. List ALL datasets for the country (comprehensive, paged to the end)
    all_datasets = hdx.list_all_datasets(iso3)
    total_count = getattr(hdx, 'last_count', len(all_datasets))
    print('  Total datasets for {}: {} (listed {})'.format(
        iso3, total_count, len(all_datasets)))
    if getattr(hdx, 'last_truncated', False):
        errors.append(('HDX CKAN listing', SourceUnavailable(
            'listed {} of {} datasets'.format(len(all_datasets), total_count))))

    if all_datasets:
        # Save full catalogue (1 row per dataset, no resource explosion)
        catalogue_rows = []
        for ds in all_datasets:
            formats = set(r['format'] for r in ds.get('resources', []) if r.get('format'))
            catalogue_rows.append({
                'dataset_name': ds['name'],
                'dataset_title': ds['title'],
                'org': ds['org'],
                # last metadata edit, NOT the period the data covers (HDX's
                # own `dataset_date`): the column used to be named dataset_date
                'metadata_modified': ds.get('metadata_modified', ''),
                'data_period': ds.get('data_period_start', ''),
                'license': ds.get('license', ''),
                'num_resources': ds['num_resources'],
                'formats': ', '.join(sorted(formats)),
                'hdx_url': ds.get('url', ''),
            })
        save_csv(catalogue_rows, os.path.join(output_dir, 'hdx_all_datasets.csv'),
                 ['dataset_name', 'dataset_title', 'org', 'metadata_modified',
                  'data_period', 'license', 'num_resources', 'formats', 'hdx_url'])
        files.append('hdx_all_datasets.csv')

    # 2. Filter for key themes (disability, needs, assessment), on the listing
    # already fetched rather than three identical queries.
    def themed(keywords):
        return [ds for ds in all_datasets
                if any(k in '{} {}'.format(ds['title'], ds.get('notes', '')).lower()
                       for k in keywords)]
    disability_ds = themed(['disability', 'disabled', 'handicap', 'inclusion'])
    needs_ds = themed(['needs assessment', 'msna', 'multi-sector'])

    print('  Disability-related: {}'.format(len(disability_ds)))
    print('  Needs assessment: {}'.format(len(needs_ds)))

    return _make_result(
        source='HDX CKAN', category='catalogue',
        total_records=len(all_datasets),
        disability_records=len(disability_ds),
        files=files,
        note='{} datasets total, {} disability-related, {} needs assessments'.format(
            total_count, len(disability_ds), len(needs_ds)),
        errors=errors,
    )

def fetch_idmc(iso3, output_dir, date_from=None):
    """Fetch IDMC displacement data (annual figures + events)."""
    from idmc_client import IDMCClient
    idmc = IDMCClient()
    files = []

    print('\n--- IDMC ---')
    if not idmc.client_id:
        print('  SKIPPED: no IDMC client_id configured')
        return _make_result(
            source='IDMC', category='raw', total_records=0, disability_records=0,
            status='skipped',
            note='Skipped - no client_id (keyring sds.idmc/client_id or IDMC_CLIENT_ID)',
        )

    # Derive year_from from period date_from
    year_from = int(date_from[:4]) if date_from else 2018
    errors = []
    total = 0

    data = _attempt(errors, 'IDMC displacements', idmc.get_displacement,
                    iso3, year_from=year_from)
    print('  Annual displacement: {} years'.format(_n(data)))
    if data:
        save_csv(data, os.path.join(output_dir, 'idmc_displacement.csv'),
                 ['iso3', 'year', 'conflict_new_displacements', 'disaster_new_displacements',
                  'conflict_stock', 'disaster_stock'])
        files.append('idmc_displacement.csv')
        total += len(data)

    events = _attempt(errors, 'IDMC events', idmc.get_displacement_events,
                      iso3, year_from=year_from)
    print('  Displacement events: {}'.format(_n(events)))
    if events:
        save_csv(events, os.path.join(output_dir, 'idmc_events.csv'),
                 ['iso3', 'event_id', 'event_name', 'year', 'displacement_type',
                  'new_displacements', 'cause', 'start_date', 'end_date'])
        files.append('idmc_events.csv')
        total += len(events)

    p_from, p_to = _extract_date_range(events, 'start_date') if events else ('', '')
    # Stock of the latest year, read from the rows above (the old second request
    # for an "overview" returned the same row). Never printed as 0 when unknown.
    if data is None:
        stock = 'n/a'
    elif not data:
        stock = 'no GIDD row'
    else:
        latest = data[-1]
        stock = '{:,} ({})'.format(latest['conflict_stock'] + latest['disaster_stock'],
                                   latest['year'])
    return _make_result(
        source='IDMC', category='raw',
        total_records=total,
        disability_records=0,
        period_from=p_from, period_to=p_to,
        files=files,
        note='Stock {} IDPs, {} events since {}'.format(stock, _n(events), year_from),
        errors=errors,
    )


def fetch_unhcr(iso3, output_dir, date_from=None):
    """Fetch UNHCR refugee population data."""
    from unhcr_client import UNHCRClient
    unhcr = UNHCRClient()
    files = []

    print('\n--- UNHCR ---')

    year_from = int(date_from[:4]) if date_from else 2018

    # Column names mirror UNHCR's own taxonomy (coa/coo family) so outputs join
    # with the official `refugees` R package and Refugee Data Finder exports.
    # The pre-2026-07-25 aliases (country_asylum, country_origin) were the bug.
    POP_COLS = ['year', 'coa', 'coa_iso', 'coa_name', 'coo', 'coo_iso', 'coo_name',
                'refugees', 'asylum_seekers', 'returned_refugees', 'idps',
                'returned_idps', 'stateless', 'oip', 'ooc', 'hst']
    pop = unhcr.get_population(country_asylum=iso3, year_from=year_from)
    print('  Population records (asylum): {}'.format(len(pop)))
    total = 0
    if pop:
        save_csv(pop, os.path.join(output_dir, 'unhcr_population.csv'), POP_COLS)
        files.append('unhcr_population.csv')
        total += len(pop)

    # Origin direction: "nationals of {iso3} displaced anywhere" is a different
    # question from "people hosted in {iso3}". For a crisis like Sudan both
    # matter, and we used to ship neither.
    pop_origin = unhcr.get_population(country_origin=iso3, year_from=year_from)
    print('  Population records (origin): {}'.format(len(pop_origin)))
    if pop_origin:
        save_csv(pop_origin,
                 os.path.join(output_dir, 'unhcr_population_origin.csv'), POP_COLS)
        files.append('unhcr_population_origin.csv')
        total += len(pop_origin)

    # get_demographics() existed but was never called: the repo extracted zero
    # age/sex disaggregation from UNHCR despite HI doctrine requiring SADD.
    demo = unhcr.get_demographics(iso3)
    print('  Demographics records: {}'.format(len(demo)))
    if demo:
        save_csv(demo, os.path.join(output_dir, 'unhcr_demographics.csv'),
                 ['year', 'coa', 'coa_iso', 'coa_name', 'coo', 'coo_iso',
                  'female_0_4', 'female_5_11', 'female_12_17', 'female_18_59',
                  'female_60_plus', 'male_0_4', 'male_5_11', 'male_12_17',
                  'male_18_59', 'male_60_plus', 'total'])
        files.append('unhcr_demographics.csv')
        total += len(demo)

    solutions = unhcr.get_solutions(country_asylum=iso3, year_from=year_from)
    print('  Solutions records: {}'.format(len(solutions)))
    if solutions:
        save_csv(solutions, os.path.join(output_dir, 'unhcr_solutions.csv'),
                 ['year', 'coa', 'coa_iso', 'coo', 'coo_iso', 'returned_refugees',
                  'resettlement', 'naturalisation', 'complementary_pathways'])
        files.append('unhcr_solutions.csv')
        total += len(solutions)

    years = [str(r.get('year', '')) for r in pop if r.get('year')]
    p_from = min(years) if years else ''
    p_to = max(years) if years else ''

    return _make_result(
        source='UNHCR', category='raw',
        total_records=total,
        disability_records=0,
        period_from=p_from, period_to=p_to,
        files=files,
        note='{} population records, {} solutions'.format(len(pop), len(solutions)),
    )


def fetch_inform(iso3, output_dir):
    """Fetch INFORM Risk Index (national + subnational)."""
    from inform_client import INFORMClient
    inform = INFORMClient()

    print('\n--- INFORM ---')
    errors = []
    files = []
    total = 0

    risk = _attempt(errors, 'INFORM scores', inform.get_country_risk, iso3)
    if risk:
        save_csv([risk], os.path.join(output_dir, 'inform_risk.csv'),
                 list(risk.keys()))
        files.append('inform_risk.csv')
        total += 1
        print('  Overall risk: {:.1f} ({})'.format(
            risk.get('overall_risk', 0), risk.get('workflow_name', '')))

    subnational = _attempt(errors, 'INFORM subnational', inform.get_subnational, iso3)
    print('  Subnational: {} admin units'.format(_n(subnational)))
    if subnational:
        save_csv(subnational, os.path.join(output_dir, 'inform_subnational.csv'),
                 list(subnational[0].keys()))
        files.append('inform_subnational.csv')
        total += len(subnational)

    if risk:
        head = 'Risk {:.1f} ({})'.format(risk['overall_risk'], risk.get('workflow_name', ''))
    elif risk is None:
        head = 'Risk n/a'
    else:
        head = ('no INFORM Risk score for {} in the latest release (national risk '
                'also in HAPI national-risk)'.format(iso3))
    return _make_result(
        source='INFORM', category='raw',
        total_records=total,
        disability_records=0,
        files=files,
        note='{}, {} subnational units'.format(head, _n(subnational)),
        errors=errors,
    )


def fetch_wfp(iso3, output_dir):
    """Fetch WFP HungerMap data."""
    from wfp_client import WFPClient
    wfp = WFPClient()
    files = []
    errors = []

    print('\n--- WFP HungerMap ---')

    data = _attempt(errors, 'WFP countryData', wfp.get_country_data, iso3)
    total = 0
    if data:
        save_csv([data], os.path.join(output_dir, 'wfp_hungermap.csv'),
                 list(data.keys()))
        files.append('wfp_hungermap.csv')
        total += 1
        fcs = data.get('fcs_people_insufficient', 0)
        print('  FCS insufficient: {:,} people'.format(fcs))

    subnational = _attempt(errors, 'WFP subnational', wfp.get_subnational, iso3)
    print('  Subnational: {} admin units'.format(_n(subnational)))
    if subnational:
        save_csv(subnational, os.path.join(output_dir, 'wfp_subnational.csv'),
                 list(subnational[0].keys()))
        files.append('wfp_subnational.csv')
        total += len(subnational)

    fcs_state = 'n/a' if data is None else ('available' if data else 'none')
    return _make_result(
        source='WFP HungerMap', category='raw',
        total_records=total,
        disability_records=0,
        files=files,
        note='{} subnational, FCS data {}'.format(_n(subnational), fcs_state),
        errors=errors,
    )


def fetch_worldbank(iso3, output_dir):
    """Fetch World Bank development indicators."""
    from worldbank_client import WorldBankClient
    wb = WorldBankClient()

    print('\n--- World Bank ---')
    errors = []

    profile = _attempt(errors, 'World Bank indicators', wb.get_country_profile,
                       iso3, year_from=2015)
    # The profile skips indicators that failed: each one makes the source partial.
    for ind_id, e in getattr(wb, 'last_failures', []):
        errors.append(('World Bank ' + ind_id, e))
    print('  Indicators: {}'.format(_n(profile)))
    if profile:
        save_csv(profile, os.path.join(output_dir, 'worldbank_profile.csv'),
                 ['iso3', 'indicator_id', 'indicator_name', 'latest_year', 'latest_value'])

    info = _attempt(errors, 'World Bank country', wb.get_country_info, iso3)

    return _make_result(
        source='World Bank', category='raw',
        total_records=len(profile or []),
        disability_records=0,
        files=['worldbank_profile.csv'] if profile else [],
        note='{} indicators, income: {}'.format(
            _n(profile), (info or {}).get('income_level') or 'n/a'),
        errors=errors,
    )


def fetch_acled(iso3, output_dir, date_from=None, date_to=None):
    """Fetch ACLED direct conflict events + CAST forecasts (requires OAuth2 credentials)."""
    from acled_client import ACLEDClient, acled_country_name
    from config import NotCovered

    print('\n--- ACLED Direct ---')
    try:
        acled = ACLEDClient()
    except ValueError as e:
        print('  SKIPPED: {}'.format(str(e).splitlines()[0]))
        return _make_result(
            source='ACLED', category='raw',
            total_records=0, disability_records=0,
            status='skipped',
            note='Skipped - no API key',
        )
    try:
        country_name = acled_country_name(iso3)
    except NotCovered as e:
        print('  NOT COVERED: {}'.format(e))
        return _make_result(
            source='ACLED', category='raw', total_records=0, disability_records=0,
            status='not_covered', note=str(e))

    files = []
    errors = []
    events = _attempt(errors, 'ACLED events', acled.get_events, country_name,
                      date_from=date_from or '2025-01-01', date_to=date_to)
    print('  Events: {}'.format(_n(events)))

    if events:
        acled.save_csv(events, os.path.join(output_dir, 'acled_events.csv'))
        files.append('acled_events.csv')

    # CAST forecasts
    forecasts = _attempt(errors, 'ACLED CAST', acled.get_cast_forecasts, country_name)
    print('  CAST forecasts: {}'.format(_n(forecasts)))
    if forecasts:
        acled.save_csv(forecasts, os.path.join(output_dir, 'acled_cast_forecasts.csv'))
        files.append('acled_cast_forecasts.csv')

    if events is None:
        note = 'events n/a'
    else:
        fatalities = sum(int(e.get('fatalities', 0) or 0) for e in events)
        note = '{} events, {:,} fatalities'.format(len(events), fatalities)
    if forecasts:
        note += ', {} CAST forecasts'.format(len(forecasts))

    p_from, p_to = _extract_date_range(events, 'event_date') if events else ('', '')

    return _make_result(
        source='ACLED', category='raw',
        total_records=len(events or []) + len(forecasts or []),
        disability_records=0,
        period_from=p_from, period_to=p_to,
        files=files,
        note=note,
        errors=errors,
    )


def fetch_acaps(iso3, output_dir):
    """Fetch ACAPS severity + access data (requires API key)."""
    from acaps_client import ACAPSClient
    acaps = ACAPSClient()

    print('\n--- ACAPS ---')
    if not acaps.api_key:
        print('  SKIPPED: no ACAPS API key configured')
        return _make_result(
            source='ACAPS', category='raw', total_records=0, disability_records=0,
            status='skipped',
            note='Skipped - no API key (keyring sds.acaps/api_key or ACAPS_API_KEY)')

    total = 0
    errors = []
    files = []

    severity = _attempt(errors, 'ACAPS inform-severity', acaps.get_inform_severity, iso3)
    print('  Severity records: {}'.format(_n(severity)))
    if severity:
        acaps.save_csv(severity, os.path.join(output_dir, 'acaps_severity.csv'))
        files.append('acaps_severity.csv')
        total += len(severity)

    access = _attempt(errors, 'ACAPS humanitarian-access', acaps.get_access_constraints, iso3)
    print('  Humanitarian access rows: {}'.format(_n(access)))
    if access:
        acaps.save_csv(access, os.path.join(output_dir, 'acaps_access.csv'))
        files.append('acaps_access.csv')
        total += len(access)

    if severity is None:
        sev_class = 'n/a'
    elif not severity:
        sev_class = 'none'
    else:
        sev_class = severity[0].get('severity_class') or 'N/A'
    return _make_result(
        source='ACAPS', category='raw',
        total_records=total,
        disability_records=0,
        files=files,
        note='Severity: {}, {} humanitarian-access rows'.format(sev_class, _n(access)),
        errors=errors,
    )


def fetch_dtm(iso3, output_dir):
    """Fetch IOM DTM datasets from HDX."""
    from dtm_client import DTMClient
    dtm = DTMClient()

    country_name = COUNTRY_NAMES.get(iso3, iso3.lower())
    print('\n--- IOM DTM ---')

    datasets = dtm.search_dtm_datasets(country_name, iso3=iso3)
    print('  DTM datasets on HDX: {}'.format(len(datasets)))

    files = []
    if datasets:
        rows = dtm.datasets_to_csv_rows(datasets)
        save_csv(rows, os.path.join(output_dir, 'dtm_datasets.csv'),
                 ['dataset_name', 'dataset_title', 'org', 'metadata_modified',
                  'resource_name', 'resource_format', 'resource_url'])
        files.append('dtm_datasets.csv')

    return _make_result(
        source='DTM/IOM', category='catalogue',
        total_records=len(datasets),
        disability_records=0,
        files=files,
        note='{} DTM datasets on HDX'.format(len(datasets)),
    )


def fetch_gdacs(iso3, output_dir, date_from=None):
    """Fetch GDACS disaster alerts for a country."""
    from gdacs_client import GDACSClient
    gdacs = GDACSClient()
    files = []

    print('\n--- GDACS ---')

    # Compute days from date_from if provided
    if date_from:
        delta = datetime.now() - datetime.strptime(date_from, '%Y-%m-%d')
        days = max(int(delta.days), 1)
    else:
        days = 180

    # Filtrer par ISO3 : le chemin par NOM rendait 0 alerte en silence ('Phl').
    alerts = gdacs.get_recent_by_iso3(iso3, days=days, limit=300)
    print('  Alerts (last {} days): {}'.format(days, len(alerts)))

    if alerts:
        save_csv(alerts, os.path.join(output_dir, 'gdacs_alerts.csv'),
                 ['event_id', 'event_type', 'event_type_name', 'alert_level',
                  'severity_value', 'severity_text', 'country', 'name',
                  'date_start', 'date_end', 'lon', 'lat', 'url',
                  # colonnes ajoutees par le correctif GDACS (severite lue dans
                  # severitydata, pays affectes en ISO3, evenements multi-pays)
                  'severity_unit', 'alert_score', 'affected_iso3',
                  'affected_countries', 'n_countries_affected', 'is_current', 'glide'])
        files.append('gdacs_alerts.csv')

    p_from, p_to = _extract_date_range(alerts, 'date_start') if alerts else ('', '')
    red = sum(1 for a in alerts if a['alert_level'] == 'Red')
    orange = sum(1 for a in alerts if a['alert_level'] == 'Orange')
    errors = []
    if getattr(gdacs, 'last_saturated', False):
        errors.append(('GDACS window', ValueError(
            'server limit reached: events older than the 300 most recent worldwide '
            'were not examined, the list may be incomplete')))
    return _make_result(
        source='GDACS', category='raw',
        total_records=len(alerts),
        disability_records=0,
        period_from=p_from, period_to=p_to,
        files=files,
        note='{} alerts ({} red, {} orange)'.format(len(alerts), red, orange),
        errors=errors,
    )


def fetch_hpc(iso3, output_dir, date_from=None):
    """Fetch HPC/FTS funding flows and response plans."""
    from hpc_client import HPCClient
    hpc = HPCClient()
    files = []
    errors = []

    print('\n--- HPC/FTS ---')
    total = 0

    plans = _attempt(errors, 'HPC plans', hpc.get_plans, iso3)
    print('  Response plans: {}'.format(_n(plans)))
    if plans:
        save_csv(plans, os.path.join(output_dir, 'hpc_plans.csv'),
                 ['plan_id', 'plan_name', 'plan_type', 'year',
                  'requirements_usd', 'funding_usd', 'coverage_pct'])
        files.append('hpc_plans.csv')
        total += len(plans)

    # Use year from period if provided, else current year
    flow_year = int(date_from[:4]) if date_from else datetime.now().year
    flow_cap = 200
    flows = _attempt(errors, 'HPC flows', hpc.get_funding_flows,
                     iso3, year=flow_year, limit=flow_cap)
    print('  Funding flows ({}): {}'.format(flow_year, _n(flows)))
    if flows:
        save_csv(flows, os.path.join(output_dir, 'hpc_funding_flows.csv'),
                 ['flow_id', 'amount_usd', 'source_org', 'destination_org',
                  'plan', 'cluster', 'flow_date', 'status', 'description', 'boundary'])
        files.append('hpc_funding_flows.csv')
        total += len(flows)

    p_from, p_to = _extract_date_range(flows, 'flow_date') if flows else ('', '')
    # No sum of the flows here. The listed flows are capped (one page) and a flow
    # appears once per boundary, so their sum is neither complete nor a total: the
    # funding figure is the API aggregate carried by each plan (funding_usd).
    latest = ''
    if plans and plans[0].get('coverage_pct') is not None:
        latest = ' (latest {}: {}% funded)'.format(plans[0]['year_max'],
                                                    plans[0]['coverage_pct'])
    capped = (', capped at {} - not the full list'.format(flow_cap)
              if flows is not None and len(flows) >= flow_cap else '')
    return _make_result(
        source='HPC/FTS', category='raw',
        total_records=total,
        disability_records=0,
        period_from=p_from, period_to=p_to,
        files=files,
        note='{} plans{}, {} flows listed for {}{}'.format(
            _n(plans), latest, _n(flows), flow_year, capped),
        errors=errors,
    )


def fetch_ifrcgo(iso3, output_dir):
    """Fetch IFRC Go emergencies, appeals, and 3W projects."""
    from ifrcgo_client import IFRCGoClient
    ifrc = IFRCGoClient()

    print('\n--- IFRC Go ---')
    total = 0
    files = []
    errors = []

    events = _attempt(errors, 'IFRC GO emergencies', ifrc.get_emergencies,
                      iso3=iso3, limit=50)
    print('  Emergencies: {}'.format(_n(events)))
    if events:
        save_csv(events, os.path.join(output_dir, 'ifrcgo_emergencies.csv'),
                 ['event_id', 'name', 'dtype', 'is_featured', 'num_affected', 'num_dead',
                  'num_injured', 'num_displaced', 'num_missing', 'date_start',
                  'countries', 'glide', 'appeal_amount_requested_chf',
                  'appeal_amount_funded_chf', 'currency'])
        files.append('ifrcgo_emergencies.csv')
        total += len(events)

    appeals = _attempt(errors, 'IFRC GO appeals', ifrc.get_appeals, iso3=iso3, limit=50)
    print('  Appeals: {}'.format(_n(appeals)))
    if appeals:
        save_csv(appeals, os.path.join(output_dir, 'ifrcgo_appeals.csv'),
                 ['appeal_id', 'code', 'name', 'atype', 'status', 'country',
                  'amount_requested_chf', 'amount_funded_chf', 'currency', 'coverage_pct',
                  'num_beneficiaries', 'start_date', 'end_date'])
        files.append('ifrcgo_appeals.csv')
        total += len(appeals)

    projects = _attempt(errors, 'IFRC GO projects', ifrc.get_projects, iso3=iso3, limit=100)
    print('  3W Projects: {}'.format(_n(projects)))
    if projects:
        save_csv(projects, os.path.join(output_dir, 'ifrcgo_projects.csv'),
                 ['project_id', 'name', 'reporting_ns', 'primary_sector',
                  'programme_type', 'status', 'budget_amount',
                  'target_total', 'reached_total', 'start_date', 'end_date'])
        files.append('ifrcgo_projects.csv')
        total += len(projects)

    p_from, p_to = _extract_date_range(events, 'date_start') if events else ('', '')
    return _make_result(
        source='IFRC Go', category='raw',
        total_records=total,
        disability_records=0,
        period_from=p_from, period_to=p_to,
        files=files,
        note='{} emergencies, {} appeals, {} projects'.format(
            _n(events), _n(appeals), _n(projects)),
        errors=errors,
    )


def fetch_impact(iso3, output_dir):
    """Fetch IMPACT/REACH resources for a country (MSNA datasets + all resources)."""
    from impact_client import IMPACTClient
    impact = IMPACTClient()

    print('\n--- IMPACT/REACH ---')
    total = 0

    # 1. MSNA datasets (programme=756, type=777)
    msna = impact.search_msna_datasets(iso3)
    print('  MSNA datasets: {}'.format(len(msna)))
    if msna:
        save_csv(msna, os.path.join(output_dir, 'impact_msna_datasets.csv'),
                 ['title', 'url', 'country', 'doc_type', 'programme', 'sector',
                  'pub_date', 'collection_date', 'file_format'])
        total += len(msna)

    # 2. All resources for the country (all types)
    all_res = impact.search(location_iso3=iso3, limit=50, order='latest')
    all_count = all_res['total']
    resources = all_res['resources']
    print('  All resources: {} total ({} on page 1)'.format(all_count, len(resources)))
    if resources:
        save_csv(resources, os.path.join(output_dir, 'impact_all_resources.csv'),
                 ['title', 'url', 'country', 'doc_type', 'programme', 'sector',
                  'pub_date', 'collection_date', 'file_format'])
        total += len(resources)

    files = []
    if msna:
        files.append('impact_msna_datasets.csv')
    if resources:
        files.append('impact_all_resources.csv')

    return _make_result(
        source='IMPACT/REACH', category='catalogue',
        total_records=all_count,
        disability_records=len(msna),
        files=files,
        note='{} MSNA datasets, {} total resources'.format(len(msna), all_count),
    )


def fetch_dtm_portal(iso3, output_dir, max_pages=3):
    """Catalogue DTM du pays, AVEC les liens de telechargement directs.

    Reecrit 2026-07-25 (test persona "IM qui telecharge"). L'ancienne version
    scrapait titres+urls de FICHES sans lien de fichier : le flux
    `01b_download.py` scannait donc "0 downloadable" alors que 95 % du catalogue
    DTM est telechargeable. `browse_catalogue` porte `download_url` par ligne, et
    `01b` detecte cette colonne -> le chemin catalogue -> selection ->
    telechargement fonctionne enfin de bout en bout.
    """
    from config import SourceUnavailable
    from dtm_client import DTMClient
    dtm = DTMClient()
    errors = []

    print('\n--- DTM Portal (catalogue) ---')
    # UnmappedCountry is a NotCovered: it ends the source as `not_covered`.
    rows = _attempt(errors, 'DTM catalogue', dtm.browse_catalogue, iso3,
                    max_pages=max_pages)
    if rows is None:
        return _make_result(
            source='DTM Portal', category='catalogue',
            total_records=0, disability_records=0, note='', errors=errors,
        )
    if dtm.last_browse_error:
        # Rows were read, then a later page failed: a partial listing.
        errors.append(('DTM catalogue', SourceUnavailable(
            'walk stopped at ' + dtm.last_browse_error)))

    n_open = sum(1 for r in rows if r['access'] == 'open')
    print('  {} datasets ({} telechargeables, {} verrouilles) sur {} page(s)'.format(
        len(rows), n_open, len(rows) - n_open, max_pages))
    files = []
    if rows:
        save_csv(rows, os.path.join(output_dir, 'dtm_portal_datasets.csv'),
                 ['slug', 'title', 'published', 'country_slug', 'activities',
                  'dataset_format', 'access', 'download_url', 'url'])
        files.append('dtm_portal_datasets.csv')

    msna = [r for r in rows
            if 'msna' in (r['title'] + r['activities']).lower()
            or 'needs assessment' in (r['title'] + r['activities']).lower()]
    if msna:
        print('  dont MSNA / needs assessment : {}'.format(len(msna)))

    return _make_result(
        source='DTM Portal', category='catalogue',
        total_records=len(rows),
        disability_records=0,      # aucun produit DTM ne porte de ventilation handicap
        files=files,
        note='{} datasets, {} downloadable ({} MSNA), newest {} page(s) only'.format(
            len(rows), n_open, len(msna), max_pages),
        errors=errors,
    )


def fetch_liveuamap(iso3, output_dir, date_from=None, date_to=None, max_pages=200):
    """Fetch Liveuamap conflict events for a country (scraping, no API key)."""
    from config import SourceUnavailable
    from liveuamap_client import LiveuamapClient
    client = LiveuamapClient()
    files = []
    errors = []

    print('\n--- Liveuamap ---')

    events = _attempt(errors, 'Liveuamap', client.get_events, iso3,
                      max_pages=max_pages, date_from=date_from, date_to=date_to)
    meta = client.last_meta or {}
    if events is not None and meta.get('stopped_on_errors'):
        errors.append(('Liveuamap pagination', SourceUnavailable(
            'stopped after repeated errors at page {}: older events missing'
            .format(meta.get('pages')))))
    if events is not None and meta.get('truncated_before'):
        errors.append(('Liveuamap window', ValueError(
            '--max-pages {} reached at {} before --date-from {}: the period is only '
            'partly covered, raise --max-pages'.format(
                max_pages, meta['truncated_before'], date_from))))

    if events:
        save_csv(events, os.path.join(output_dir, 'liveuamap_events.csv'),
                 ['event_id', 'feed', 'feed_scope', 'datetime', 'event_type', 'cat_id',
                  'color_id', 'name', 'location', 'lat', 'lng',
                  'source_url', 'link', 'picture'])
        files.append('liveuamap_events.csv')

    note = '{} conflict events (scraped)'.format(_n(events))
    if meta.get('feed_scope') == 'binational':
        note += '; binational feed "{}": events not split by country'.format(
            meta.get('subdomain'))
    if meta.get('stale_days'):
        # A frozen feed returns few or no recent events: that is not calm.
        note += '; feed STALE, newest event {} ({} days old)'.format(
            meta['newest_event'], meta['stale_days'])
    p_from, p_to = _extract_date_range(events, 'datetime') if events else ('', '')
    return _make_result(
        source='Liveuamap', category='raw',
        total_records=len(events or []),
        disability_records=0,
        period_from=p_from, period_to=p_to,
        files=files,
        note=note,
        errors=errors,
    )


def _iso_date(value):
    """argparse type: a YYYY-MM-DD date, kept as the string the clients expect.

    Validated once here because the sources disagreed on a malformed date: GDACS
    raised, HPC read the first 4 characters, Liveuamap ignored it and fetched the
    whole feed without any date filter.
    """
    try:
        datetime.strptime(value, '%Y-%m-%d')
    except ValueError:
        raise argparse.ArgumentTypeError(
            'expected a date as YYYY-MM-DD, got {!r}'.format(value))
    return value


SUMMARY_FIELDS = ['source', 'status', 'category', 'total_records',
                  'disability_records', 'period_from', 'period_to', 'files', 'note']


def main():
    parser = argparse.ArgumentParser(
        description='Fetch secondary data for a country',
        epilog='Exit status: 0 when every requested source answered (ok, empty, '
               'skipped or not covered), 1 when at least one source is partial, '
               'unavailable or in error, 2 on invalid arguments.')
    parser.add_argument('iso3', help='ISO3 country code (e.g., LBN, SDN, SYR)')
    parser.add_argument('--date-from', type=_iso_date,
                        help='Start date YYYY-MM-DD (ReliefWeb, HAPI, IDMC, UNHCR, '
                             'ACLED, GDACS, HPC flows year, Liveuamap)')
    parser.add_argument('--date-to', type=_iso_date,
                        help='End date YYYY-MM-DD (ReliefWeb, HAPI, ACLED, Liveuamap)')
    parser.add_argument('--skip-hdx', action='store_true', help='Skip HDX CKAN search')
    parser.add_argument('--skip-acled', action='store_true', help='Skip ACLED (needs API key)')
    parser.add_argument('--skip-acaps', action='store_true', help='Skip ACAPS (needs API key)')
    parser.add_argument('--skip-worldbank', action='store_true', help='Skip World Bank')
    parser.add_argument('--skip-dtm', action='store_true', help='Skip DTM/IOM')
    parser.add_argument('--skip-gdacs', action='store_true', help='Skip GDACS')
    parser.add_argument('--skip-hpc', action='store_true', help='Skip HPC/FTS')
    parser.add_argument('--skip-ifrcgo', action='store_true', help='Skip IFRC Go')
    parser.add_argument('--only', help='Comma-separated list of sources to fetch '
                                       '(e.g., reliefweb,hapi,idmc)')
    parser.add_argument('--output-dir', help='Override output directory')
    parser.add_argument('--max-pages', type=int, default=200,
                        help='Max pagination pages for Liveuamap (default 200)')
    args = parser.parse_args()

    # Validated, not just upper-cased: this value becomes a directory name below.
    from config import normalize_iso3
    try:
        iso3 = normalize_iso3(args.iso3)
    except ValueError as e:
        print(e)
        return 2
    if args.date_from and args.date_to and args.date_from > args.date_to:
        print('--date-from {} is after --date-to {}'.format(args.date_from, args.date_to))
        return 2

    # Determine which sources to run. An unknown name is an error before any
    # request, not a warning lost in the middle of a 17-source run.
    skipped_by_flag = []
    skip_flags = [k for k in ('hdx', 'acled', 'acaps', 'worldbank', 'dtm', 'gdacs',
                              'hpc', 'ifrcgo') if getattr(args, 'skip_' + k)]
    if args.only and skip_flags:
        # --only used to override every --skip-* without a word.
        print('--only and --skip-* cannot be combined: list the sources you want '
              'in --only instead.')
        return 2
    if args.only:
        sources = [s.strip().lower() for s in args.only.split(',') if s.strip()]
        unknown = [s for s in sources if s not in ALL_SOURCES]
        if unknown:
            print('Unknown source(s) in --only: {}. Available: {}'.format(
                ', '.join(unknown), ', '.join(ALL_SOURCES)))
            return 2
    else:
        sources = list(ALL_SOURCES)
        for key in ('hdx', 'acled', 'acaps', 'worldbank', 'dtm', 'gdacs', 'hpc',
                    'ifrcgo'):
            if getattr(args, 'skip_' + key):
                sources.remove(key)
                skipped_by_flag.append(key)

    # Output: {ISO3}_data/ with raw/ and catalogue/ subdirs
    base_dir = args.output_dir or os.path.join(PROJECT_DIR, '{}_data'.format(iso3))
    output_dir = os.path.join(base_dir, 'raw')           # raw data CSVs
    catalogue_dir = os.path.join(base_dir, 'catalogue')  # dataset listings
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(catalogue_dir, exist_ok=True)
    stale = clear_previous_outputs(sources, [output_dir, catalogue_dir])

    print('=' * 60)
    print('Secondary Data Sources - {} ({})'.format(iso3, datetime.now().strftime('%Y-%m-%d %H:%M')))
    print('Output: {}'.format(os.path.abspath(base_dir)))
    print('Sources: {}'.format(', '.join(sources)))
    if args.date_from:
        print('Period: {} -> {}'.format(args.date_from, args.date_to or 'now'))
    if stale:
        print('Removed {} file(s) of these sources from an earlier run'.format(len(stale)))
    print('=' * 60)

    summaries = []

    # Date parameters for raw data sources
    df = args.date_from
    dt = args.date_to
    mp = args.max_pages

    # Source dispatch - catalogue sources go to catalogue_dir, raw data to raw/
    # Note: ReliefWeb is catalogue but keeps date_from/date_to (we want period-filtered listings)
    dispatch = {
        'reliefweb': lambda: fetch_reliefweb(iso3, catalogue_dir, df, dt),
        'hapi': lambda: fetch_hapi(iso3, output_dir, df, dt),
        'hdx': lambda: fetch_hdx_ckan(iso3, catalogue_dir),
        'idmc': lambda: fetch_idmc(iso3, output_dir, df),
        'unhcr': lambda: fetch_unhcr(iso3, output_dir, df),
        'inform': lambda: fetch_inform(iso3, output_dir),
        'wfp': lambda: fetch_wfp(iso3, output_dir),
        'worldbank': lambda: fetch_worldbank(iso3, output_dir),
        'acled': lambda: fetch_acled(iso3, output_dir, df, dt),
        'acaps': lambda: fetch_acaps(iso3, output_dir),
        'dtm': lambda: fetch_dtm(iso3, catalogue_dir),
        'gdacs': lambda: fetch_gdacs(iso3, output_dir, df),
        'hpc': lambda: fetch_hpc(iso3, output_dir, df),
        'ifrcgo': lambda: fetch_ifrcgo(iso3, output_dir),
        'impact': lambda: fetch_impact(iso3, catalogue_dir),
        'dtm_portal': lambda: fetch_dtm_portal(iso3, catalogue_dir),
        'liveuamap': lambda: fetch_liveuamap(iso3, output_dir, df, dt, mp),
    }

    for source in sources:
        name = SOURCE_DISPLAY_NAMES.get(source, source)
        try:
            summaries.append(dispatch[source]())
        except Exception as e:  # noqa: BLE001 - recorded with its state below
            print('  {} {}: {}'.format(_failure_state(e).upper(), source, _short(e)))
            summaries.append(_make_result(
                source=name, category=SOURCE_CATEGORIES.get(source, ''),
                total_records=0, disability_records=0, note='',
                errors=[(name, e)],
            ))
    for source in skipped_by_flag:
        summaries.append(_make_result(
            source=SOURCE_DISPLAY_NAMES.get(source, source),
            category=SOURCE_CATEGORIES.get(source, ''),
            total_records=0, disability_records=0, status='skipped',
            note='excluded by --skip-{}'.format(source)))

    # One row per source, whatever happened to it. Before this file came back, a
    # source that failed left no trace on disk: its missing CSV was
    # indistinguishable from a country without data.
    save_csv(summaries, os.path.join(base_dir, 'fetch_summary.csv'), SUMMARY_FIELDS,
             audit=False)

    # Data inventory - 1 row per file across both dirs
    inventory = build_inventory(summaries, output_dir, catalogue_dir, ran=sources)
    if inventory:
        save_csv(inventory, os.path.join(base_dir, 'data_inventory.csv'),
                 INVENTORY_FIELDS, audit=False)

    print('\n' + '=' * 60)
    print('SUMMARY - {}'.format(iso3))
    print('=' * 60)
    for s in summaries:
        print('  {:<14} {:<11} {:>6} records (disability: {}) | {}'.format(
            s['source'], s['status'].upper(), s['total_records'],
            s['disability_records'], s['note']))

    # List files
    raw_files = [f for f in sorted(os.listdir(output_dir))
                 if f.endswith('.csv')]
    catalogue_files = [f for f in sorted(os.listdir(catalogue_dir))
                       if f.endswith('.csv')] if os.path.isdir(catalogue_dir) else []

    if raw_files:
        print('\nRaw - {} files'.format(len(raw_files)))
        for f in raw_files:
            size = os.path.getsize(os.path.join(output_dir, f))
            print('  {} ({:,} bytes)'.format(f, size))
    if catalogue_files:
        print('\nCatalogue - {} files'.format(len(catalogue_files)))
        for f in catalogue_files:
            size = os.path.getsize(os.path.join(catalogue_dir, f))
            print('  {} ({:,} bytes)'.format(f, size))

    print('\nOutput: {}'.format(os.path.abspath(base_dir)))

    print('=' * 60)
    failed = [s for s in summaries if s['status'] in FAILED_STATES]
    if failed:
        print('INCOMPLETE: {} of {} sources did not answer in full ({}).'.format(
            len(failed), len(summaries),
            ', '.join('{} {}'.format(s['source'], s['status']) for s in failed)))
        print('A source without a file above FAILED: that is not an absence of data '
              'for {}. Details in fetch_summary.csv.'.format(iso3))
        return 1
    return 0


if __name__ == '__main__':
    # sys.exit so a rejected country code returns a non-zero status to the caller
    # (a shell loop, a CI step, or an agent) instead of looking like a clean run.
    sys.exit(main())
