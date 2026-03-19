"""
Unified Country Data Fetcher
=============================
Fetch all secondary data sources for a given country (ISO3 code).
Generates CSVs in data/{ISO3}/ directory.

Sources (14): ReliefWeb, HDX HAPI, HDX CKAN, IDMC, UNHCR, INFORM,
         WFP HungerMap, World Bank, GDACS, HPC/FTS, IFRC Go
         + optional ACLED, ACAPS, DTM.

Usage:
    python -X utf8 fetch_country_data.py LBN
    python -X utf8 fetch_country_data.py SDN --date-from 2026-01-01 --date-to 2026-03-31
    python -X utf8 fetch_country_data.py LBN --skip-hdx --skip-worldbank
    python -X utf8 fetch_country_data.py LBN --only reliefweb,hapi,idmc

Objectives: O4 (innovation), O5 (evidence-based), O2 (pack IM urgence)
"""
import sys
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
    'HTI': 'haiti', 'IRQ': 'iraq', 'KEN': 'kenya', 'LBN': 'lebanon',
    'LBY': 'libya', 'MLI': 'mali', 'MMR': 'myanmar', 'MOZ': 'mozambique',
    'NER': 'niger', 'NGA': 'nigeria', 'PAK': 'pakistan', 'PSE': 'palestine',
    'SDN': 'sudan', 'SOM': 'somalia', 'SSD': 'south sudan', 'SYR': 'syria',
    'TCD': 'chad', 'UKR': 'ukraine', 'YEM': 'yemen',
}

ALL_SOURCES = [
    'reliefweb', 'hapi', 'hdx', 'idmc', 'unhcr', 'inform',
    'wfp', 'worldbank', 'acled', 'acaps', 'dtm',
    'gdacs', 'hpc', 'ifrcgo', 'impact', 'dtm_portal',
]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

# Source categories: catalogue (listings to review) vs analytical (exploitable data)
SOURCE_CATEGORIES = {
    'reliefweb': 'catalogue',   # Listings of reports (not the content)
    'hapi': 'analytical',
    'hdx': 'catalogue',
    'idmc': 'analytical',
    'unhcr': 'analytical',
    'inform': 'analytical',     # Risk scores = exploitable data
    'wfp': 'analytical',
    'worldbank': 'analytical',  # Indicators = exploitable data
    'acled': 'analytical',
    'acaps': 'analytical',
    'dtm': 'catalogue',
    'gdacs': 'analytical',
    'hpc': 'analytical',
    'ifrcgo': 'analytical',
    'impact': 'catalogue',
    'dtm_portal': 'catalogue',
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
}

SUMMARY_FIELDS = [
    'source', 'category', 'total_records', 'disability_records',
    'period_from', 'period_to', 'last_update', 'files', 'note',
]


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


def _make_result(source, category, total_records, disability_records, note,
                 period_from='', period_to='', last_update='', files=None):
    """Build a standardized result dict for fetch_summary."""
    return {
        'source': source,
        'category': category,
        'total_records': total_records,
        'disability_records': disability_records,
        'period_from': period_from or '',
        'period_to': period_to or '',
        'last_update': last_update or '',
        'files': ', '.join(files) if files else '',
        'note': note,
    }


def save_csv(rows, filepath, fieldnames):
    """Save list of dicts to CSV. Retries once on PermissionError (OneDrive lock)."""
    parent = os.path.dirname(os.path.abspath(filepath))
    if parent:
        os.makedirs(parent, exist_ok=True)
    for attempt in range(3):
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
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


def build_inventory(summaries, raw_dir, catalogue_dir=None):
    """Build data_inventory.csv rows from fetch summaries.

    Scans raw_dir and catalogue_dir for actual CSV files produced by each source,
    and returns one inventory row per file with records count, size, etc.
    """
    # Map source display name -> summary dict
    summary_by_source = {s['source']: s for s in summaries}

    inventory = []
    for directory, dir_label in [(raw_dir, 'analytical'), (catalogue_dir, 'catalogue')]:
        if not directory or not os.path.isdir(directory):
            continue
        for fname in sorted(os.listdir(directory)):
            if not fname.endswith('.csv') or fname in ('fetch_summary.csv', 'data_inventory.csv'):
                continue
            fpath = os.path.join(directory, fname)
            size = os.path.getsize(fpath)
            # Count rows (excluding header)
            records = 0
            try:
                with open(fpath, encoding='utf-8') as f:
                    records = max(0, sum(1 for _ in f) - 1)
            except Exception:
                pass
            # Find matching summary
            matched = None
            for s in summaries:
                s_files = s.get('files', '')
                if fname in s_files:
                    matched = s
                    break
            inventory.append({
                'file': fname,
                'source': matched['source'] if matched else '',
                'category': matched['category'] if matched else dir_label,
                'records': records,
                'size_bytes': size,
                'period_from': matched.get('period_from', '') if matched else '',
                'period_to': matched.get('period_to', '') if matched else '',
                'directory': dir_label,
                'note': matched.get('note', '') if matched else '',
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
        pop = hapi.get_baseline_population(iso3)  # Reference — NOT filtered
        print('  Baseline Population: {} records'.format(len(pop)))
        if pop:
            save_csv(pop, os.path.join(output_dir, 'hapi_population.csv'),
                     ['location_code', 'admin1_name', 'admin2_name', 'gender',
                      'age_range', 'population', 'date_start'])
            files.append('hapi_population.csv')
            summary_parts.append('{} population'.format(len(pop)))
            total += len(pop)

    # Humanitarian Needs — KEY FOR HI: contains disabled_marker field
    disability_records = 0
    if 'humanitarian-needs' in avail:
        hum_needs = _filter_by_period(hapi.get_humanitarian_needs(iso3))
        print('  Humanitarian Needs: {} records'.format(len(hum_needs)))
        if hum_needs:
            save_csv(hum_needs, os.path.join(output_dir, 'hapi_humanitarian_needs.csv'),
                     ['location_code', 'admin1_name', 'admin2_name', 'sector_name',
                      'population_group', 'population_status', 'gender', 'age_range',
                      'disabled_marker', 'population', 'date_start', 'date_end'])
            files.append('hapi_humanitarian_needs.csv')
            summary_parts.append('{} humanitarian needs'.format(len(hum_needs)))
            total += len(hum_needs)
            all_dates.extend(r.get('date_start', '') for r in hum_needs)
            # Disability disaggregation analysis
            disabled_y = [r for r in hum_needs if r.get('disabled_marker') == 'y']
            disabled_pop = sum(int(r.get('population', 0) or 0) for r in disabled_y)
            if disabled_y:
                disability_records = len(disabled_y)
                print('  ** DISABILITY: {} records with disabled_marker=y (pop {:,})'.format(
                    len(disabled_y), disabled_pop))
            else:
                print('  ** No disability-disaggregated records found')

    if 'returnees' in avail:
        returnees = _filter_by_period(hapi.get_returnees(iso3))
        print('  Returnees: {} records'.format(len(returnees)))
        if returnees:
            save_csv(returnees, os.path.join(output_dir, 'hapi_returnees.csv'),
                     ['location_code', 'admin1_name', 'admin2_name',
                      'origin_location_code', 'origin_location_name',
                      'population_group', 'gender', 'age_range',
                      'population', 'date_start', 'date_end'])
            files.append('hapi_returnees.csv')
            total_ret_pop = sum(int(r.get('population', 0) or 0) for r in returnees)
            summary_parts.append('{} returnees (pop {:,})'.format(len(returnees), total_ret_pop))
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
        source='HDX HAPI', category='analytical',
        total_records=total,
        disability_records=disability_records,
        period_from=p_from, period_to=p_to,
        files=files,
        note=', '.join(summary_parts) + disability_note,
    )


def fetch_hdx_ckan(iso3, output_dir):
    """List ALL HDX CKAN datasets for a country + MSNA-specific search."""
    from hdx_ckan_client import HDXClient
    hdx = HDXClient()
    files = []

    country_name = COUNTRY_NAMES.get(iso3, iso3.lower())
    print('\n--- HDX CKAN ---')

    # 1. List ALL datasets for the country (comprehensive)
    all_datasets = hdx.list_all_datasets(iso3)
    total_count = len(all_datasets)
    print('  Total datasets for {}: {}'.format(iso3, total_count))

    if all_datasets:
        # Save full catalogue (1 row per dataset, no resource explosion)
        catalogue_rows = []
        for ds in all_datasets:
            formats = set(r['format'] for r in ds.get('resources', []) if r.get('format'))
            catalogue_rows.append({
                'dataset_name': ds['name'],
                'dataset_title': ds['title'],
                'org': ds['org'],
                'dataset_date': ds['date'],
                'license': ds.get('license', ''),
                'num_resources': ds['num_resources'],
                'formats': ', '.join(sorted(formats)),
                'hdx_url': ds.get('url', ''),
            })
        save_csv(catalogue_rows, os.path.join(output_dir, 'hdx_all_datasets.csv'),
                 ['dataset_name', 'dataset_title', 'org', 'dataset_date', 'license',
                  'num_resources', 'formats', 'hdx_url'])
        files.append('hdx_all_datasets.csv')

    # 2. Filter for key themes (disability, needs, assessment)
    disability_ds = hdx.list_all_datasets(iso3, theme_filter=[
        'disability', 'disabled', 'handicap', 'inclusion'])
    needs_ds = hdx.list_all_datasets(iso3, theme_filter=[
        'needs assessment', 'msna', 'multi-sector'])

    print('  Disability-related: {}'.format(len(disability_ds)))
    print('  Needs assessment: {}'.format(len(needs_ds)))

    return _make_result(
        source='HDX CKAN', category='catalogue',
        total_records=total_count,
        disability_records=len(disability_ds),
        files=files,
        note='{} datasets total, {} disability-related, {} needs assessments'.format(
            total_count, len(disability_ds), len(needs_ds)),
    )


def fetch_idmc(iso3, output_dir, date_from=None):
    """Fetch IDMC displacement data (annual figures + events)."""
    from idmc_client import IDMCClient
    idmc = IDMCClient()
    files = []

    print('\n--- IDMC ---')

    # Derive year_from from period date_from
    year_from = int(date_from[:4]) if date_from else 2018

    overview = idmc.get_country_overview(iso3)
    total = 0

    data = idmc.get_displacement(iso3, year_from=year_from)
    print('  Annual displacement: {} years'.format(len(data)))
    if data:
        save_csv(data, os.path.join(output_dir, 'idmc_displacement.csv'),
                 ['iso3', 'year', 'conflict_new_displacements', 'disaster_new_displacements',
                  'conflict_stock', 'disaster_stock'])
        files.append('idmc_displacement.csv')
        total += len(data)

    events = idmc.get_displacement_events(iso3, year_from=year_from)
    print('  Displacement events: {}'.format(len(events)))
    if events:
        save_csv(events, os.path.join(output_dir, 'idmc_events.csv'),
                 ['iso3', 'event_id', 'event_name', 'year', 'displacement_type',
                  'new_displacements', 'cause', 'start_date', 'end_date'])
        files.append('idmc_events.csv')
        total += len(events)

    p_from, p_to = _extract_date_range(events, 'start_date') if events else ('', '')
    stock = overview.get('total_stock', 0) if overview else 0
    return _make_result(
        source='IDMC', category='analytical',
        total_records=total,
        disability_records=0,
        period_from=p_from, period_to=p_to,
        files=files,
        note='Stock {:,} IDPs, {} events since {}'.format(stock, len(events), year_from),
    )


def fetch_unhcr(iso3, output_dir, date_from=None):
    """Fetch UNHCR refugee population data."""
    from unhcr_client import UNHCRClient
    unhcr = UNHCRClient()
    files = []

    print('\n--- UNHCR ---')

    year_from = int(date_from[:4]) if date_from else 2018

    pop = unhcr.get_population(country_asylum=iso3, year_from=year_from)
    print('  Population records: {}'.format(len(pop)))
    total = 0
    if pop:
        save_csv(pop, os.path.join(output_dir, 'unhcr_population.csv'),
                 ['year', 'country_asylum', 'country_asylum_name', 'country_origin',
                  'country_origin_name', 'refugees', 'asylum_seekers', 'idps',
                  'stateless', 'oip', 'ooc', 'hst'])
        files.append('unhcr_population.csv')
        total += len(pop)

    solutions = unhcr.get_solutions(country_asylum=iso3, year_from=year_from)
    print('  Solutions records: {}'.format(len(solutions)))
    if solutions:
        save_csv(solutions, os.path.join(output_dir, 'unhcr_solutions.csv'),
                 ['year', 'country_asylum', 'country_origin', 'returned_refugees',
                  'resettlement', 'naturalisation', 'complementary_pathways'])
        files.append('unhcr_solutions.csv')
        total += len(solutions)

    years = [str(r.get('year', '')) for r in pop if r.get('year')]
    p_from = min(years) if years else ''
    p_to = max(years) if years else ''

    return _make_result(
        source='UNHCR', category='analytical',
        total_records=total,
        disability_records=0,
        period_from=p_from, period_to=p_to,
        files=files,
        note='{} population records, {} solutions'.format(len(pop), len(solutions)),
    )


def fetch_inform(iso3, output_dir):
    """Fetch INFORM Risk Index (national + subnational).

    Note: JRC server has been restructured (late 2025). API may be temporarily
    unavailable. Wrapped in try/except for graceful degradation.
    """
    from inform_client import INFORMClient
    inform = INFORMClient()

    print('\n--- INFORM ---')

    try:
        risk = inform.get_country_risk(iso3)
    except Exception as e:
        print('  INFORM API temporarily unavailable: {}'.format(e))
        return _make_result(
            source='INFORM', category='analytical',
            total_records=0, disability_records=0,
            note='API temporarily unavailable (JRC restructuring)',
        )

    files = []
    total = 0
    if risk:
        save_csv([risk], os.path.join(output_dir, 'inform_risk.csv'),
                 list(risk.keys()))
        files.append('inform_risk.csv')
        total += 1
        print('  Overall risk: {:.1f} (rank {})'.format(
            risk.get('overall_risk', 0), risk.get('overall_rank', '')))

    try:
        subnational = inform.get_subnational(iso3)
    except Exception:
        subnational = []
    print('  Subnational: {} admin units'.format(len(subnational)))
    if subnational:
        save_csv(subnational, os.path.join(output_dir, 'inform_subnational.csv'),
                 list(subnational[0].keys()))
        files.append('inform_subnational.csv')
        total += len(subnational)

    return _make_result(
        source='INFORM', category='analytical',
        total_records=total,
        disability_records=0,
        files=files,
        note='Risk {:.1f}, {} subnational units'.format(
            risk.get('overall_risk', 0) if risk else 0, len(subnational)),
    )


def fetch_wfp(iso3, output_dir):
    """Fetch WFP HungerMap data."""
    from wfp_client import WFPClient
    wfp = WFPClient()
    files = []

    print('\n--- WFP HungerMap ---')

    data = wfp.get_country_data(iso3)
    total = 0
    if data:
        save_csv([data], os.path.join(output_dir, 'wfp_hungermap.csv'),
                 list(data.keys()))
        files.append('wfp_hungermap.csv')
        total += 1
        fcs = data.get('fcs_people_insufficient', 0)
        print('  FCS insufficient: {:,} people'.format(fcs))

    subnational = wfp.get_subnational(iso3)
    print('  Subnational: {} admin units'.format(len(subnational)))
    if subnational:
        save_csv(subnational, os.path.join(output_dir, 'wfp_subnational.csv'),
                 list(subnational[0].keys()))
        files.append('wfp_subnational.csv')
        total += len(subnational)

    return _make_result(
        source='WFP HungerMap', category='analytical',
        total_records=total,
        disability_records=0,
        files=files,
        note='{} subnational, FCS data {}'.format(
            len(subnational), 'available' if data else 'unavailable'),
    )


def fetch_worldbank(iso3, output_dir):
    """Fetch World Bank development indicators."""
    from worldbank_client import WorldBankClient
    wb = WorldBankClient()

    print('\n--- World Bank ---')

    profile = wb.get_country_profile(iso3, year_from=2015)
    print('  Indicators: {}'.format(len(profile)))
    if profile:
        save_csv(profile, os.path.join(output_dir, 'worldbank_profile.csv'),
                 ['iso3', 'indicator_id', 'indicator_name', 'latest_year', 'latest_value'])

    info = wb.get_country_info(iso3)

    return _make_result(
        source='World Bank', category='analytical',
        total_records=len(profile),
        disability_records=0,
        files=['worldbank_profile.csv'] if profile else [],
        note='{} indicators, income: {}'.format(
            len(profile), info.get('income_level', 'N/A')),
    )


def fetch_acled(iso3, output_dir, date_from=None, date_to=None):
    """Fetch ACLED direct conflict events + CAST forecasts (requires OAuth2 credentials)."""
    from acled_client import ACLEDClient

    print('\n--- ACLED Direct ---')
    try:
        acled = ACLEDClient()
    except ValueError as e:
        print('  SKIPPED: {}'.format(e))
        return _make_result(
            source='ACLED', category='analytical',
            total_records=0, disability_records=0,
            note='Skipped — no API key',
        )

    files = []
    country_name = COUNTRY_NAMES.get(iso3, iso3.lower()).title()
    events = acled.get_events(country_name, date_from=date_from or '2025-01-01',
                              date_to=date_to)
    print('  Events: {}'.format(len(events)))

    if events:
        acled.save_csv(events, os.path.join(output_dir, 'acled_events.csv'))
        files.append('acled_events.csv')

    fatalities = sum(int(e.get('fatalities', 0) or 0) for e in events)

    # CAST forecasts
    cast_count = 0
    try:
        forecasts = acled.get_cast_forecasts(country_name)
        cast_count = len(forecasts)
        print('  CAST forecasts: {}'.format(cast_count))
        if forecasts:
            acled.save_csv(forecasts, os.path.join(output_dir, 'acled_cast_forecasts.csv'))
            files.append('acled_cast_forecasts.csv')
    except Exception as e:
        print('  CAST forecasts: unavailable ({})'.format(e))

    note = '{} events, {:,} fatalities'.format(len(events), fatalities)
    if cast_count:
        note += ', {} CAST forecasts'.format(cast_count)

    p_from, p_to = _extract_date_range(events, 'event_date') if events else ('', '')

    return _make_result(
        source='ACLED', category='analytical',
        total_records=len(events) + cast_count,
        disability_records=0,
        period_from=p_from, period_to=p_to,
        files=files,
        note=note,
    )


def fetch_acaps(iso3, output_dir):
    """Fetch ACAPS severity + access data (requires API key)."""
    from acaps_client import ACAPSClient
    acaps = ACAPSClient()

    print('\n--- ACAPS ---')
    total = 0

    severity = acaps.get_inform_severity(iso3)
    print('  Severity records: {}'.format(len(severity)))
    if severity:
        acaps.save_csv(severity, os.path.join(output_dir, 'acaps_severity.csv'))
        total += len(severity)

    access = acaps.get_access_constraints(iso3)
    print('  Access constraints: {}'.format(len(access)))
    if access:
        acaps.save_csv(access, os.path.join(output_dir, 'acaps_access.csv'))
        total += len(access)

    files = []
    if severity:
        files.append('acaps_severity.csv')
    if access:
        files.append('acaps_access.csv')

    sev_class = severity[0].get('severity_class', 'N/A') if severity else 'N/A'
    return _make_result(
        source='ACAPS', category='analytical',
        total_records=total,
        disability_records=0,
        files=files,
        note='Severity: {}, {} access constraints'.format(sev_class, len(access)),
    )


def fetch_dtm(iso3, output_dir):
    """Fetch IOM DTM datasets from HDX."""
    from dtm_client import DTMClient
    dtm = DTMClient()

    country_name = COUNTRY_NAMES.get(iso3, iso3.lower())
    print('\n--- IOM DTM ---')

    datasets = dtm.search_dtm_datasets(country_name)
    print('  DTM datasets on HDX: {}'.format(len(datasets)))

    files = []
    if datasets:
        rows = dtm.datasets_to_csv_rows(datasets)
        save_csv(rows, os.path.join(output_dir, 'dtm_datasets.csv'),
                 ['dataset_name', 'dataset_title', 'org', 'date',
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

    country_name = COUNTRY_NAMES.get(iso3, iso3.lower()).title()
    print('\n--- GDACS ---')

    # Compute days from date_from if provided
    if date_from:
        from datetime import timedelta
        delta = datetime.now() - datetime.strptime(date_from, '%Y-%m-%d')
        days = max(int(delta.days), 1)
    else:
        days = 180

    alerts = gdacs.get_recent_by_country(country_name, days=days, limit=50)
    print('  Alerts (last {} days): {}'.format(days, len(alerts)))

    if alerts:
        save_csv(alerts, os.path.join(output_dir, 'gdacs_alerts.csv'),
                 ['event_id', 'event_type', 'event_type_name', 'alert_level',
                  'severity_value', 'severity_text', 'country', 'name',
                  'date_start', 'date_end', 'lon', 'lat', 'population_affected', 'url'])
        files.append('gdacs_alerts.csv')

    p_from, p_to = _extract_date_range(alerts, 'date_start') if alerts else ('', '')
    red = sum(1 for a in alerts if a['alert_level'] == 'Red')
    orange = sum(1 for a in alerts if a['alert_level'] == 'Orange')
    return _make_result(
        source='GDACS', category='analytical',
        total_records=len(alerts),
        disability_records=0,
        period_from=p_from, period_to=p_to,
        files=files,
        note='{} alerts ({} red, {} orange)'.format(len(alerts), red, orange),
    )


def fetch_hpc(iso3, output_dir, date_from=None):
    """Fetch HPC/FTS funding flows and response plans."""
    from hpc_client import HPCClient
    hpc = HPCClient()
    files = []

    print('\n--- HPC/FTS ---')
    total = 0

    plans = hpc.get_plans(iso3)
    print('  Response plans: {}'.format(len(plans)))
    if plans:
        save_csv(plans, os.path.join(output_dir, 'hpc_plans.csv'),
                 ['plan_id', 'plan_name', 'plan_type', 'year',
                  'requirements_usd', 'funding_usd', 'coverage_pct'])
        files.append('hpc_plans.csv')
        total += len(plans)

    # Use year from period if provided, else current year
    flow_year = int(date_from[:4]) if date_from else datetime.now().year
    flows = hpc.get_funding_flows(iso3, year=flow_year, limit=200)
    print('  Funding flows ({}): {}'.format(flow_year, len(flows)))
    if flows:
        save_csv(flows, os.path.join(output_dir, 'hpc_funding_flows.csv'),
                 ['flow_id', 'amount_usd', 'source_org', 'destination_org',
                  'plan', 'cluster', 'flow_date', 'status', 'description', 'boundary'])
        files.append('hpc_funding_flows.csv')
        total += len(flows)

    p_from, p_to = _extract_date_range(flows, 'flow_date') if flows else ('', '')
    total_funding = sum(float(f.get('amount_usd', 0) or 0) for f in flows)
    return _make_result(
        source='HPC/FTS', category='analytical',
        total_records=total,
        disability_records=0,
        period_from=p_from, period_to=p_to,
        files=files,
        note='{} plans, {} flows (${:,.0f} total {})'.format(
            len(plans), len(flows), total_funding, flow_year),
    )


def fetch_ifrcgo(iso3, output_dir):
    """Fetch IFRC Go emergencies, appeals, and 3W projects."""
    from ifrcgo_client import IFRCGoClient
    ifrc = IFRCGoClient()

    print('\n--- IFRC Go ---')
    total = 0

    events = ifrc.get_emergencies(iso3=iso3, limit=50)
    print('  Emergencies: {}'.format(len(events)))
    if events:
        save_csv(events, os.path.join(output_dir, 'ifrcgo_emergencies.csv'),
                 ['event_id', 'name', 'dtype', 'status', 'num_affected', 'num_dead',
                  'num_injured', 'num_displaced', 'num_missing', 'date_start',
                  'countries', 'glide', 'appeal_amount_requested', 'appeal_amount_funded'])
        total += len(events)

    appeals = ifrc.get_appeals(iso3=iso3, limit=50)
    print('  Appeals: {}'.format(len(appeals)))
    if appeals:
        save_csv(appeals, os.path.join(output_dir, 'ifrcgo_appeals.csv'),
                 ['appeal_id', 'code', 'name', 'atype', 'status', 'country',
                  'amount_requested', 'amount_funded', 'coverage_pct',
                  'num_beneficiaries', 'start_date', 'end_date'])
        total += len(appeals)

    projects = ifrc.get_projects(iso3=iso3, limit=100)
    print('  3W Projects: {}'.format(len(projects)))
    if projects:
        save_csv(projects, os.path.join(output_dir, 'ifrcgo_projects.csv'),
                 ['project_id', 'name', 'reporting_ns', 'primary_sector',
                  'programme_type', 'status', 'budget_amount',
                  'target_total', 'reached_total', 'start_date', 'end_date'])
        total += len(projects)

    files = []
    if events:
        files.append('ifrcgo_emergencies.csv')
    if appeals:
        files.append('ifrcgo_appeals.csv')
    if projects:
        files.append('ifrcgo_projects.csv')

    p_from, p_to = _extract_date_range(events, 'date_start') if events else ('', '')
    return _make_result(
        source='IFRC Go', category='analytical',
        total_records=total,
        disability_records=0,
        period_from=p_from, period_to=p_to,
        files=files,
        note='{} emergencies, {} appeals, {} projects'.format(
            len(events), len(appeals), len(projects)),
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


def fetch_dtm_portal(iso3, output_dir):
    """Fetch DTM datasets from portal (MSNA + all datasets for the country)."""
    from dtm_client import DTMClient, PORTAL_COUNTRY_IDS
    dtm = DTMClient()

    print('\n--- DTM Portal ---')
    country_id = PORTAL_COUNTRY_IDS.get(iso3.upper())
    if not country_id:
        print('  No DTM portal country ID for {}'.format(iso3))
        return _make_result(
            source='DTM Portal', category='catalogue',
            total_records=0, disability_records=0,
            note='No portal country ID for {}'.format(iso3),
        )

    # 1. All datasets for the country
    result = dtm.search_portal_datasets(country_id=country_id)
    all_datasets = result['datasets']
    print('  All datasets: {}'.format(len(all_datasets)))
    if all_datasets:
        save_csv(all_datasets, os.path.join(output_dir, 'dtm_portal_datasets.csv'),
                 ['title', 'url', 'slug'])

    # 2. MSNA specifically
    msna = dtm.search_portal_msna(iso3)
    print('  MSNA datasets: {}'.format(len(msna)))
    if msna:
        save_csv(msna, os.path.join(output_dir, 'dtm_portal_msna.csv'),
                 ['title', 'url', 'slug'])

    files = []
    if all_datasets:
        files.append('dtm_portal_datasets.csv')
    if msna:
        files.append('dtm_portal_msna.csv')

    return _make_result(
        source='DTM Portal', category='catalogue',
        total_records=len(all_datasets),
        disability_records=len(msna),
        files=files,
        note='{} datasets total, {} MSNA'.format(len(all_datasets), len(msna)),
    )


def main():
    parser = argparse.ArgumentParser(description='Fetch secondary data for a country')
    parser.add_argument('iso3', help='ISO3 country code (e.g., LBN, SDN, SYR)')
    parser.add_argument('--date-from', help='Start date YYYY-MM-DD (for ReliefWeb/ACLED)')
    parser.add_argument('--date-to', help='End date YYYY-MM-DD (for ReliefWeb)')
    parser.add_argument('--skip-hdx', action='store_true', help='Skip HDX CKAN search')
    parser.add_argument('--skip-acled', action='store_true', help='Skip ACLED (needs API key)')
    parser.add_argument('--skip-acaps', action='store_true', help='Skip ACAPS (needs API key)')
    parser.add_argument('--skip-worldbank', action='store_true', help='Skip World Bank')
    parser.add_argument('--skip-dtm', action='store_true', help='Skip DTM/IOM')
    parser.add_argument('--skip-gdacs', action='store_true', help='Skip GDACS')
    parser.add_argument('--skip-hpc', action='store_true', help='Skip HPC/FTS')
    parser.add_argument('--skip-ifrcgo', action='store_true', help='Skip IFRC Go')
    parser.add_argument('--only', help='Comma-separated list of sources to fetch (e.g., reliefweb,hapi,idmc)')
    parser.add_argument('--output-dir', help='Override output directory')
    args = parser.parse_args()

    iso3 = args.iso3.upper()
    output_dir = args.output_dir or os.path.join(PROJECT_DIR, 'data', iso3)
    data_dir = os.path.dirname(output_dir)  # parent = data/
    catalogue_dir = os.path.join(data_dir, 'catalogue')
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(catalogue_dir, exist_ok=True)

    # Determine which sources to run
    if args.only:
        sources = [s.strip().lower() for s in args.only.split(',')]
    else:
        sources = list(ALL_SOURCES)
        if args.skip_hdx:
            sources.remove('hdx')
        if args.skip_acled:
            sources.remove('acled')
        if args.skip_acaps:
            sources.remove('acaps')
        if args.skip_worldbank:
            sources.remove('worldbank')
        if args.skip_dtm:
            sources.remove('dtm')
        if args.skip_gdacs:
            sources.remove('gdacs')
        if args.skip_hpc:
            sources.remove('hpc')
        if args.skip_ifrcgo:
            sources.remove('ifrcgo')

    print('=' * 60)
    print('Secondary Data Sources — {} ({})'.format(iso3, datetime.now().strftime('%Y-%m-%d %H:%M')))
    print('Analytical -> {}'.format(output_dir))
    print('Catalogue  -> {}'.format(catalogue_dir))
    print('Sources: {}'.format(', '.join(sources)))
    if args.date_from:
        print('Period: {} -> {}'.format(args.date_from, args.date_to or 'now'))
    print('=' * 60)

    summaries = []

    # Date parameters for analytical sources
    df = args.date_from
    dt = args.date_to

    # Source dispatch — catalogue sources go to catalogue_dir, analytical to output_dir
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
    }

    for source in sources:
        if source not in dispatch:
            print('\n  WARNING: Unknown source "{}", skipping'.format(source))
            continue
        try:
            summaries.append(dispatch[source]())
        except Exception as e:
            print('  ERROR {}: {}'.format(source, e))
            summaries.append(_make_result(
                source=SOURCE_DISPLAY_NAMES.get(source, source),
                category=SOURCE_CATEGORIES.get(source, ''),
                total_records=0, disability_records=0, note=str(e),
            ))

    # Summary in raw/ (backward compat)
    save_csv(summaries, os.path.join(output_dir, 'fetch_summary.csv'), SUMMARY_FIELDS)

    # Data inventory — 1 row per file across both dirs
    inventory = build_inventory(summaries, output_dir, catalogue_dir)
    if inventory:
        save_csv(inventory, os.path.join(data_dir, 'data_inventory.csv'), INVENTORY_FIELDS)

    print('\n' + '=' * 60)
    print('SUMMARY — {}'.format(iso3))
    print('=' * 60)
    for s in summaries:
        print('  {} [{}] — {} records (disability: {}) | {}'.format(
            s['source'], s['category'], s['total_records'], s['disability_records'], s['note']))

    # List files per directory
    for label, d in [('Analytical (raw/)', output_dir), ('Catalogue (catalogue/)', catalogue_dir)]:
        if os.path.isdir(d):
            files = [f for f in sorted(os.listdir(d)) if f.endswith('.csv')]
            if files:
                print('\n{} — {} files'.format(label, len(files)))
                for f in files:
                    size = os.path.getsize(os.path.join(d, f))
                    print('  {} ({:,} bytes)'.format(f, size))

    if inventory:
        print('\nInventory: {} files -> {}'.format(
            len(inventory), os.path.join(data_dir, 'data_inventory.csv')))

    print('=' * 60)


if __name__ == '__main__':
    main()
