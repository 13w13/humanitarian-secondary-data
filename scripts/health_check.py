"""
Health Check - Secondary Data Sources
=====================================
One minimal, side-effect-free probe per source (17 dispatch keys, mirrors
fetch_country_data.py). Classifies each source and writes a durable report.

Semantics:
  GREEN  = HTTP call returned AND client parsed it AND >=1 record on the test country
  YELLOW = call succeeded but 0 records (suspicious: test country is data-rich)
  RED    = exception / non-2xx (status code or exception text captured)
  SKIP   = missing free credential (config gap, not a broken API) or no country id

A probe = ONE underlying HTTP call. It does NOT reproduce fetch_*'s full fan-out
(e.g. HAPI here only hits data-availability, not the ~10 data endpoints).
No CSVs are written; the only artifact is the report.

Usage:
    python -X utf8 health_check.py            # default country SDN
    python -X utf8 health_check.py SYR
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import io
import time
import contextlib
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, os.path.join(SCRIPT_DIR, 'clients'))

COUNTRY = (sys.argv[1].upper() if len(sys.argv) > 1 else 'SDN')
COUNTRY_NAMES = {
    'SDN': 'Sudan', 'SYR': 'Syria', 'YEM': 'Yemen', 'LBN': 'Lebanon',
    'AFG': 'Afghanistan', 'UKR': 'Ukraine', 'COD': 'Congo', 'PSE': 'Palestine',
    'ETH': 'Ethiopia', 'SOM': 'Somalia', 'IRQ': 'Iraq', 'NGA': 'Nigeria',
}
CNAME = COUNTRY_NAMES.get(COUNTRY, COUNTRY.title())

_d30 = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
_d21 = (datetime.now() - timedelta(days=21)).strftime('%Y-%m-%d')
_year = datetime.now().year


# ─── Detect missing free credentials (keyring or env) ──────────────
def _cred(service, field, envvar):
    try:
        import keyring
        if keyring.get_password(service, field):
            return True
    except Exception:
        pass
    return bool(os.environ.get(envvar))


KEYLESS_MISSING = set()
if not _cred('sds.acaps', 'api_key', 'ACAPS_API_KEY'):
    KEYLESS_MISSING.add('acaps')
if not _cred('sds.idmc', 'client_id', 'IDMC_CLIENT_ID'):
    KEYLESS_MISSING.add('idmc')


# ─── Probes: each returns (count:int, detail:str), raises on failure ─
# ONE HTTP call each. No save_csv, no writes.
def p_reliefweb():
    from reliefweb_client import ReliefWebClient
    r = ReliefWebClient().get_facets(COUNTRY)
    return r.get('total', 0), 'reports listed'

def p_hapi():
    from hapi_client import HAPIClient
    avail = HAPIClient().get_data_availability(COUNTRY)
    has_dis = 'humanitarian-needs' in avail
    return len(avail), '{}/13 endpoints; humanitarian-needs(disability)={}'.format(
        len(avail), 'yes' if has_dis else 'NO')

def p_hdx():
    from hdx_ckan_client import HDXClient
    ds = HDXClient().search_datasets(CNAME.lower(), rows=5)
    return len(ds), 'datasets (sample of 5-row search)'

def p_idmc():
    from idmc_client import IDMCClient
    ov = IDMCClient().get_country_overview(COUNTRY)
    n = (ov or {}).get('total_stock', 0) if ov else 0
    return (1 if n else 0), 'stock {:,} IDPs'.format(n)

def p_unhcr():
    from unhcr_client import UNHCRClient
    pop = UNHCRClient().get_population(country_asylum=COUNTRY, year_from=_year - 1, year_to=_year - 1)
    return len(pop), 'population rows ({})'.format(_year - 1)

def p_inform():
    from inform_client import INFORMClient
    r = INFORMClient().get_country_risk(COUNTRY)
    if not r:
        return 0, 'no risk record'
    return 1, 'risk {}'.format(r.get('overall_risk', '?'))

def p_wfp():
    from wfp_client import WFPClient, WFP_NOT_PUBLIC
    if COUNTRY in WFP_NOT_PUBLIC:
        raise _NoCountryId('HungerMap withholds {} publicly (restricted-display '
                           'list) - food security via HAPI'.format(COUNTRY))
    d = WFPClient().get_country_data(COUNTRY)
    fcs = (d or {}).get('fcs_people_insufficient', 0)
    return (1 if d else 0), 'FCS insufficient {:,}'.format(fcs) if d else 'no data'

def p_acaps():
    from acaps_client import ACAPSClient
    sev = ACAPSClient().get_inform_severity(COUNTRY)
    return len(sev), 'severity records'

def p_dtm():
    from dtm_client import DTMClient
    ds = DTMClient().search_dtm_datasets(CNAME.lower(), rows=5)
    return len(ds), 'DTM datasets on HDX (5-row search)'

def p_worldbank():
    from worldbank_client import WorldBankClient
    info = WorldBankClient().get_country_info(COUNTRY)
    return (1 if info else 0), 'income: {}'.format(info.get('income_level', 'N/A'))

def p_acled():
    from acled_client import ACLEDClient
    c = ACLEDClient()  # OAuth2 token fetch happens here
    ev = c.get_events(CNAME, date_from=_d21, limit=50)
    return len(ev), 'events since {}'.format(_d21)

def p_gdacs():
    from gdacs_client import GDACSClient
    # Filtrer par ISO3 : le chemin par NOM rend 404/0 alerte (bug 'Phl').
    a = GDACSClient().get_recent_by_iso3(COUNTRY, days=30, limit=300)
    return len(a), 'alerts /30d (disaster-specific)'

def p_hpc():
    from hpc_client import HPCClient
    plans = HPCClient().get_plans(COUNTRY)
    return len(plans), 'response plans'

def p_ifrcgo():
    from ifrcgo_client import IFRCGoClient
    ev = IFRCGoClient().get_emergencies(iso3=COUNTRY, limit=5)
    return len(ev), 'emergencies (limit 5)'

def p_impact():
    from impact_client import IMPACTClient
    res = IMPACTClient().search(location_iso3=COUNTRY, limit=5)
    return res.get('total', 0), 'REACH resources total'

def p_dtm_portal():
    from dtm_client import DTMClient, PORTAL_COUNTRY_IDS
    cid = PORTAL_COUNTRY_IDS.get(COUNTRY)
    if not cid:
        raise _NoCountryId('no DTM portal id for {}'.format(COUNTRY))
    res = DTMClient().search_portal_datasets(country_id=cid)
    return len(res.get('datasets', [])), 'portal datasets'

def p_liveuamap():
    # 30-day window: low-volume feeds (e.g. sudan, ~2 events/week) legitimately
    # return 0 over 7 days - that read as "frozen" when it was just sparse.
    from liveuamap_client import LiveuamapClient
    ev = LiveuamapClient().get_events(COUNTRY, max_pages=1, date_from=_d30)
    return len(ev), 'events (1 page, since {})'.format(_d30)


class _NoCountryId(Exception):
    pass


class _Quiet(io.StringIO):
    """StringIO that tolerates the clients' module-level sys.stdout.reconfigure()."""
    def reconfigure(self, *a, **k):
        pass

    def detach(self, *a, **k):
        return self


# key, display, fn  - mirrors the 17 dispatch keys of fetch_country_data.py
PROBES = [
    ('reliefweb', 'ReliefWeb', p_reliefweb),
    ('hapi', 'HDX HAPI', p_hapi),
    ('hdx', 'HDX CKAN', p_hdx),
    ('idmc', 'IDMC', p_idmc),
    ('unhcr', 'UNHCR', p_unhcr),
    ('inform', 'INFORM', p_inform),
    ('wfp', 'WFP HungerMap', p_wfp),
    ('worldbank', 'World Bank', p_worldbank),
    ('acled', 'ACLED', p_acled),
    ('acaps', 'ACAPS', p_acaps),
    ('dtm', 'DTM/IOM (HDX)', p_dtm),
    ('gdacs', 'GDACS', p_gdacs),
    ('hpc', 'HPC/FTS', p_hpc),
    ('ifrcgo', 'IFRC Go', p_ifrcgo),
    ('impact', 'IMPACT/REACH', p_impact),
    ('dtm_portal', 'DTM Portal', p_dtm_portal),
    ('liveuamap', 'Liveuamap', p_liveuamap),
]

STATUS_ICON = {'GREEN': 'OK  ', 'YELLOW': 'WARN', 'RED': 'FAIL', 'SKIP': 'SKIP'}


def _err_short(e):
    code = getattr(e, 'code', None)  # urllib HTTPError.code
    if code:
        reason = getattr(e, 'reason', '') or getattr(e, 'msg', '')
        return 'HTTP {} {}'.format(code, reason).strip()
    txt = str(e).strip().replace('\n', ' ')
    return '{}: {}'.format(type(e).__name__, txt[:70]) if txt else type(e).__name__


def run():
    print('=' * 74)
    print('HEALTH CHECK - Secondary Data Sources | country={} ({}) | {}'.format(
        COUNTRY, CNAME, datetime.now().strftime('%Y-%m-%d %H:%M')))
    print('=' * 74)
    results = []
    for key, display, fn in PROBES:
        t = time.time()
        err = None
        count = 0
        detail = ''
        try:
            # Suppress client chatter; keep our table clean
            with contextlib.redirect_stdout(_Quiet()):
                count, detail = fn()
        except Exception as e:  # noqa: BLE001 - probe isolation is the point
            err = e
        ms = (time.time() - t) * 1000

        if err is not None:
            if key in KEYLESS_MISSING:
                status = 'SKIP'
                detail = 'needs free key ({})'.format(_err_short(err))
            elif isinstance(err, _NoCountryId):
                status = 'SKIP'
                detail = str(err)
            else:
                status = 'RED'
                detail = _err_short(err)
        else:
            if key in KEYLESS_MISSING:
                # unexpected success without a configured key
                status = 'GREEN' if count > 0 else 'YELLOW'
                detail = (detail + ' [public endpoint]').strip()
            elif count > 0:
                status = 'GREEN'
            else:
                status = 'YELLOW'
                detail = detail or '0 records'

        results.append((key, display, status, count, ms, detail))
        print('  [{}] {:<16} {:>7.0f}ms  n={:<6} {}'.format(
            STATUS_ICON[status], display, ms, count, detail))

    # Tally
    tally = {}
    for _, _, status, _, _, _ in results:
        tally[status] = tally.get(status, 0) + 1
    print('-' * 74)
    print('  {} sources  |  GREEN {}  YELLOW {}  RED {}  SKIP {}'.format(
        len(results), tally.get('GREEN', 0), tally.get('YELLOW', 0),
        tally.get('RED', 0), tally.get('SKIP', 0)))
    print('=' * 74)

    _write_report(results, tally)
    return results


def _write_report(results, tally):
    stamp = datetime.now().strftime('%Y-%m-%d')
    out = os.path.join(PROJECT_DIR, 'health_check_{}.md'.format(stamp))
    icon = {'GREEN': '🟢', 'YELLOW': '🟡', 'RED': '🔴', 'SKIP': '⚪'}
    lines = []
    lines.append('# Health check - Secondary Data Sources')
    lines.append('')
    lines.append('- **Date**: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M')))
    lines.append('- **Test country**: {} ({})'.format(COUNTRY, CNAME))
    lines.append('- **Result**: {} green, {} yellow, {} red, {} skip (of {} sources)'.format(
        tally.get('GREEN', 0), tally.get('YELLOW', 0), tally.get('RED', 0),
        tally.get('SKIP', 0), len(results)))
    lines.append('')
    lines.append('Green = reachable + parsed + ≥1 record. Yellow = parsed but 0 records '
                 '(suspicious on a data-rich country). Red = exception/non-2xx. '
                 'Skip = missing free key / no country id. One HTTP call per probe.')
    lines.append('')
    lines.append('| Source | Status | Latency | Records | Detail |')
    lines.append('|--------|--------|--------:|--------:|--------|')
    for key, display, status, count, ms, detail in results:
        lines.append('| {} | {} {} | {:.0f} ms | {} | {} |'.format(
            display, icon[status], status, ms, count, detail.replace('|', '/')))
    lines.append('')
    with open(out, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print('\nReport: {}'.format(out))


if __name__ == '__main__':
    run()
