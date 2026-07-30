"""
Secondary Data Sources — Configuration
=======================================
API endpoints, appnames, and constants.
NO secrets here — credentials in keyring or env vars.
"""

# ─── ReliefWeb ──────────────────────────────────────────────
# API v1 was decommissioned (returns 410 Gone). v2 is fully v1-compatible.
# Since 2025-11-01 the appname must be PRE-APPROVED (request at reliefweb.int/help/api).
# Your personal pre-approved appname goes in keyring sds.reliefweb/appname
# (NOT here — this file is published to the public GitHub repo).
RELIEFWEB_BASE = 'https://api.reliefweb.int/v2'
RELIEFWEB_APPNAME = 'humanitarian-secondary-data'  # generic fallback identifier

# ─── HDX HAPI ───────────────────────────────────────────────
HAPI_BASE = 'https://hapi.humdata.org/api/v2'
# app_identifier: base64 of "your-app:your@email.com"
# Generate yours: python -c "import base64; print(base64.b64encode(b'my-app:me@example.com').decode())"
HAPI_APP_ID = 'aHVtYW5pdGFyaWFuLXNlY29uZGFyeS1kYXRhOm5vcmVwbHlAZXhhbXBsZS5jb20='

# ─── HDX CKAN ───────────────────────────────────────────────
HDX_CKAN_BASE = 'https://data.humdata.org/api/3/action'
# JWT API key via keyring sds.hdx/api_key (Authorization header)

# ─── ACLED ──────────────────────────────────────────────────
ACLED_BASE = 'https://acleddata.com/api/acled/read'  # New endpoint (2025+)
ACLED_CAST_URL = 'https://acleddata.com/api/cast/read'  # CAST conflict forecasts
ACLED_DELETED_URL = 'https://acleddata.com/api/deleted/read'  # Deleted events (sync)
ACLED_TOKEN_URL = 'https://acleddata.com/oauth/token'
# ACLED OAuth2 password grant — keyring sds.acled/{email,password}
# 3 endpoints: events (/acled), forecasts (/cast), deleted (/deleted)

# ─── IDMC ───────────────────────────────────────────────────
IDMC_GRAPHQL = 'https://api.internal-displacement.org/graphql'  # DEPRECATED — 404 since late 2025
IDMC_REST_BASE = 'https://helix-tools-api.idmcdb.org/external-api'
# IDMC REST requires client_id — email ch.datainfo@idmc.ch (free)
# Set env var: IDMC_CLIENT_ID

# ─── UNHCR ─────────────────────────────────────────────────
UNHCR_BASE = 'https://api.unhcr.org/population/v1'

# ─── INFORM ────────────────────────────────────────────────
INFORM_BASE = 'https://drmkc.jrc.ec.europa.eu/inform-index/API/InformAPI/countries'
INFORM_SUBNATIONAL_BASE = 'https://drmkc.jrc.ec.europa.eu/inform-index/API/InformAPI/Subnational'

# ─── WFP HungerMap ─────────────────────────────────────────
WFP_HUNGERMAP_BASE = 'https://api.hungermapdata.org/v2'

# ─── ACAPS ─────────────────────────────────────────────────
ACAPS_BASE = 'https://api.acaps.org/api/v1'
# ACAPS requires free API key — set ACAPS_API_KEY env var

# ─── DTM / IOM ─────────────────────────────────────────────
DTM_API_BASE = 'https://dtm.iom.int/api/v2'

# ─── World Bank ────────────────────────────────────────────
WORLDBANK_BASE = 'https://api.worldbank.org/v2'

# ─── GDACS ────────────────────────────────────────────────
GDACS_BASE = 'https://www.gdacs.org/gdacsapi/api'
# Public, no auth. Real-time disaster alerts (EQ, TC, FL, VO, WF, DR)

# ─── HPC / FTS ───────────────────────────────────────────
HPC_BASE = 'https://api.hpc.tools/v1/public'
# Public (appname optional). Humanitarian funding flows, plans, emergencies

# ─── IFRC Go ─────────────────────────────────────────────
IFRCGO_BASE = 'https://goadmin.ifrc.org/api/v2'
# Public for core endpoints. Emergencies, appeals, field reports, 3W projects

# ─── API Keys — keyring (preferred) or env vars ─────────
# Convention: keyring service = sds.{provider}, username = field name
#
#   keyring.set_password('sds.hdx', 'api_key', '...')     # HDX HAPI JWT token
#   keyring.set_password('sds.acled', 'email', '...')
#   keyring.set_password('sds.acled', 'password', '...')
#   keyring.set_password('sds.acaps', 'api_key', '...')
#   keyring.set_password('sds.idmc', 'client_id', '...')
#
# Fallback env vars: ACLED_EMAIL, ACLED_PASSWORD, ACAPS_API_KEY, IDMC_CLIENT_ID
#
# Registration:
#   ACLED — acleddata.com (free account, OAuth2 password grant)
#   ACAPS — api.acaps.org/register (free)
#   IDMC — email ch.datainfo@idmc.ch (free)

# ─── Liveuamap ──────────────────────────────────────────────
# Public, no auth. Conflict events scraped from HTML (base64 ovens + AJAX pagination).
# Covers: SDN, SYR, YEM, LBN, IRQ, AFG, COD, PSE, MMR, ETH, LBY + more
LIVEUAMAP_PAGE_DELAY = 1.5  # seconds between pagination requests
LIVEUAMAP_MAX_PAGES = 200   # safety cap per region

# ─── Common ─────────────────────────────────────────────────
DEFAULT_TIMEOUT = 30
DEFAULT_PAGE_SIZE = 1000
USER_AGENT = 'humanitarian-secondary-data/1.0'
RATE_LIMIT_DELAY = 0.5  # seconds between API calls


# ─── Shared utilities ──────────────────────────────────────
import csv
import os


def get_credential(service, field, *env_vars):
    """Resolve one credential: OS keychain first, then environment variables.

    Why this exists rather than `import keyring` in every client. The README
    promises the toolkit runs with no `pip install`, and `keyring` is not in the
    standard library. Six clients imported it at call time without a guard, so on
    a machine without keyring the failure was a raw ModuleNotFoundError from
    inside a constructor, which reads like a broken toolkit rather than a missing
    optional extra. Here a missing keyring simply means "no keychain available,
    use the environment".

    Returns '' when nothing is set, so callers can treat empty as "not configured"
    without distinguishing absent-keyring from absent-secret.
    """
    try:
        import keyring
    except Exception:                      # not installed, or no usable backend
        pass
    else:
        try:
            got = keyring.get_password(service, field)
            if got:
                return got
        except Exception:                  # locked keychain, D-Bus absent, etc.
            pass
    for name in env_vars:
        got = os.environ.get(name)
        if got:
            return got
    return ''


def normalize_iso3(value):
    """Validate and upper-case an ISO 3166-1 alpha-3 code, or raise ValueError.

    The country code reaches `os.path.join(PROJECT_DIR, '{}_data')`, so it becomes
    part of a filesystem path. Typed by hand at a prompt that is harmless. Reached
    by an agent that resolves a country from a chat message, or worse from text it
    extracted out of a downloaded report, it is untrusted input on a path, and
    `../..` would write outside the project. Three letters, nothing else.
    """
    got = str(value or '').strip().upper()
    if len(got) != 3 or not got.isalpha() or not got.isascii():
        raise ValueError(
            'Not an ISO3 country code: {!r}. Expected three ASCII letters, '
            'for example SDN, LBN or UKR.'.format(value))
    return got


def require_module(name, feature, install=None):
    """Import an optional dependency, or fail with an instruction instead of a trace.

    The core is standard library only. A handful of features are not: reading
    workbooks (openpyxl), reading PDFs (pymupdf), drawing charts (matplotlib).
    Those imports stay inside the functions that need them, so the toolkit still
    imports and runs without them. What was missing is a usable message when one
    is absent.
    """
    try:
        return __import__(name)
    except ImportError:
        raise ImportError(
            '{} needs the optional package "{}", which is not installed.\n'
            '    pip install {}\n'
            'The rest of the toolkit does not need it: the core is standard '
            'library only, and only this feature is unavailable.'.format(
                feature, name, install or name))

# Columns legitimately empty for some countries/periods: do not warn on these.
# Keep this list SHORT and justified — it is the escape hatch, not a dumping ground.
EXPECTED_SPARSE = {
    'admin2_name',       # many sources stop at admin1
    'admin3',            # ACLED: often blank
    'actor2',            # ACLED: one-sided events have no second actor
    'civilian_targeting',  # ACLED: only set for a subset of events
    'tags',              # ACLED: sparse by design
    'picture',           # Liveuamap: most events have none
    'date_end',          # open-ended reference periods
    'note', 'notes',
}


def audit_columns(records, filepath, expected_sparse=None):
    """Flag columns that are 100% empty, and phantom columns nothing fills.

    This is THE cheap invariant behind the 2026-07-25 review: every silent-data
    bug found that day (world totals with empty country columns, GDACS severity
    read from a non-existent key, HAPI returnees with 3 empty geo columns,
    a dead `iso3` field in 4862 ACLED rows) manifests first as a column that is
    empty in 100% of rows. Warn, never raise: a false positive must not stop a
    country fetch.

    Returns the list of suspect column names (also useful for the manifest).
    """
    if not records:
        return []
    sparse_ok = EXPECTED_SPARSE if expected_sparse is None else expected_sparse
    keys = list(records[0].keys())
    suspects = []
    for k in keys:
        if k in sparse_ok:
            continue
        if all(str(r.get(k, '')).strip() in ('', 'None') for r in records):
            suspects.append(k)
    if suspects:
        print('  WARNING [{}]: {} column(s) empty in 100% of {} rows: {}'.format(
            os.path.basename(filepath), len(suspects), len(records),
            ', '.join(suspects)))
        print('    -> either the API renamed/removed the field, or the request '
              'was not honoured. Check before using this file.')
    return suspects


def save_csv(records, filepath, fieldnames=None, expected_sparse=None):
    """Save list of dicts (or single dict) to CSV. Shared by all clients.

    `fieldnames` may be passed to freeze the header (the orchestrator does this
    to keep published CSV schemas stable); extra keys in the records are then
    IGNORED rather than raising, so clients can be extended additively.
    Runs `audit_columns` before writing.
    """
    if not records:
        return []
    if isinstance(records, dict):
        records = [records]
    parent = os.path.dirname(os.path.abspath(filepath))
    if parent:
        os.makedirs(parent, exist_ok=True)
    suspects = audit_columns(records, filepath, expected_sparse)
    header = list(fieldnames) if fieldnames else list(records[0].keys())
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        # extrasaction='ignore': adding a key to a client return must never
        # raise ValueError against a frozen header (review 2026-07-25).
        writer = csv.DictWriter(f, fieldnames=header, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(records)
    print('  Saved {} rows -> {}'.format(len(records), os.path.basename(filepath)))
    return suspects
