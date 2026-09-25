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
import http.client
import os
import re
import socket
from urllib.error import HTTPError, URLError


# ─── Failure taxonomy ───────────────────────────────────────
# Four ways a request can come back without data, and none of them is "0 rows".
# Before these existed, eight clients caught every error, printed it and returned
# [], so a blocked network produced "0 plans, $0 funded", "stock 0 IDPs" and
# "0 conflict events" for Sudan, and the run exited 0 (test of 2026-09-24).

class SourceUnavailable(Exception):
    """The provider could not be reached, or answered with a server error.

    An OUTAGE, never an absence of data. Raised instead of returning [] so that no
    caller can turn a failed request into a count. The pipeline records it as
    `unavailable`, the test suites as SKIP.
    """


class MissingCredential(ValueError):
    """A free key this source requires is not configured: a skip, not a zero.

    Subclasses ValueError so existing `except ValueError` handlers still catch it
    (ACLED raised a bare ValueError for this case before the class existed).
    """


class NotCovered(ValueError):
    """The source does not cover this country. Absence of coverage, said out loud.

    Distinct from an empty answer: the question was never asked, because this
    source has no id, region or facet for the country.
    """


RELIEFWEB_APPNAME_HELP = (
    'ReliefWeb refused the appname (HTTP 403): since 1 Nov 2025 it must be '
    'pre-approved. Request one at https://apidoc.reliefweb.int/parameters#appname, then set '
    'RELIEFWEB_APPNAME (or keyring sds.reliefweb/appname).')


def is_outage(exc):
    """True when `exc` says the provider is down rather than that we have a bug.

    Outages: HTTP 5xx and 429, transport failures (timeout, refused or reset
    connection, DNS, a proxy refusing the tunnel, a truncated response) and
    SourceUnavailable itself. Not outages: other 4xx (a wrong URL or parameter
    is our bug), KeyError / ValueError (the response changed shape). Keeping the
    two apart is what lets a red test mean "fix the code" rather than "a provider
    had a bad afternoon".
    """
    if isinstance(exc, SourceUnavailable):
        return True
    if isinstance(exc, HTTPError):             # before URLError: it subclasses it
        return exc.code >= 500 or exc.code == 429
    return isinstance(exc, (URLError, socket.timeout, TimeoutError,
                            ConnectionError, http.client.HTTPException))


MAX_DOWNLOAD_BYTES = 512 * 1024 * 1024


def safe_opener():
    """A urllib opener for FILE downloads that follows redirects to http(s) only.

    The scheme check on the first URL was not enough: urllib follows a 302 to
    ftp:// by default, and to another host with the same headers. Downloads use
    this opener; the API clients still use plain urlopen (see the review of
    2026-09: one shared HTTP layer is the lasting fix).
    """
    from urllib.parse import urlsplit
    from urllib.request import HTTPRedirectHandler, build_opener

    class _HttpOnlyRedirect(HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            if urlsplit(newurl).scheme.lower() not in ('http', 'https'):
                raise HTTPError(newurl, code, 'refused redirect to a non-http(s) URL',
                                headers, fp)
            new = super().redirect_request(req, fp, code, msg, headers, newurl)
            if new is not None and urlsplit(newurl).netloc != urlsplit(req.full_url).netloc:
                for h in ('Authorization', 'Cookie'):    # never forward credentials
                    new.remove_header(h)
            return new

    return build_opener(_HttpOnlyRedirect)


def sniff_mismatch(first_bytes, path):
    """Why the first bytes cannot be the file `path` claims to be, or None.

    An HTML login or error page answered with HTTP 200 used to be saved under a
    .xlsx name; a truncated zip still carries the right extension.
    """
    head = first_bytes[:512].lstrip().lower()
    ext = os.path.splitext(path)[1].lower()
    if ext not in ('.html', '.htm') and (head.startswith(b'<!doctype html')
                                        or head.startswith(b'<html')):
        return 'an HTML page came back instead of a {} file'.format(ext or 'data')
    if ext in ('.xlsx', '.xlsm', '.zip', '.docx') and not first_bytes.startswith(b'PK'):
        return 'not a zip container, so not a valid {} file'.format(ext)
    if ext == '.pdf' and not first_bytes.startswith(b'%PDF'):
        return 'not a PDF'
    return None
_FILENAME_BAD = re.compile(r'[^A-Za-z0-9._ ()-]+')


def safe_filename(name, default='resource'):
    """A publisher-supplied name reduced to one harmless path component.

    Resource names come from remote metadata: "IDPs 2023/2024" or "../../x" must
    neither create directories nor climb out of the output folder.
    """
    # Separators become '_' rather than cutting the name: "IDPs 2023/2024" keeps
    # both years, and "../../x" can no longer climb out of the folder.
    base = str(name or '').replace('\\', '_').replace('/', '_')
    base = _FILENAME_BAD.sub('_', base).strip(' ._')
    return base[:150] or default


def download_stream(url, dest_path, headers=None, timeout=60,
                    max_bytes=MAX_DOWNLOAD_BYTES):
    """Stream `url` to `dest_path` safely; return the number of bytes written.

    http/https only (a `file://` URL in remote metadata must not be read), a size
    cap, a `.part` file moved into place with os.replace once complete, and the
    partial file deleted on any failure: a truncated workbook still opens and still
    sums. Raises on failure (SourceUnavailable for an outage).
    """
    import zipfile
    from urllib.request import Request
    if not str(url).lower().startswith(('http://', 'https://')):
        raise ValueError('refused scheme (only http/https): {}'.format(str(url)[:80]))
    tmp_path = dest_path + '.part'
    try:
        resp = safe_opener().open(
            Request(url, headers=headers or {'User-Agent': USER_AGENT}), timeout=timeout)
        declared = resp.headers.get('Content-Length')
        if declared and declared.isdigit() and int(declared) > max_bytes:
            raise ValueError('declared size {:,} B exceeds cap {:,} B'.format(
                int(declared), max_bytes))
        written, first = 0, b''
        with open(tmp_path, 'wb') as f:
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                if not first:
                    first = chunk[:512]
                written += len(chunk)
                if written > max_bytes:
                    raise ValueError('exceeded cap {:,} B while streaming'.format(max_bytes))
                f.write(chunk)
        if written == 0:
            raise ValueError('empty response body')
        # A dropped connection ends the stream early without an error.
        if declared and declared.isdigit() and written != int(declared):
            raise ValueError('truncated: {:,} of {:,} declared bytes'.format(
                written, int(declared)))
        why = sniff_mismatch(first, dest_path)
        if why:
            raise ValueError(why)
        if dest_path.lower().endswith(('.xlsx', '.xlsm', '.zip')) \
                and not zipfile.is_zipfile(tmp_path):
            raise ValueError('incomplete zip container (truncated download?)')
        os.replace(tmp_path, dest_path)
        return written
    except Exception as e:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        raise_unavailable('download {}'.format(str(url)[:80]), e)


def raise_unavailable(label, exc):
    """Re-raise `exc` as SourceUnavailable if it is an outage, unchanged otherwise.

    Call from inside an `except` block:

        except Exception as e:
            raise_unavailable('HPC plans', e)
    """
    if is_outage(exc):
        raise SourceUnavailable('{}: {}'.format(label, exc)) from exc
    raise exc


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
        got = keyring.get_password(service, field)
        if got:
            return got
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:  # noqa: BLE001 - see below
        # Not installed, no usable backend, locked keychain, D-Bus absent... and
        # worse: a broken system backend can PANIC while loading (pyo3's
        # PanicException from `cryptography`, observed 2026-09-25), which is a
        # BaseException, not an Exception. It crashed the whole toolkit although
        # keyring is optional. Any keychain failure means "use the environment".
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
