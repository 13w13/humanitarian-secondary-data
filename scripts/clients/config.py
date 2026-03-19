"""
Secondary Data Sources — Configuration
=======================================
API endpoints, appnames, and constants.
NO secrets here — credentials in keyring or env vars.
"""

# ─── ReliefWeb ──────────────────────────────────────────────
RELIEFWEB_BASE = 'https://api.reliefweb.int/v1'
RELIEFWEB_APPNAME = 'humanitarian-secondary-data'  # Any string — identifies your requests

# ─── HDX HAPI ───────────────────────────────────────────────
HAPI_BASE = 'https://hapi.humdata.org/api/v2'
# app_identifier: base64 of "your-app:your@email.com"
# Generate yours: python -c "import base64; print(base64.b64encode(b'my-app:me@example.com').decode())"
HAPI_APP_ID = 'aHVtYW5pdGFyaWFuLXNlY29uZGFyeS1kYXRhOm5vcmVwbHlAZXhhbXBsZS5jb20='

# ─── HDX CKAN ───────────────────────────────────────────────
HDX_CKAN_BASE = 'https://data.humdata.org/api/3/action'

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


def save_csv(records, filepath):
    """Save list of dicts (or single dict) to CSV. Shared by all clients."""
    if not records:
        return
    if isinstance(records, dict):
        records = [records]
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    fieldnames = list(records[0].keys())
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print('  Saved {} rows -> {}'.format(len(records), os.path.basename(filepath)))
