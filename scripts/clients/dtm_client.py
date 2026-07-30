"""
IOM DTM Client
===============
Query IOM Displacement Tracking Matrix data from 3 sources:
1. HDX CKAN — dataset discovery (existing)
2. DTM API v3 (dtmapi.iom.int) — aggregated IDP figures (Admin 0/1/2)
3. DTM Datasets Portal (dtm.iom.int/datasets) — MSNA + other datasets (scraping)

Usage:
    from dtm_client import DTMClient
    dtm = DTMClient()

    # HDX search (existing)
    datasets = dtm.search_dtm_datasets('lebanon')

    # DTM API v3 — IDP figures (needs subscription key)
    idps = dtm.get_idp_data('LBN')
    idps_admin1 = dtm.get_idp_data('LBN', admin_level=1)

    # Portal scraping — MSNA datasets
    msna = dtm.search_portal_datasets('msna', country_id=65)  # Haiti
    msna = dtm.search_portal_msna('HTI')

Note on DTM API: only IDP figures, NOT MSNA microdata. Auth: Ocp-Apim-Subscription-Key.
Note on Portal: Drupal, scrapable by URL, download direct, no auth for public datasets.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
import os
import re
import time
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote

from html import unescape as _unescape
from config import (
    HDX_CKAN_BASE, DTM_API_BASE, DEFAULT_TIMEOUT, RATE_LIMIT_DELAY,
    USER_AGENT, save_csv
)


def _strip_tags(s):
    """HTML fragment → collapsed plain text."""
    return re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', '', _unescape(s or ''))).strip()

# DTM API v3
# DTM API v3 - contract confirmed by the official OpenAPI spec, archived at
# `references/IOM-DTMServiceAPI-V3.openapi.json` (title "IOM-DTM.Services.API V3").
#
# WARNING: the portal HTML documentation (dtm-apim-portal.iom.int/documentation)
# advertises `/api/idpAdmin0Data/GetAdmin0Datav2` and `/api/Common/GetAllCountryList`.
# Those return `{"statusCode":404,"message":"Resource not found"}` with AND without
# a valid key (an APIM gateway answers 401, not 404, on a route that exists).
# The spec lists only the `/v3/displacement/*` family below - probed live 2026-07-25
# (HTTP 200, isSuccess=true) BEFORE the spec was obtained, and the spec then
# confirmed it. Behaviour beats prose documentation: do not "fix" these paths.
DTM_API_V3_BASE = 'https://dtmapi.iom.int/v3'
# Second server declared by the spec: used as failover on connection errors.
DTM_API_V3_FALLBACK = 'https://dtm-apim.iom.int/v3'
# The spec also allows the key as a `subscription-key` QUERY parameter. We do NOT
# use it: a key in a URL leaks into logs, proxies and shell history. Header only.

# ── DTM Datasets catalogue (dtm.iom.int/datasets) ────────────
# THE channel for actual DTM data FILES. Measured 2026-07-25 by sweeping all
# 124 pages: 2,375 datasets, 2,250 with a direct download link (95%), 125 gated.
# 93 of those 125 are Lebanon (0 open) — gating is a per-mission decision, not a
# DTM-wide policy. The gated remainder is household-survey microdata (Ukraine
# Mobility & Needs Assessment, Cameroon MSNA, Ukraine Registered IDPs); aggregate
# mobility tracking is open.
DTM_PORTAL_BASE = 'https://dtm.iom.int'

# Header profile. dtm.iom.int answers 403 to a *half-spoofed* browser: a Chrome
# User-Agent with no matching Accept/Accept-Language/Sec-Fetch-* headers. Measured
# on the same URL, same minute (2026-07-25):
#   urllib default UA ................ 200
#   'humanitarian-secondary-data/1.0'  403   <- config.USER_AGENT
#   Chrome UA alone .................. 403
#   Chrome UA + full browser headers . 200
# The old _portal_get sent a truncated Chrome UA + minimal Accept, i.e. exactly the
# failing profile — which is why the HTML surfaces were wrongly recorded as
# "WAF-blocked, needs Playwright". They are not: stdlib reaches them.
PORTAL_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'),
    'Accept': ('text/html,application/xhtml+xml,application/xml;q=0.9,'
               'image/avif,image/webp,*/*;q=0.8'),
    'Accept-Language': 'en-US,en;q=0.9',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Upgrade-Insecure-Requests': '1',
}

# iso3 → catalogue identity. Built empirically 2026-07-25 by matching the 56
# (label, facet id) pairs exposed in the catalogue's own facet block against the
# 56 country slugs observed across 2,375 rows: 56/56 matched, no orphan either way.
# `slug` is FROZEN because it is what each row carries and it is NOT derivable by
# slugifying the label — Democratic Republic of the Congo → 'democratic-republic-congo'
# (verified live). Asserting a slugified label would false-alarm on DRC's 157 datasets.
# `facet_id` is a FALLBACK only: ids are resolved live from the facet block, which
# self-updates. Countries absent here have no DTM catalogue facet at all.
DTM_CATALOGUE_COUNTRIES = {
    'AFG': ('Afghanistan', 'afghanistan', 56),
    'ARM': ('Armenia', 'armenia', 10177),
    'ATG': ('Antigua and Barbuda', 'antigua-and-barbuda', 1936),
    'BDI': ('Burundi', 'burundi', 57),
    'BEN': ('Benin', 'benin', 6136),
    'BFA': ('Burkina Faso', 'burkina-faso', 1083),
    'BGD': ('Bangladesh', 'bangladesh', 97),
    'BHS': ('Bahamas', 'bahamas', 6718),
    'BOL': ('Bolivia', 'bolivia', 267),
    'CAF': ('Central African Republic', 'central-african-republic', 59),
    'CMR': ('Cameroon', 'cameroon', 58),
    'COD': ('Democratic Republic of the Congo', 'democratic-republic-congo', 62),
    'DJI': ('Djibouti', 'djibouti', 1938),
    'DMA': ('Dominica', 'dominica', 1982),
    'ECU': ('Ecuador', 'ecuador', 148),
    'ETH': ('Ethiopia', 'ethiopia', 63),
    'FJI': ('Fiji', 'fiji', 109),
    'GTM': ('Guatemala', 'guatemala', 709),
    'HND': ('Honduras', 'honduras', 1085),
    'HTI': ('Haiti', 'haiti', 65),
    'IDN': ('Indonesia', 'indonesia', 66),
    'IRQ': ('Iraq', 'iraq', 67),
    'KAZ': ('Kazakhstan', 'kazakhstan', 10683),
    'KEN': ('Kenya', 'kenya', 691),
    'KGZ': ('Kyrgyzstan', 'kyrgyzstan', 10684),
    'LBN': ('Lebanon', 'lebanon', 9442),
    'LBY': ('Libya', 'libya', 69),
    'LKA': ('Sri Lanka', 'sri-lanka', 130),
    'MDG': ('Madagascar', 'madagascar', 793),
    'MLI': ('Mali', 'mali', 71),
    'MNG': ('Mongolia', 'mongolia', 815),
    'MOZ': ('Mozambique', 'mozambique', 72),
    'MRT': ('Mauritania', 'mauritania', 113),
    'MWI': ('Malawi', 'malawi', 118),
    'NER': ('Niger', 'niger', 75),
    'NGA': ('Nigeria', 'nigeria', 76),
    'NPL': ('Nepal', 'nepal', 74),
    'PAK': ('Pakistan', 'pakistan', 77),
    'PER': ('Peru', 'peru', 1087),
    'PHL': ('Philippines', 'philippines', 978),
    'PNG': ('Papua New Guinea', 'papua-new-guinea', 78),
    'SDN': ('Sudan', 'sudan', 82),
    'SLV': ('El Salvador', 'el-salvador', 706),
    'SOM': ('Somalia', 'somalia', 80),
    'SSD': ('South Sudan', 'south-sudan', 81),
    'SYR': ('Syrian Arab Republic', 'syrian-arab-republic', 83),
    'TCD': ('Chad', 'chad', 60),
    'THA': ('Thailand', 'thailand', 816),
    'TJK': ('Tajikistan', 'tajikistan', 10687),
    'UGA': ('Uganda', 'uganda', 4484),
    'UKR': ('Ukraine', 'ukraine', 1088),
    'VUT': ('Vanuatu', 'vanuatu', 135),
    'YEM': ('Yemen', 'yemen', 85),
    'ZAF': ('South Africa', 'south-africa', 14368),
    'ZMB': ('Zambia', 'zambia', 10688),
    'ZWE': ('Zimbabwe', 'zimbabwe', 86),
}

# Legacy alias kept so existing callers keep working.
PORTAL_COUNTRY_IDS = {k: v[2] for k, v in DTM_CATALOGUE_COUNTRIES.items()}


class NoApiAccess(Exception):
    """No DTM subscription key configured: "no access", not "no data"."""


class WrongCountry(Exception):
    """The DTM response describes a country other than the one requested."""


class UnmappedCountry(Exception):
    """No DTM catalogue facet for this country: refuse rather than search blind.

    The catalogue's `search=` parameter is full text, NOT a country scope
    (`search=lebanon` returned 11 Syria rows out of 20 on 2026-07-25). Falling
    back to it would answer a country question with another country's data.
    """


class DatasetGated(Exception):
    """The dataset exists but its file is behind DTM's "Request access" gate."""


class DTMClient:
    """Client for IOM DTM data via HDX CKAN + DTM API v3 + Portal scraping.

    Three access paths, authoritative for different questions:
      - **API v3** (needs a key): the ONLY path to actual IDP FIGURES,
        admin 0/1/2, with sex / displacement reason / origin / round.
      - **HDX CKAN**: dataset FILES. Must be scoped `fq=groups:{iso3}`;
        free-text search is not a country scope (a Lebanon text search returned
        30 Ethiopian rows out of 35 on 2026-07-24).
      - **Portal scraping**: last resort for datasets absent from HDX.
    """

    def __init__(self, api_key=None):
        self.hdx_base = HDX_CKAN_BASE
        self.dtm_base = DTM_API_BASE
        self.api_v3_base = DTM_API_V3_BASE
        self.portal_base = DTM_PORTAL_BASE
        # DTM API v3 subscription key (Azure APIM).
        # Register free at https://dtm-apim-portal.iom.int/ then generate a key.
        # Header name VERIFIED from the API's own 401 (2026-07-25):
        #   WWW-Authenticate: AzureApiManagementKey realm="https://dtm-apim.iom.int/",
        #                     name="Ocp-Apim-Subscription-Key"
        # Resolution order: explicit arg > keyring sds.dtm > env var.
        self.api_key = api_key or self._resolve_key()

    @staticmethod
    def _resolve_key():
        """Subscription key from keyring sds.dtm/subscription_key, else env."""
        try:
            import keyring
            k = keyring.get_password('sds.dtm', 'subscription_key')
            if k:
                return k
        except Exception:
            pass
        return (os.environ.get('DTM_SUBSCRIPTION_KEY')
                or os.environ.get('DTMAPI_SUBSCRIPTION_KEY', ''))

    def has_api_access(self):
        """True when a subscription key is configured.

        Use this to say "no access" instead of "no data": before 2026-07-25 the
        IDP methods returned [] with no key AND had no caller, so the only path
        to actual DTM figures was silently dead.
        """
        return bool(self.api_key)

    # ── HDX CKAN (existing) ──────────────────────────────────

    def _hdx_get(self, action, params=''):
        """GET request to HDX CKAN API."""
        url = '{}/{}?{}'.format(self.hdx_base, action, params)
        req = Request(url, headers={'User-Agent': USER_AGENT})
        resp = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
        if not resp.get('success'):
            raise RuntimeError('CKAN API error: {}'.format(resp.get('error', 'unknown')))
        return resp['result']

    def search_dtm_datasets(self, country_name, rows=20, iso3=None):
        """Datasets DTM sur HDX, scopes par PAYS et non par recherche plein texte.

        ⚠ Une recherche plein texte n'est PAS un filtre pays. Mesure le 2026-07-24 :
        une requete "DTM Lebanon" rendait **30 lignes ethiopiennes sur 35**. Le scope
        pays de CKAN est la facette de groupe : `fq=groups:{iso3_minuscule}`.

        Args:
            iso3: code ISO3. **Fortement recommande** : sans lui on retombe sur le
                  plein texte, et le resultat est un corpus multi-pays.
        """
        # ⚠ CKAN attend UN SEUL parametre `fq` : deux `fq=` separes font echouer la
        # requete. Les filtres se combinent dans la meme expression, separes par un
        # espace (encode `+`), ce qui vaut un ET.
        fq = ['organization:international-organization-for-migration']
        parts = ['rows={}'.format(rows)]
        if iso3:
            fq.append('groups:{}'.format(iso3.lower()))
        else:
            print('  DTM/HDX: appel SANS iso3 -> recherche plein texte, resultat '
                  'multi-pays possible (30/35 lignes ETH pour une requete LBN en 2026-07)')
            parts.append('q=DTM+{}'.format(country_name.replace(' ', '+')))
        parts.insert(0, 'fq={}'.format('+'.join(fq)))
        params = '&'.join(parts)
        result = self._hdx_get('package_search', params)
        datasets = []
        for pkg in result.get('results', []):
            resources = [{
                'id': res.get('id', ''), 'name': res.get('name', ''),
                'format': res.get('format', ''), 'url': res.get('url', ''),
                'size': res.get('size', 0),
            } for res in pkg.get('resources', [])]
            org = pkg.get('organization', {})
            # Colonne pays EXPLICITE : les groupes CKAN portent les codes pays. Sans
            # elle, un fichier "datasets DTM du Liban" ne permettait pas de voir qu'il
            # contenait de l'Ethiopie.
            groups = [str(g.get('name', '')).upper()
                      for g in (pkg.get('groups') or []) if g.get('name')]
            datasets.append({
                'name': pkg.get('name', ''),
                'title': pkg.get('title', ''),
                'org': org.get('title', '') if org else '',
                'country_groups': ';'.join(groups),
                'requested_iso3': (iso3 or '').upper(),
                # `metadata_modified` = MAJ des metadonnees HDX, pas la periode couverte
                'metadata_modified': str(pkg.get('metadata_modified', ''))[:10],
                'data_period': str(pkg.get('dataset_date', '') or '')[:40],
                'num_resources': len(resources),
                'resources': resources,
                'url': 'https://data.humdata.org/dataset/{}'.format(pkg.get('name', '')),
            })
        # Post-condition : quand un iso3 est demande, chaque ligne doit le porter.
        if iso3:
            want = iso3.upper()
            off = [d['name'] for d in datasets
                   if d['country_groups'] and want not in d['country_groups'].split(';')]
            if off:
                raise ValueError(
                    'HDX a renvoye {} dataset(s) hors {} : {} -> la facette groups n\'a '
                    'pas porte.'.format(len(off), want, off[:3]))
        return datasets

    # ── DTM API v3 — IDP figures ─────────────────────────────

    def _api_v3_get(self, endpoint, params=None):
        """GET request to DTM API v3 (requires subscription key)."""
        if not self.api_key:
            try:
                import keyring
                self.api_key = keyring.get_password('sds.dtm', 'subscription_key') or ''
            except Exception:
                pass
        if not self.api_key:
            raise ValueError(
                'No DTM API key. Set DTMAPI_SUBSCRIPTION_KEY env var '
                'or register at https://dtm-apim-portal.iom.int/')

        headers = {
            'User-Agent': USER_AGENT,
            'Ocp-Apim-Subscription-Key': self.api_key,
            'Accept': 'application/json',
        }
        qs = '?{}'.format(urlencode(params)) if params else ''
        last_err = None
        # The spec declares two servers; try the primary, then the fallback.
        for base in (self.api_v3_base, DTM_API_V3_FALLBACK):
            try:
                req = Request('{}/{}{}'.format(base, endpoint, qs), headers=headers)
                data = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
                break
            except Exception as e:  # noqa: BLE001 - failover, re-raised below
                last_err = e
                data = None
        if data is None:
            raise last_err

        # Envelope: {result, statusCode, isSuccess, errorMessages, totalRecordsCount}
        if isinstance(data, dict):
            if not data.get('isSuccess', False):
                # Surface the API's own validation message instead of returning []
                # ("No Country found matching your query", date-format errors...).
                raise RuntimeError('DTM API rejected the query: {}'.format(
                    data.get('errorMessages') or data.get('message') or data))
            result = data.get('result') or []
            declared = data.get('totalRecordsCount')
            if isinstance(declared, int) and declared != len(result):
                print('  DTM WARNING: API declares {} records but returned {} '
                      '-> possible truncation'.format(declared, len(result)))
            return result
        return data if isinstance(data, list) else []

    # Endpoint paths VERIFIED LIVE 2026-07-25 (200 + isSuccess=true), NOT the
    # ones the portal documentation advertises (those 404 - see the base-URL note).
    IDP_ENDPOINTS = {
        0: 'displacement/admin0',
        1: 'displacement/admin1',
        2: 'displacement/admin2',
    }
    COUNTRY_LIST_ENDPOINT = 'displacement/country-list'
    OPERATION_LIST_ENDPOINT = 'displacement/operation-list'

    def get_idp_data(self, iso3=None, country_name=None, admin_level=0,
                     date_from=None, date_to=None, round_from=None, round_to=None,
                     operation=None):
        """Get IDP figures from the DTM API (GET methods, Aug 2024 revision).

        Contract from the official documentation:
          - base `https://dtmapi.iom.int/api/`, header `Ocp-Apim-Subscription-Key`
          - **`Admin0Pcode` takes the ISO3 directly** -> we filter on ISO3, never
            on a country name. This matters: name-based filtering is exactly what
            produced "Fiji served as Sudan" (WFP) and `'Phl'` returning zero
            (GDACS) elsewhere in this repo.
          - at least one of Operation / CountryName / Admin0Pcode is REQUIRED
            (the API answers "Please provide either the Country Name, Admin0
            Pcode or Operation" otherwise)
          - dates are yyyy-mm-dd; FromReportingDate must be <= ToReportingDate;
            FromRoundNumber must be <= ToRoundNumber

        Scope: NON-SENSITIVE IDP figures only, aggregated at admin 0/1/2, from
        two DTM components (baseline assessments and multi-sectoral location
        assessments). Rounds are snapshots at irregular frequency (yearly to
        more often, per context), so a "latest" figure is a round, not a date.
        **No disability disaggregation exists in this API** (verified against the
        documentation and the official `dtmapi` package): for disability, use
        HAPI humanitarian-needs, the World Bank Disability Data Hub, or MSNA
        WG-SS microdata.

        Raises NoApiAccess without a key, so a caller reports "no access" rather
        than publishing an empty result as "no data".
        """
        if not self.api_key:
            raise NoApiAccess(
                'DTM API needs a subscription key (header '
                'Ocp-Apim-Subscription-Key). Register free at '
                'https://dtm-apim-portal.iom.int/ then store it: '
                "keyring.set_password('sds.dtm','subscription_key', '<key>').")

        if admin_level not in self.IDP_ENDPOINTS:
            raise ValueError('admin_level must be 0, 1 or 2')
        if not (iso3 or country_name or operation):
            raise ValueError('DTM requires at least one of iso3 (Admin0Pcode), '
                             'country_name or operation.')

        endpoint = self.IDP_ENDPOINTS[admin_level]
        params = {}
        if iso3:
            params['Admin0Pcode'] = iso3.upper()
        if country_name:
            params['CountryName'] = country_name
        if operation:
            params['Operation'] = operation
        if date_from:
            params['FromReportingDate'] = date_from
        if date_to:
            params['ToReportingDate'] = date_to
        if round_from:
            params['FromRoundNumber'] = int(round_from)
        if round_to:
            params['ToRoundNumber'] = int(round_to)

        records = self._api_v3_get(endpoint, params)

        # Field names VERIFIED LIVE 2026-07-25 on /v3/displacement/admin0 (LBN).
        # DEDUP KEY at admin0: (reportingDate, roundNumber, operation,
        # assessmentType, displacementReason, idpOriginAdmin1Pcode). Rows are
        # split by displacement reason AND origin admin1, so a naive sum over the
        # whole response mixes rounds and double counts. Filter one round first.
        out = []
        for r in records:
            out.append({
                # The API echoes admin0Pcode, so the country check is real
                # (never copy the request into the output).
                'iso3': r.get('admin0Pcode', ''),
                'admin0_name': r.get('admin0Name', ''),
                'admin1_name': r.get('admin1Name', ''),
                'admin1_pcode': r.get('admin1Pcode', ''),
                'admin2_name': r.get('admin2Name', ''),
                'admin2_pcode': r.get('admin2Pcode', ''),
                'admin_level': admin_level,
                'num_idps': r.get('numPresentIdpInd', ''),
                'num_males': r.get('numberMales', ''),
                'num_females': r.get('numberFemales', ''),
                'displacement_reason': r.get('displacementReason', ''),
                'origin_admin1_name': r.get('idpOriginAdmin1Name', ''),
                'origin_admin1_pcode': r.get('idpOriginAdmin1Pcode', ''),
                'round': r.get('roundNumber', ''),
                'operation': r.get('operation', ''),
                # BA = baseline assessment, MSLA = multi-sectoral location
                # assessment. Different methodologies: never mix them in a total.
                'assessment_type': r.get('assessmentType', ''),
                'reporting_date': str(r.get('reportingDate', ''))[:10],
                'year': r.get('yearReportingDate', ''),
                'month': r.get('monthReportingDate', ''),
            })

        # Post-condition: the response must describe what we asked for.
        # The API echoes admin0Pcode, so this check is real (not a copy of the
        # request) - the failure mode that made IDMC and GDACS uncheckable.
        if out and iso3:
            got = {str(r['iso3']).upper() for r in out if r['iso3']}
            if got and iso3.upper() not in got:
                raise WrongCountry(
                    'DTM: asked for Admin0Pcode={} but the response describes {} '
                    '-> filter not honoured.'.format(iso3.upper(), sorted(got)[:5]))

        # STALENESS GUARD. The API's coverage is NOT the coverage of the DTM
        # website: verified 2026-07-25, Sudan's latest round here is 70, dated
        # 2018-08-30, operation "Darfur conflict", 2,042,896 IDPs - while IOM DTM
        # publishes ~11.6M for the current Sudan crisis elsewhere. Quoting the API
        # figure as current would be wrong by roughly a factor of five. Warn loudly
        # so a caller cannot mistake a legacy operation for today's situation.
        if out:
            newest = max((r['reporting_date'] for r in out if r['reporting_date']),
                         default='')
            if newest:
                try:
                    age_days = (datetime.now()
                                - datetime.strptime(newest, '%Y-%m-%d')).days
                except ValueError:
                    age_days = 0
                if age_days > 550:  # ~18 months
                    print('  DTM WARNING: newest round for {} is {} ({:.0f} months '
                          'old), operations={}. The DTM API may only expose a LEGACY '
                          'operation for this country - do NOT present this as the '
                          'current situation. Cross-check IDMC/HAPI or the DTM '
                          'website.'.format(
                              iso3 or country_name, newest, age_days / 30.4,
                              sorted({r['operation'] for r in out})[:3]))
        return out

    def get_availability(self, iso3=None, admin_level=0):
        """What DTM actually HAS for a country: operations, coverage, freshness.

        Settles "is there usable DTM data here, how old is it, and which
        operation should I read?" BEFORE any figure is quoted.

        **THE TRAP THIS EXISTS TO PREVENT (verified on Sudan, 2026-07-25):
        roundNumber is scoped PER OPERATION, not per country.** Sudan carries 4
        operations; `max(roundNumber)` = 70 belongs to the legacy "Darfur
        conflict" operation and resolves to 2018-08-30 / 2,042,896 IDPs, while
        the live "Armed Clashes in Sudan (Overview)" operation is at round 36,
        2026-05-31 / 8,805,506 IDPs. Selecting by round instead of by date gives
        a figure 4.3x too low and 8 years stale. **Always select the latest
        snapshot by reporting_date, within one operation.**

        Returns per-operation detail plus a `latest` block for the operation
        holding the most recent reporting date.
        """
        rows = self.get_idp_data(iso3=iso3, admin_level=admin_level)
        if not rows:
            return {'iso3': iso3, 'has_data': False, 'operations': [],
                    'latest': None, 'stale': None}

        ops = {}
        for r in rows:
            op = r['operation'] or '(unnamed)'
            ops.setdefault(op, []).append(r)

        op_summaries = []
        for op, rs in ops.items():
            dates = [x['reporting_date'] for x in rs if x['reporting_date']]
            rounds = [x['round'] for x in rs if x['round'] is not None]
            newest_op = max(dates) if dates else ''
            snap = [x for x in rs if x['reporting_date'] == newest_op]
            op_summaries.append({
                'operation': op,
                'rows': len(rs),
                'round_min': min(rounds) if rounds else None,
                'round_max': max(rounds) if rounds else None,
                'date_from': min(dates) if dates else None,
                'date_to': newest_op or None,
                'assessment_types': sorted({x['assessment_type'] for x in rs}),
                # Sum of ONE snapshot of ONE operation: the only legitimate total.
                'idps_latest_snapshot': sum(int(x['num_idps'] or 0) for x in snap),
                'rows_in_snapshot': len(snap),
            })
        # Cadence-relative staleness. An ABSOLUTE threshold is wrong: DTM rounds
        # run "yearly, quarterly, or more often depending on context" (their own
        # docs). Lebanon publishes ~weekly, so 9 months late is dead; a yearly
        # product 9 months old is current. Verified failure of the absolute rule
        # 2026-07-25: an 18-month threshold rated Lebanon "fresh" at 8.8 months
        # while the DTM website was 21 rounds ahead of the API.
        for s in op_summaries:
            rs = ops[s['operation']]
            dates = sorted({x['reporting_date'] for x in rs if x['reporting_date']})
            gaps = []
            for a, b in zip(dates, dates[1:]):
                gaps.append((datetime.strptime(b, '%Y-%m-%d')
                             - datetime.strptime(a, '%Y-%m-%d')).days)
            gaps = [g for g in gaps if g > 0]
            cadence = sorted(gaps)[len(gaps) // 2] if gaps else None
            age_op = ((datetime.now()
                       - datetime.strptime(s['date_to'], '%Y-%m-%d')).days
                      if s['date_to'] else None)
            s['cadence_days'] = cadence
            s['age_days'] = age_op
            # Late by more than 3 publication cycles (floor 45 days to avoid
            # flagging a daily feed that paused for a week).
            s['stale'] = bool(cadence and age_op is not None
                              and age_op > max(3 * cadence, 45))
            s['late_by_cycles'] = (round(age_op / cadence, 1)
                                   if cadence and age_op is not None else None)

        op_summaries.sort(key=lambda s: s['date_to'] or '', reverse=True)
        latest = op_summaries[0]
        return {
            'iso3': iso3,
            'has_data': True,
            'rows': len(rows),
            'n_operations': len(op_summaries),
            'operations': op_summaries,
            'latest': latest,
            'age_months': round((latest['age_days'] or 0) / 30.4, 1),
            'stale': latest['stale'],
            'warnings': [
                'roundNumber is per-operation: select the latest figure with '
                'max(reporting_date) INSIDE one operation, never max(round).',
                'operationStatus="Active" in operation-list does NOT mean the API '
                'data is current (Lebanon is Active while 9 months behind).',
                'The DTM API is NOT the DTM website: products such as the Lebanon '
                '"Mobility Snapshot" (round 109, 2026-07-23) are published on '
                'dtm.iom.int and absent from this API. If a figure looks stale '
                'against the cadence, check the country page before answering.',
            ],
        }

    def get_country_list(self):
        """Get list of countries with DTM data."""
        try:
            return self._api_v3_get('displacement/country-list')
        except Exception:
            return []

    # ── Datasets catalogue (dtm.iom.int/datasets) ────────────
    #
    # This is where DTM data FILES live. The API v3 serves aggregated figures and
    # can lag the site badly (Lebanon: round 88 / Oct 2025 via API vs round 109 /
    # 23 Jul 2026 on the site, while reporting the operation "Active"). ReliefWeb
    # carries the *reports* — on a sample of the 200 most recent IOM reports, 167
    # attachments were 100% application/pdf, 0 spreadsheets, and the exact dataset
    # title "Flow Monitoring Counting" returned 0 hits. So: ReliefWeb for the
    # published FIGURE, this catalogue for the FILE. Neither replaces the other.

    _ROW_SPLIT = re.compile(r'<div class="report-item1">')

    def _portal_get(self, url, retry=True):
        """GET an HTML page from dtm.iom.int.

        Sends a coherent browser header set (see PORTAL_HEADERS): a Chrome UA with
        no matching Accept/Sec-Fetch headers is answered 403. On 403 anyway, retries
        once with bare urllib headers, which was also observed to pass.
        """
        try:
            req = Request(url, headers=PORTAL_HEADERS)
            return urlopen(req, timeout=DEFAULT_TIMEOUT).read().decode('utf-8', 'replace')
        except Exception as e:
            if retry and '403' in str(e):
                time.sleep(2)
                return urlopen(Request(url), timeout=DEFAULT_TIMEOUT).read().decode(
                    'utf-8', 'replace')
            raise

    def get_facet_map(self, refresh=False):
        """Country label → catalogue facet id, read live from the facet block.

        Harvested rather than hardcoded: the block is the site's own authority and
        self-updates (56 countries on 2026-07-25). DTM_CATALOGUE_COUNTRIES supplies
        the fallback id when the page cannot be read.
        """
        if getattr(self, '_facets', None) and not refresh:
            return self._facets
        facets = {}
        try:
            html_txt = self._portal_get('{}/datasets'.format(self.portal_base))
            pairs = re.findall(
                r'dataset_country[:%3A]*(\d+)[^"]*"[^>]*>(?:<[^>]+>)*\s*([^<]{2,50})',
                html_txt)
            for fid, label in pairs:
                label = re.sub(r'\s*\(\d+\)\s*$', '', _unescape(label)).strip()
                if label and label not in facets:
                    facets[label] = int(fid)
        except Exception as e:
            print('  DTM catalogue: facet block unreadable ({}), using frozen ids'
                  .format(str(e)[:60]))
        for _iso, (label, _slug, fid) in DTM_CATALOGUE_COUNTRIES.items():
            facets.setdefault(label, fid)
        self._facets = facets
        return facets

    def _parse_catalogue_rows(self, html_txt):
        """Parse catalogue listing rows.

        Each row already carries the access status: a `div.dtm-file-download` block
        holds the direct download URL for open datasets and is absent for gated ones.
        So open/gated is known WITHOUT opening each dataset page.
        """
        rows = []
        for chunk in self._ROW_SPLIT.split(html_txt)[1:]:
            m = re.search(r'href="(/datasets/[^"]+)"[^>]*>(.*?)</a>', chunk, re.S)
            if not m:
                continue
            dl = re.search(r'class="dtm-file-download".*?href="([^"]+)"', chunk, re.S)
            desc = re.search(r'<div class="content">(.*?)</div>', chunk, re.S)
            dblock = re.search(r'<div class="date">(.*?)</div>', chunk, re.S)
            spans = []
            if dblock:
                spans = [_strip_tags(s) for s in
                         re.findall(r'<span[^>]*>(.*?)</span>', dblock.group(1), re.S)]
                spans = [s for s in spans if s and s != '·']
            country = re.search(
                r'<div class="date">.*?<a href="/((?!regions)[^"/?]+)"', chunk, re.S)
            region = re.search(r'href="/regions/([^"]+)"', chunk)
            rows.append({
                'slug': m.group(1).replace('/datasets/', ''),
                'title': _strip_tags(m.group(2)),
                'url': '{}{}'.format(self.portal_base, m.group(1)),
                'published': spans[0] if spans else '',
                'region': region.group(1) if region else '',
                'country_slug': country.group(1) if country else '',
                'activities': spans[3] if len(spans) > 3 else '',
                'dataset_format': spans[4] if len(spans) > 4 else '',
                'description': _strip_tags(desc.group(1))[:300] if desc else '',
                'download_url': _unescape(dl.group(1)) if dl else '',
                'access': 'open' if dl else 'gated',
            })
        return rows

    def browse_catalogue(self, iso3=None, search=None, max_pages=2,
                         open_only=False, newest_first=True):
        """List DTM datasets, optionally scoped to one country.

        Args:
            iso3: country to scope to, via the server-side facet (the only real
                country filter). Raises UnmappedCountry if DTM has no facet for it.
            search: full-text keywords. NOT a country scope — combine with iso3 or
                accept a cross-country result set.
            max_pages: pages of ~20 rows to walk (0.5s apart).
            open_only: keep only rows with a download link.
            newest_first: sort by published date descending.

        Returns list of row dicts (see _parse_catalogue_rows).

        Post-condition: when iso3 is given, every row's country_slug must equal the
        expected slug (empty is tolerated: 42 of 2,375 rows are regional products
        such as "Europe — Mixed Migration Flows"). A mismatch raises WrongCountry
        rather than returning another country's datasets.
        """
        expected_slug = None
        params = []
        if iso3:
            iso3 = iso3.upper()
            if iso3 not in DTM_CATALOGUE_COUNTRIES:
                raise UnmappedCountry(
                    '{}: no DTM catalogue facet. DTM covers {} countries here; '
                    'refusing to fall back to search= (full text, not a country '
                    'scope).'.format(iso3, len(DTM_CATALOGUE_COUNTRIES)))
            label, expected_slug, frozen_id = DTM_CATALOGUE_COUNTRIES[iso3]
            fid = self.get_facet_map().get(label, frozen_id)
            params.append('f%5B0%5D=dataset_country:{}'.format(fid))
        if search:
            params.append('search={}'.format(quote(search)))
        if newest_first:
            params.append('sort_by=field_dataset_published_date&sort_order=DESC')

        out, seen = [], set()
        for page in range(max(1, max_pages)):
            q = list(params) + (['page={}'.format(page)] if page else [])
            url = '{}/datasets?{}'.format(self.portal_base, '&'.join(q))
            try:
                rows = self._parse_catalogue_rows(self._portal_get(url))
            except Exception as e:
                print('  DTM catalogue: {}'.format(str(e)[:90]))
                break
            if not rows:
                break
            fresh = [r for r in rows if r['slug'] not in seen]
            for r in fresh:
                seen.add(r['slug'])
            out.extend(fresh)
            if len(rows) < 15:      # last page
                break
            time.sleep(RATE_LIMIT_DELAY)

        if expected_slug:
            wrong = sorted({r['country_slug'] for r in out
                            if r['country_slug'] and r['country_slug'] != expected_slug})
            if wrong:
                raise WrongCountry(
                    'catalogue facet for {} returned rows from {}: the country '
                    'filter did not apply'.format(iso3, wrong[:4]))
        if open_only:
            out = [r for r in out if r['access'] == 'open']
        return out

    def catalogue_summary(self, iso3):
        """What DTM data exists for a country, and how much of it is downloadable.

        Answers "is there data, is it open, how fresh" in one call — the honest
        precondition to quoting any DTM figure.
        """
        rows = self.browse_catalogue(iso3, max_pages=3)
        op = [r for r in rows if r['access'] == 'open']
        acts = {}
        for r in rows:
            for a in (r['activities'] or '').split(','):
                a = a.strip()
                if a:
                    acts[a] = acts.get(a, 0) + 1
        return {
            'iso3': iso3.upper(),
            'label': DTM_CATALOGUE_COUNTRIES.get(iso3.upper(), ('?',))[0],
            'datasets_seen': len(rows),
            'open': len(op),
            'gated': len(rows) - len(op),
            'latest': rows[0] if rows else None,
            'latest_open': op[0] if op else None,
            'activities': dict(sorted(acts.items(), key=lambda kv: -kv[1])),
            'pages_walked': 3,
            'note': ('rows walked from the newest page(s) only, not the country total'
                     if len(rows) >= 55 else ''),
        }

    def download_dataset(self, row_or_url, dest_dir, filename=None):
        """Download an open dataset file. Returns the written path.

        `row_or_url` is a row from browse_catalogue (preferred) or a download URL.
        Download URLs embed volatile ids (`dtm_download_track/{file_id}?...&id={node_id}`)
        so they are NOT cached — re-resolve from the catalogue at download time.

        Raises DatasetGated when the row has no download link. Never attempts to
        circumvent the gate: gated microdata is requested by email from the mission
        (contact on the dataset page).
        """
        if isinstance(row_or_url, dict):
            if row_or_url['access'] != 'open':
                raise DatasetGated(
                    '{!r} is behind "Request access" — ask the DTM mission by email; '
                    'meanwhile the published figures are in the latest report.'
                    .format(row_or_url['title'][:70]))
            url = row_or_url['download_url']
            default_name = row_or_url['slug']
        else:
            url, default_name = row_or_url, 'dtm_download'
        if not url:
            raise DatasetGated('no download URL on this row')

        req = Request(url, headers=dict(PORTAL_HEADERS, Referer=self.portal_base + '/datasets'))
        try:
            resp = urlopen(req, timeout=180)
            blob = resp.read()
        except Exception as e:
            if '403' not in str(e):
                raise
            time.sleep(2)
            resp = urlopen(Request(url), timeout=180)
            blob = resp.read()

        if not filename:
            cd = resp.headers.get('content-disposition') or ''
            m = re.search(r'filename="?([^";]+)', cd)
            filename = m.group(1).strip() if m else default_name + '.xlsx'
        # Post-condition: a Drupal error page is HTML, HTTP 200, and would be saved
        # as a silently corrupt ".xlsx" without this check.
        if blob[:2] != b'PK' and blob[:5] != b'%PDF-' and b'<html' in blob[:600].lower():
            raise ValueError('{} returned an HTML page, not a file ({} bytes)'
                             .format(url[:70], len(blob)))
        os.makedirs(dest_dir, exist_ok=True)
        path = os.path.join(dest_dir, re.sub(r'[<>:"/\\|?*]', '_', filename))
        with open(path, 'wb') as fh:
            fh.write(blob)
        print('  DTM: {} ({:,} bytes)'.format(os.path.basename(path), len(blob)))
        return path

    def search_portal_datasets(self, search='', country_id=None, page=0):
        """Search DTM datasets portal.

        Args:
            search: Search keywords (e.g., 'msna', 'flow monitoring')
            country_id: DTM portal country facet ID (see PORTAL_COUNTRY_IDS)
            page: Page number (0-indexed)

        Returns dict with: datasets (list), total_pages
        """
        url = '{}/datasets?sort_by=field_dataset_published_date&sort_order=DESC'.format(
            self.portal_base)
        if search:
            url += '&search={}'.format(quote(search))
        facet_idx = 0
        if country_id:
            # Drupal facets need URL-encoded brackets: f%5B0%5D=dataset_country:82
            url += '&f%5B{}%5D=dataset_country:{}'.format(facet_idx, country_id)
            facet_idx += 1
        if page > 0:
            url += '&page={}'.format(page)

        try:
            html = self._portal_get(url)
        except Exception as e:
            print('  DTM portal: {}'.format(e))
            return {'datasets': [], 'total_pages': 0}

        # Extract dataset links — DTM uses Drupal, links are in /datasets/{slug}
        datasets = []
        links = re.findall(r'href="(/datasets/[^"]+)"', html)

        seen = set()
        for path in links:
            if path in seen:
                continue
            seen.add(path)
            slug = path.replace('/datasets/', '')

            # Extract title from surrounding context
            idx = html.find(path)
            if idx >= 0:
                context = html[idx:idx + 500]
                # Title is typically right after the closing tag
                title_match = re.search(r'>\s*([^<]{10,100})', context)
                title = title_match.group(1).strip() if title_match else slug.replace('-', ' ').title()
            else:
                title = slug.replace('-', ' ').title()

            datasets.append({
                'title': title,
                'url': '{}{}'.format(self.portal_base, path),
                'slug': slug,
            })

        # Total pages
        pages_match = re.search(r'page=(\d+)["\s>]*last', html)
        total_pages = int(pages_match.group(1)) if pages_match else 0

        return {'datasets': datasets, 'total_pages': total_pages}

    def search_portal_msna(self, iso3, max_pages=3):
        """Search DTM portal for MSNA datasets for a country.

        Args:
            iso3: ISO3 country code
            max_pages: Max pages to scrape

        Returns list of dataset dicts.
        """
        country_id = PORTAL_COUNTRY_IDS.get(iso3.upper())
        all_datasets = []

        for page in range(max_pages):
            result = self.search_portal_datasets(
                search='msna', country_id=country_id, page=page)
            datasets = result['datasets']
            if not datasets:
                break
            all_datasets.extend(datasets)
            if page >= result.get('total_pages', 0):
                break
            time.sleep(0.5)

        print('  DTM portal: {} MSNA datasets for {}'.format(
            len(all_datasets), iso3))
        return all_datasets

    # ── Legacy methods (backward compat) ─────────────────────

    def search_displacement_tracking(self, country_name, rows=20):
        """Search HDX for displacement tracking data (broader than DTM)."""
        query = 'displacement+tracking+{}'.format(country_name.replace(' ', '+'))
        params = 'q={}&rows={}'.format(query, rows)
        result = self._hdx_get('package_search', params)
        return [{
            'name': pkg.get('name', ''),
            'title': pkg.get('title', ''),
            'org': (pkg.get('organization', {}) or {}).get('title', ''),
            'date': str(pkg.get('metadata_modified', ''))[:10],
            'num_resources': len(pkg.get('resources', [])),
            'resources': [{'name': r.get('name', ''), 'format': r.get('format', ''),
                           'url': r.get('url', '')} for r in pkg.get('resources', [])],
        } for pkg in result.get('results', [])]

    def get_tracking_data(self, iso3):
        """Legacy: get DTM tracking via old API. Falls back to v3."""
        return self.get_idp_data(iso3, admin_level=0)

    @staticmethod
    def datasets_to_csv_rows(datasets):
        """Flatten datasets to CSV rows (1 row per resource)."""
        rows = []
        for ds in datasets:
            for res in ds.get('resources', []):
                rows.append({
                    'dataset_name': ds['name'],
                    'dataset_title': ds['title'],
                    'org': ds.get('org', ''),
                    'date': ds.get('date', ''),
                    'resource_name': res.get('name', ''),
                    'resource_format': res.get('format', ''),
                    'resource_url': res.get('url', ''),
                })
        return rows

    @staticmethod
    def save_csv(records, filepath):
        """Save records to CSV."""
        save_csv(records, filepath)


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='IOM DTM Client')
    parser.add_argument('country', nargs='?', default='lebanon',
                        help='Country name or ISO3')
    parser.add_argument('--msna', action='store_true',
                        help='Search MSNA datasets on DTM portal')
    parser.add_argument('--idp', action='store_true',
                        help='Get IDP figures via DTM API v3')
    parser.add_argument('--admin', type=int, default=0,
                        help='Admin level for IDP (0, 1, 2)')
    args = parser.parse_args()

    dtm = DTMClient()
    iso3 = args.country.upper() if len(args.country) == 3 else ''

    if args.msna and iso3:
        print('=== DTM Portal — MSNA datasets for {} ==='.format(iso3))
        datasets = dtm.search_portal_msna(iso3)
        for d in datasets:
            print('  {} | {}'.format(d['title'][:60], d['url']))

    elif args.idp and iso3:
        print('=== DTM API v3 — IDP Admin{} for {} ==='.format(args.admin, iso3))
        idps = dtm.get_idp_data(iso3, admin_level=args.admin)
        print('{} records'.format(len(idps)))
        for r in idps[:10]:
            loc = r['admin{}'.format(args.admin)] or r['admin0']
            print('  {} | {:,} IDPs | {} | {}'.format(
                loc, r['num_idps'], r['displacement_reason'][:20],
                r['reporting_date'][:10]))

    else:
        country = args.country
        print('=== IOM DTM — {} ==='.format(country))
        datasets = dtm.search_dtm_datasets(country)
        print('DTM datasets on HDX: {}'.format(len(datasets)))
        for ds in datasets[:5]:
            print('  {} [{}] ({})'.format(
                ds['title'][:60], ds['org'][:30], ds['date']))
