"""
INFORM Index API Client
========================
Query INFORM Risk Index for subnational risk data.
Public API - no authentication required.

Usage:
    from inform_client import INFORMClient
    inform = INFORMClient()
    risk = inform.get_country_risk('LBN')
    subnational = inform.get_subnational('LBN')
"""
import sys
if hasattr(sys.stdout, 'reconfigure'):   # absent in Jupyter, IDLE, captured output
    sys.stdout.reconfigure(encoding='utf-8')

import json
import re
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from urllib.parse import urlencode

from config import (
    INFORM_BASE, INFORM_SUBNATIONAL_BASE, DEFAULT_TIMEOUT, USER_AGENT, save_csv,
    raise_unavailable
)

# Main annual releases: "INFORM Risk 2026", "INFORM 2025 2nd edition",
# "INFORM Risk Mid 2025" - NOT the trend workflows ("INFORM Risk 2026 - 2019").
_MAIN_RELEASE_RE = re.compile(r'^INFORM (?:Risk )?(?:Mid )?\d{4}(?: 2nd edition)?$')


class INFORMClient:
    """Client for INFORM Risk Index API (public, no auth)."""

    # Key indicator IDs
    INDICATORS = {
        'INFORM': 'INFORM Risk Index (composite)',
        'HA': 'Hazard & Exposure',
        'VU': 'Vulnerability',
        'CC': 'Lack of Coping Capacity',
        'HA.HUM': 'Human Hazard',
        'HA.NAT': 'Natural Hazard',
        'VU.SEV': 'Socio-Economic Vulnerability',
        'VU.VGR': 'Vulnerable Groups',
        # INFORM codes: CC.INS = Institutional (DRR, governance), CC.INF =
        # Infrastructure (communication, physical, access to health). They were
        # swapped, so the two columns held each other's scores.
        'CC.INS': 'Institutional',
        'CC.INF': 'Infrastructure',
    }

    def __init__(self):
        self.base = INFORM_BASE
        self.subnational_base = INFORM_SUBNATIONAL_BASE

    def _get(self, url, params=None):
        """GET request to INFORM API."""
        if params:
            url += '?{}'.format(urlencode(params))
        req = Request(url, headers={
            'User-Agent': USER_AGENT,
            'Accept': 'application/json',
        })
        resp = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
        return resp

    _workflow_cache = None  # (WorkflowId, Name) - shared across instances

    def _get_retry(self, url, tries=3, delay=4):
        """GET with retries - the JRC server intermittently resets connections."""
        for attempt in range(tries):
            try:
                return self._get(url)
            except Exception:
                if attempt + 1 == tries:
                    raise
                time.sleep(delay * (attempt + 1))

    def _latest_main_workflow(self):
        """Resolve the latest MAIN release workflow (e.g. 505 'INFORM Risk 2026').

        The restructured JRC API (2026) is workflow-based: scores are queried
        per WorkflowId. /Workflows lists ~142 of them; trend workflows carry a
        ' - YYYY' suffix and are NOT the current release.
        """
        if INFORMClient._workflow_cache:
            return INFORMClient._workflow_cache
        api_root = self.base.rsplit('/countries', 1)[0]
        workflows = self._get_retry('{}/Workflows'.format(api_root))
        mains = [w for w in workflows
                 if _MAIN_RELEASE_RE.match((w.get('Name') or '').strip())]
        if not mains:
            # Not "no risk score": the release naming changed under the regex.
            raise ValueError(
                'INFORM: no main release among {} workflows matches {!r}; the '
                'naming convention changed.'.format(len(workflows),
                                                    _MAIN_RELEASE_RE.pattern))
        latest = max(mains, key=lambda w: w.get('WorkflowDate') or '')
        INFORMClient._workflow_cache = (latest['WorkflowId'], latest['Name'].strip())
        return INFORMClient._workflow_cache

    def get_country_risk(self, iso3):
        """Get INFORM Risk scores for a country (workflow-based API, 2026).

        Flow (verified 2026-07-24): /Workflows -> latest main release (505 =
        'INFORM Risk 2026') -> /countries/Scores/?WorkflowId=X&Iso3=Y ->
        rows keyed by IndicatorId (INFORM, HA, VU, CC, ...).
        Returns {} if the release has no INFORM score for this country (fail-safe:
        never an all-zero record). Raises SourceUnavailable when JRC cannot be
        reached: that used to return {} too, and the pipeline then reported an
        outage as "INFORM API restructured".
        """
        try:
            wid, wname = self._latest_main_workflow()
            url = '{}/Scores/?WorkflowId={}&Iso3={}'.format(
                self.base, wid, iso3.upper())
            data = self._get_retry(url)
        except Exception as e:
            raise_unavailable('INFORM scores', e)
        if not data:
            return {}

        # Scope: every row must be the requested country. If the Iso3 filter were
        # ignored, the last row per indicator would win and another country's
        # scores would be labelled with this one.
        want = iso3.upper()
        others = sorted({str(i.get('Iso3', '')).upper() for i in data
                         if i.get('Iso3') and str(i.get('Iso3')).upper() != want})
        if others:
            raise ValueError('INFORM /Scores returned {} for {}: the Iso3 filter did '
                             'not apply'.format(others[:4], want))
        scores = {}
        for item in data:
            scores[item.get('IndicatorId', '')] = item.get('IndicatorScore')

        if not scores.get('INFORM'):
            return {}

        # Missing sub-scores are None: 0.0 is the LOWEST risk, not "unknown".
        return {
            'iso3': want,
            'overall_risk': scores.get('INFORM'),
            'hazard_exposure': scores.get('HA'),
            'human_hazard': scores.get('HA.HUM'),
            'natural_hazard': scores.get('HA.NAT'),
            'vulnerability': scores.get('VU'),
            'socio_economic_vuln': scores.get('VU.SEV'),
            'vulnerable_groups': scores.get('VU.VGR'),
            'coping_capacity': scores.get('CC'),
            'institutional': scores.get('CC.INS'),
            'infrastructure': scores.get('CC.INF'),
            'workflow_id': wid,
            'workflow_name': wname,
        }

    def get_subnational(self, iso3):
        """Get INFORM subnational risk data (admin1 level).

        Returns list of dicts: admin_name, overall_risk, hazard,
        vulnerability, coping_capacity. [] when INFORM has no subnational model
        for the country (HTTP 404); an outage raises SourceUnavailable.
        """
        try:
            data = self._get('{}/{}'.format(self.subnational_base, iso3.upper()))
        except HTTPError as e:
            if e.code == 404:          # no subnational model for this country
                return []
            raise_unavailable('INFORM subnational', e)
        except Exception as e:
            raise_unavailable('INFORM subnational', e)

        if not data:
            return []

        # Group by admin unit
        admin_units = {}
        for item in data:
            admin_name = item.get('Adm1Name', '') or item.get('GeoName', '')
            if not admin_name:
                continue
            if admin_name not in admin_units:
                admin_units[admin_name] = {
                    'iso3': iso3.upper(),
                    'admin_name': admin_name,
                    'admin_code': item.get('Iso3', ''),
                }
            indicator_id = item.get('IndicatorId', '')
            score = item.get('IndicatorScore', 0) or 0
            if indicator_id == 'INFORM':
                admin_units[admin_name]['overall_risk'] = score
            elif indicator_id == 'HA':
                admin_units[admin_name]['hazard_exposure'] = score
            elif indicator_id == 'VU':
                admin_units[admin_name]['vulnerability'] = score
            elif indicator_id == 'CC':
                admin_units[admin_name]['coping_capacity'] = score

        return sorted(admin_units.values(), key=lambda x: x.get('overall_risk', 0), reverse=True)

    def get_all_countries(self, top_n=None):
        """Get INFORM Risk for all countries. Optionally return top N by risk.

        Returns list of dicts: iso3, country_name, overall_risk, rank.
        """
        data = self._get(self.base)
        if not data:
            return []

        countries = {}
        for item in data:
            iso3 = item.get('Iso3', '')
            if not iso3 or len(iso3) != 3:
                continue
            if iso3 not in countries:
                countries[iso3] = {
                    'iso3': iso3,
                    'country_name': item.get('CountryName', ''),
                }
            if item.get('IndicatorId') == 'INFORM':
                countries[iso3]['overall_risk'] = item.get('IndicatorScore', 0)
                countries[iso3]['rank'] = item.get('Rank', '')

        result = sorted(countries.values(), key=lambda x: x.get('overall_risk', 0), reverse=True)
        if top_n:
            result = result[:top_n]
        return result

    @staticmethod
    def save_csv(records, filepath):
        """Save records to CSV."""
        save_csv(records, filepath)


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    iso3 = sys.argv[1] if len(sys.argv) > 1 else 'LBN'
    inform = INFORMClient()

    print('=== INFORM Risk - {} ==='.format(iso3))
    risk = inform.get_country_risk(iso3)
    if risk:
        print('Overall: {:.1f} ({})'.format(
            risk['overall_risk'], risk['workflow_name']))
        print('  Hazard: {:.1f} (human {:.1f}, natural {:.1f})'.format(
            risk['hazard_exposure'], risk['human_hazard'], risk['natural_hazard']))
        print('  Vulnerability: {:.1f} (socio-econ {:.1f}, vulnerable groups {:.1f})'.format(
            risk['vulnerability'], risk['socio_economic_vuln'], risk['vulnerable_groups']))
        print('  Coping capacity: {:.1f} (institutional {:.1f}, infrastructure {:.1f})'.format(
            risk['coping_capacity'], risk['institutional'], risk['infrastructure']))

    subnational = inform.get_subnational(iso3)
    if subnational:
        print('\nSubnational (admin1):')
        for s in subnational[:10]:
            print('  {} - Risk {:.1f}'.format(
                s['admin_name'], s.get('overall_risk', 0)))
