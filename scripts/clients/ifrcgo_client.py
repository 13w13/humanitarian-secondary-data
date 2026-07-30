"""
IFRC Go Client
===============
Query IFRC Go API for emergencies, appeals, field reports, and 3W data.
Public API for core endpoints — no authentication required.

Usage:
    from ifrcgo_client import IFRCGoClient
    ifrc = IFRCGoClient()
    events = ifrc.get_emergencies(country_id=84)  # Lebanon
    appeals = ifrc.get_appeals(country_id=84)
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
from urllib.request import Request, urlopen
from urllib.parse import urlencode

from config import DEFAULT_TIMEOUT, USER_AGENT, save_csv

IFRCGO_BASE = 'https://goadmin.ifrc.org/api/v2'

# IFRC Go uses numeric country IDs — cache resolved dynamically
# Source: goadmin.ifrc.org/api/v2/country/?iso3=XXX
IFRC_COUNTRY_IDS = {}


class IFRCGoClient:
    """Client for IFRC Go API (public, no auth for core endpoints)."""

    def __init__(self):
        self.base = IFRCGO_BASE

    def _get(self, endpoint, params=None):
        """GET request to IFRC Go API. Returns parsed JSON."""
        url = '{}/{}'.format(self.base, endpoint)
        if params:
            url += '?{}'.format(urlencode(params, doseq=True))
        req = Request(url, headers={
            'User-Agent': USER_AGENT,
            'Accept': 'application/json',
        })
        resp = urlopen(req, timeout=DEFAULT_TIMEOUT)
        return json.loads(resp.read())

    def _resolve_country_id(self, iso3):
        """Convert ISO3 to IFRC Go numeric country ID.

        IFRC Go API uses numeric IDs. We resolve by fetching the country list
        and matching on iso3 field (exact match).
        """
        iso3 = iso3.upper()
        cid = IFRC_COUNTRY_IDS.get(iso3)
        if cid:
            return cid
        # Dynamic resolution — fetch countries and match iso3
        try:
            # ⚠ `limit: 300` est une valeur en dur : si l'IFRC depasse 300 pays/
            # territoires, la resolution ISO3 echouerait en silence pour la queue de
            # liste. On verifie le compte annonce par l'API contre ce qu'on a recu.
            data = self._get('country/', {'limit': 300})
            total = data.get('count')
            got = len(data.get('results') or [])
            if isinstance(total, int) and got < total:
                print('  IFRC GO: {} pays recus sur {} annonces (limit=300 trop bas) '
                      '-> resolution ISO3 incomplete'.format(got, total))
            results = data.get('results', [])
            for c in results:
                c_iso3 = (c.get('iso3') or c.get('iso') or '').upper()
                if c_iso3 == iso3:
                    IFRC_COUNTRY_IDS[iso3] = c['id']
                    return c['id']
        except Exception:
            pass
        raise ValueError('No IFRC Go ID for {}. Check ISO3 code.'.format(iso3))

    def _paginate(self, endpoint, params=None, max_results=500):
        """Paginate through IFRC Go results (limit+offset)."""
        if params is None:
            params = {}
        params['limit'] = min(100, max_results)
        params['offset'] = 0
        all_results = []

        while len(all_results) < max_results:
            data = self._get(endpoint, params)
            results = data.get('results', [])
            if not results:
                break
            all_results.extend(results)
            if not data.get('next'):
                break
            params['offset'] += len(results)

        return all_results[:max_results]

    def get_emergencies(self, iso3=None, country_id=None, dtype=None,
                        date_from=None, limit=100):
        """Get emergencies (disasters/crises).

        Args:
            iso3: Country ISO3 (auto-resolves to country_id)
            country_id: IFRC Go country ID (overrides iso3)
            dtype: Disaster type ID (e.g. 21=Earthquake)
            date_from: YYYY-MM-DD (disaster_start_date__gte)
            limit: Max results

        Returns:
            List of emergency dicts: id, name, dtype, status, affected/dead/injured,
            date_start, country, appeal amounts.
        """
        params = {'ordering': '-disaster_start_date'}
        if country_id:
            params['countries__in'] = str(country_id)
        elif iso3:
            params['countries__in'] = str(self._resolve_country_id(iso3))
        if dtype:
            params['dtype'] = str(dtype)
        if date_from:
            params['disaster_start_date__gte'] = date_from

        try:
            results = self._paginate('event/', params, max_results=limit)
        except Exception as e:
            print('  IFRC Go emergencies: {}'.format(e))
            return []

        records = []
        for ev in results:
            countries = ev.get('countries', [])
            country_names = ', '.join(c.get('name', '') for c in countries)

            records.append({
                'event_id': ev.get('id', ''),
                'name': ev.get('name', ''),
                'dtype': ev.get('dtype', {}).get('name', '') if isinstance(ev.get('dtype'), dict) else '',
                'status': 'active' if ev.get('is_featured') else 'past',
                'num_affected': ev.get('num_affected') or 0,
                'num_dead': ev.get('num_dead') or 0,
                'num_injured': ev.get('num_injured') or 0,
                'num_displaced': ev.get('num_displaced') or 0,
                'num_missing': ev.get('num_missing') or 0,
                'date_start': ev.get('disaster_start_date', ''),
                'countries': country_names,
                'glide': ev.get('glide', ''),
                # ⚠ IFRC publie en FRANCS SUISSES, pas en dollars. Les colonnes
                # s'appelaient *_amount_* et etaient lues comme des USD.
                'appeal_amount_requested_chf': ev.get('amount_requested') or 0,
                'appeal_amount_funded_chf': ev.get('amount_funded') or 0,
                'currency': 'CHF',
            })

        return records

    def get_appeals(self, iso3=None, country_id=None, status=None, limit=100):
        """Get appeals (DREF, Emergency Appeal, etc.).

        Returns:
            List of appeal dicts: id, name, type, status, amounts, dates.
        """
        # ⚠ LES DEUX ENDPOINTS UTILISENT DES CONVENTIONS OPPOSEES (mesure 2026-07-25,
        # meme minute, meme id pays 161 = Soudan) :
        #     appeal/  country__in=161   -> IGNORE : 4 207 resultats (tout le corpus)
        #     appeal/  country=161       -> 72 resultats, tous SDN            <- bon
        #     appeal/  country__iso3=SDN -> 72 resultats, tous SDN            <- bon
        #     event/   countries__in=161 -> 79 resultats, tous SDN            <- bon
        #     event/   countries=161     -> IGNORE : 6 028 resultats
        # Le code utilisait `country__in` pour les appels : le filtre ne portait pas et
        # une requete Soudan rendait des appels Nigeria, Tchad, Kirghizistan.
        params = {'ordering': '-start_date'}
        want_iso3 = None
        if country_id:
            params['country'] = str(country_id)
        elif iso3:
            want_iso3 = iso3.upper()
            params['country__iso3'] = want_iso3
        if status:
            params['status'] = str(status)

        try:
            results = self._paginate('appeal/', params, max_results=limit)
        except Exception as e:
            print('  IFRC Go appeals: {}'.format(e))
            return []

        # Post-condition : le filtre a-t-il porte ? Un parametre ignore par cette API
        # ne produit aucune erreur, seulement le corpus mondial.
        if want_iso3:
            got = sorted({str((r.get('country') or {}).get('iso3') or '')
                          for r in results if r.get('country')})
            off = [c for c in got if c and c != want_iso3]
            if off:
                raise ValueError(
                    'IFRC GO appeal/ a renvoye {} pour une requete {} : le filtre pays '
                    'n\'a pas porte.'.format(off[:5], want_iso3))

        records = []
        for ap in results:
            records.append({
                'appeal_id': ap.get('id', ''),
                'code': ap.get('code', ''),
                'name': ap.get('name', ''),
                'atype': ap.get('atype_display', ''),
                'status': ap.get('status_display', ''),
                'country': ap.get('country', {}).get('name', '') if isinstance(ap.get('country'), dict) else '',
                # ⚠ Montants en FRANCS SUISSES (IFRC publie en CHF).
                'amount_requested_chf': ap.get('amount_requested') or 0,
                'amount_funded_chf': ap.get('amount_funded') or 0,
                'currency': 'CHF',
                'coverage_pct': round(
                    (ap.get('amount_funded', 0) or 0) / (ap.get('amount_requested', 1) or 1) * 100, 1),
                'num_beneficiaries': ap.get('num_beneficiaries') or 0,
                'start_date': ap.get('start_date', ''),
                'end_date': ap.get('end_date', ''),
            })

        return records

    def get_field_reports(self, iso3=None, country_id=None, limit=50):
        """Get field reports (situation updates from NS).

        Returns:
            List of field report dicts: id, summary, dtype, date, countries.
        """
        params = {'ordering': '-updated_at'}
        if country_id:
            params['countries__in'] = str(country_id)
        elif iso3:
            params['countries__in'] = str(self._resolve_country_id(iso3))

        try:
            results = self._paginate('field-report/', params, max_results=limit)
        except Exception as e:
            print('  IFRC Go field reports: {}'.format(e))
            return []

        records = []
        for fr in results:
            countries = fr.get('countries', [])
            country_names = ', '.join(c.get('name', '') for c in countries)

            records.append({
                'report_id': fr.get('id', ''),
                'summary': (fr.get('summary', '') or '')[:200],
                'dtype': fr.get('dtype', {}).get('name', '') if isinstance(fr.get('dtype'), dict) else '',
                'num_affected': fr.get('num_affected') or 0,
                'num_displaced': fr.get('num_displaced') or 0,
                'countries': country_names,
                'created_at': fr.get('created_at', ''),
                'updated_at': fr.get('updated_at', ''),
            })

        return records

    def get_projects(self, iso3=None, country_id=None, limit=200):
        """Get 3W projects (who what where).

        Returns:
            List of project dicts: id, name, sector, status, NS, budget, people_targeted.
        """
        params = {'ordering': '-start_date'}
        if country_id:
            params['country'] = str(country_id)
        elif iso3:
            params['country'] = str(self._resolve_country_id(iso3))

        try:
            results = self._paginate('project/', params, max_results=limit)
        except Exception as e:
            print('  IFRC Go projects: {}'.format(e))
            return []

        records = []
        for proj in results:
            records.append({
                'project_id': proj.get('id', ''),
                'name': proj.get('name', ''),
                'reporting_ns': proj.get('reporting_ns_detail', {}).get('society_name', '')
                    if isinstance(proj.get('reporting_ns_detail'), dict) else '',
                'primary_sector': proj.get('primary_sector_display', ''),
                'programme_type': proj.get('programme_type_display', ''),
                'status': proj.get('status_display', ''),
                'budget_amount': proj.get('budget_amount') or 0,
                'target_total': proj.get('target_total') or 0,
                'reached_total': proj.get('reached_total') or 0,
                'start_date': proj.get('start_date', ''),
                'end_date': proj.get('end_date', ''),
            })

        return records

    def get_country_profile(self, iso3):
        """Get country overview (NS info, INFORM scores, key figures).

        Returns dict with: country info, society name, INFORM score, key figures.
        """
        iso3 = iso3.upper()
        try:
            # Resolve country ID first, then fetch by ID for accuracy
            cid = self._resolve_country_id(iso3)
            data = self._get('country/{}/'.format(cid))
            c = data  # Direct object, not paginated
        except Exception as e:
            print('  IFRC Go country: {}'.format(e))
            return {}

        if not c or not isinstance(c, dict):
            return {}
        return {
            'id': c.get('id', ''),
            'name': c.get('name', ''),
            'iso3': c.get('iso3', ''),
            'society_name': c.get('society_name', ''),
            'overview': (c.get('overview', '') or '')[:300],
            'inform_score': c.get('inform_score'),
            'key_climate_event': c.get('key_climate_event', {}).get('name', '')
                if isinstance(c.get('key_climate_event'), dict) else '',
        }

    @staticmethod
    def save_csv(records, filepath):
        """Save records to CSV."""
        save_csv(records, filepath)


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    iso3 = sys.argv[1] if len(sys.argv) > 1 else 'LBN'
    ifrc = IFRCGoClient()

    print('=== IFRC Go — {} ==='.format(iso3))

    profile = ifrc.get_country_profile(iso3)
    if profile:
        print('Country: {} ({})'.format(profile['name'], profile['society_name']))
        print('  INFORM score: {}'.format(profile['inform_score']))

    events = ifrc.get_emergencies(iso3=iso3, limit=10)
    print('\nEmergencies: {}'.format(len(events)))
    for ev in events[:5]:
        print('  [{}] {} — {} affected, {} dead ({})'.format(
            ev['event_id'], ev['name'][:50], ev['num_affected'],
            ev['num_dead'], ev['date_start'][:10] if ev['date_start'] else ''))

    appeals = ifrc.get_appeals(iso3=iso3, limit=10)
    print('\nAppeals: {}'.format(len(appeals)))
    for ap in appeals[:5]:
        print('  [{}] {} — ${:,.0f} / ${:,.0f} ({:.1f}%) [{}]'.format(
            ap['code'], ap['name'][:40],
            ap['amount_funded_chf'], ap['amount_requested_chf'],
            ap['coverage_pct'], ap['atype']))

    projects = ifrc.get_projects(iso3=iso3, limit=10)
    print('\nProjects (3W): {}'.format(len(projects)))
    for p in projects[:5]:
        print('  {} — {} [{}] — {}'.format(
            p['name'][:50], p['primary_sector'], p['status'], p['reporting_ns']))
