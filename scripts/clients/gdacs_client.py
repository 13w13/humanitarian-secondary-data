"""
GDACS Disaster Alerts Client
==============================
Query Global Disaster Alert and Coordination System for real-time disaster alerts.
Public API — no authentication required.

6 event types: EQ (earthquake), TC (tropical cyclone), FL (flood),
               VO (volcano), WF (wildfire), DR (drought)

Usage:
    from gdacs_client import GDACSClient
    gdacs = GDACSClient()
    alerts = gdacs.get_alerts(country='Lebanon')
    alerts = gdacs.get_alerts(event_type='EQ', alert_level='Red')
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
from urllib.request import Request, urlopen
from urllib.parse import urlencode

from config import DEFAULT_TIMEOUT, USER_AGENT, save_csv

GDACS_BASE = 'https://www.gdacs.org/gdacsapi/api'

EVENT_TYPES = ['EQ', 'TC', 'FL', 'VO', 'WF', 'DR']
EVENT_TYPE_NAMES = {
    'EQ': 'Earthquake', 'TC': 'Tropical Cyclone', 'FL': 'Flood',
    'VO': 'Volcano', 'WF': 'Wildfire', 'DR': 'Drought',
}
ALERT_LEVELS = ['Green', 'Orange', 'Red']


class GDACSUnavailable(Exception):
    """GDACS injoignable : une PANNE, jamais une absence de catastrophe."""


class GDACSClient:
    """Client for GDACS API (public, no auth)."""

    def __init__(self):
        self.base = GDACS_BASE

    def _get(self, endpoint, params=None):
        """GET request to GDACS API. Returns parsed JSON.

        Note: GDACS API sometimes returns XML by default. We force JSON via Accept header
        and fall back to the /SEARCH endpoint which reliably returns GeoJSON.
        """
        url = '{}/{}'.format(self.base, endpoint)
        if params:
            url += '?{}'.format(urlencode(params))
        req = Request(url, headers={
            'User-Agent': USER_AGENT,
            'Accept': 'application/json',
        })
        resp = urlopen(req, timeout=DEFAULT_TIMEOUT)
        content = resp.read()
        if not content or content.strip()[:1] not in (b'{', b'['):
            # GDACS returned HTML/XML instead of JSON — try alternate format
            raise ValueError('GDACS returned non-JSON response')
        return json.loads(content)

    def get_alerts(self, event_type=None, alert_level=None, country=None,
                   date_from=None, date_to=None, limit=100, iso3=None):
        """Alertes catastrophes, filtrables par type / niveau / pays / dates.

        ⚠ FILTRER PAR `iso3`, PAS PAR NOM. Le nom de pays echouait en silence : la
        production passait `COUNTRY_NAMES.get(iso3, iso3.lower()).title()`, donc `PHL`
        devenait **`'Phl'`** -> 0 alerte, sans erreur. Et un evenement GDACS est souvent
        MULTI-PAYS (une secheresse porte `['ETH','KEN','SOM']`) : le nom de tete ne suffit
        pas. On filtre donc sur `affectedcountries[].iso3` (+ l'`iso3` de tete).

        ⚠ PANNE != ABSENCE. Un echec HTTP leve `GDACSUnavailable` ; une reponse valide
        sans alerte pour le pays renvoie `[]`. Sans cette distinction, un serveur en
        panne se lit comme "aucune catastrophe", ce qui est le pire faux possible ici.

        Args:
            iso3: code ISO3 (recommande). Filtrage cote client sur les pays affectes.
            country: nom de pays, transmis a l'API telle quelle (deconseille).
            limit: quand `iso3` est fourni, on elargit la fenetre serveur pour que le
                   filtrage cote client ait de la matiere.
        """
        params = {'limit': str(max(limit, 200) if iso3 else limit)}
        if event_type:
            params['eventtype'] = event_type.upper()
        if alert_level:
            params['alertlevel'] = alert_level.capitalize()
        if date_from:
            params['fromDate'] = date_from
        if date_to:
            params['toDate'] = date_to
        if country and not iso3:
            params['country'] = country

        try:
            data = self._get('Events/geteventlist/SEARCH', params)
        except Exception:
            # Fallback: try the RSS-to-JSON proxy format
            try:
                alt_url = 'https://www.gdacs.org/gdacsapi/api/events?format=geojson'
                if event_type:
                    alt_url += '&eventtype={}'.format(event_type.upper())
                if alert_level:
                    alt_url += '&alertlevel={}'.format(alert_level.capitalize())
                if limit:
                    alt_url += '&limit={}'.format(limit)
                req = Request(alt_url, headers={
                    'User-Agent': USER_AGENT,
                    'Accept': 'application/json',
                })
                resp = urlopen(req, timeout=DEFAULT_TIMEOUT)
                data = json.loads(resp.read())
            except Exception as e2:
                # PANNE, pas absence : on leve au lieu de rendre une liste vide qui
                # se lirait comme "aucune catastrophe".
                raise GDACSUnavailable(
                    'GDACS injoignable ({}) : PANNE, pas une absence d\'alerte'
                    .format(str(e2)[:90]))

        features = data.get('features', [])
        records = []
        for f in features:
            props = f.get('properties', {})
            geom = f.get('geometry', {})
            coords = geom.get('coordinates', [None, None])

            # Pays affectes : liste de {iso2, iso3, countryname}. Un evenement est
            # souvent multi-pays.
            affected = props.get('affectedcountries')
            iso_list, name_list = [], []
            if isinstance(affected, list):
                for c in affected:
                    if isinstance(c, dict):
                        if c.get('iso3'):
                            iso_list.append(str(c['iso3']).upper())
                        if c.get('countryname'):
                            name_list.append(str(c['countryname']))
            head_iso = str(props.get('iso3') or '').upper()
            if head_iso and head_iso not in iso_list:
                iso_list.insert(0, head_iso)

            # Severite : le champ est `severitydata`, PAS `severity` (les colonnes
            # severity_* etaient vides sur toutes les lignes).
            sev = props.get('severitydata') or {}

            records.append({
                'event_id': props.get('eventid', ''),
                'event_type': props.get('eventtype', ''),
                'event_type_name': EVENT_TYPE_NAMES.get(props.get('eventtype', ''), ''),
                'alert_level': props.get('alertlevel', ''),
                'alert_score': props.get('alertscore', ''),
                'severity_value': sev.get('severity', ''),
                'severity_text': sev.get('severitytext', ''),
                'severity_unit': sev.get('severityunit', ''),
                'country': props.get('country', ''),
                'affected_iso3': ';'.join(iso_list),
                'affected_countries': ';'.join(name_list),
                'n_countries_affected': len(iso_list),
                'name': props.get('name', '') or props.get('eventname', ''),
                'date_start': props.get('fromdate', ''),
                'date_end': props.get('todate', ''),
                'is_current': props.get('iscurrent', ''),
                'glide': props.get('glide', ''),
                'lon': coords[0] if len(coords) > 0 else None,
                'lat': coords[1] if len(coords) > 1 else None,
                'url': (props.get('url') or {}).get('report', ''),
            })

        if iso3:
            want = iso3.upper()
            n_before = len(records)
            records = [r for r in records
                       if want in (r['affected_iso3'] or '').split(';')]
            if not records:
                print('  GDACS: 0 alerte pour {} sur {} evenements examines '
                      '(ABSENCE, pas panne : la reponse etait valide)'
                      .format(want, n_before))
        return records

    def get_event_detail(self, event_type, event_id):
        """Get detailed info for a specific event.

        Args:
            event_type: EQ, TC, FL, VO, WF, DR
            event_id: Numeric event ID

        Returns:
            Dict with full event details.
        """
        try:
            data = self._get('Events/geteventdetails', {
                'eventtype': event_type.upper(),
                'eventid': str(event_id),
            })
        except Exception as e:
            print('  GDACS detail: {}'.format(e))
            return {}

        props = data.get('properties', {})
        return {
            'event_id': props.get('eventid', event_id),
            'event_type': event_type.upper(),
            'name': props.get('name', ''),
            'alert_level': props.get('alertlevel', ''),
            'country': props.get('country', ''),
            'description': props.get('description', ''),
            'date_start': props.get('fromdate', ''),
            'date_end': props.get('todate', ''),
            'severity_value': props.get('severity', {}).get('severity_value', ''),
            'severity_text': props.get('severity', {}).get('severity_text', ''),
            'url': props.get('url', {}).get('report', ''),
        }

    def get_recent_by_iso3(self, iso3, days=90, limit=200):
        """Raccourci : alertes recentes d'un pays, tous types, filtrees par ISO3.

        Remplace `get_recent_by_country(country_name)`, qui passait un NOM et rendait
        0 alerte en silence des que le nom ne collait pas (`'Phl'` pour les Philippines).
        """
        from datetime import datetime, timedelta
        date_from = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        return self.get_alerts(iso3=iso3, date_from=date_from, limit=limit)

    def get_recent_by_country(self, country_name, days=90, limit=200):
        """DEPRECIE : garde pour compat. Preferer `get_recent_by_iso3`."""
        from datetime import datetime, timedelta
        date_from = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        print('  GDACS: get_recent_by_country est deprecie (filtre par NOM, source du '
              'bug "Phl" = 0 alerte). Utiliser get_recent_by_iso3.')
        return self.get_alerts(country=country_name, date_from=date_from, limit=limit)

    @staticmethod
    def save_csv(records, filepath):
        """Save records to CSV."""
        save_csv(records, filepath)


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    country = sys.argv[1] if len(sys.argv) > 1 else None
    gdacs = GDACSClient()

    if country:
        print('=== GDACS — Recent alerts for {} ==='.format(country))
        alerts = gdacs.get_recent_by_country(country)
    else:
        print('=== GDACS — Recent global alerts ===')
        alerts = gdacs.get_alerts(limit=20)

    print('Found {} alerts'.format(len(alerts)))
    for a in alerts[:15]:
        print('  [{}] {} {} — {} ({}) — {}'.format(
            a['alert_level'], a['event_type_name'], a['event_id'],
            a['name'][:50], a['country'], a['date_start'][:10] if a['date_start'] else ''))
