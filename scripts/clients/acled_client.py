"""
ACLED Direct API Client
========================
Query ACLED conflict events directly (more granular than HAPI aggregate).
Returns individual events with lat/lon, actors, notes.

Auth: OAuth2 password grant. Credentials via keyring sds.acled/{email,password}
or env vars ACLED_EMAIL + ACLED_PASSWORD. Register at acleddata.com.

Token flow:
  POST https://acleddata.com/oauth/token → access_token (24h) + refresh_token (14d)
  Then: Authorization: Bearer {access_token} on all data requests.

Usage:
    from acled_client import ACLEDClient
    acled = ACLEDClient()
    events = acled.get_events('Lebanon', date_from='2024-01-01')
    acled.save_csv(events, 'data/LBN/acled_events.csv')
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
import os
import time
from urllib.request import Request, urlopen
from urllib.parse import urlencode

from config import DEFAULT_TIMEOUT, RATE_LIMIT_DELAY, USER_AGENT, save_csv

ACLED_TOKEN_URL = 'https://acleddata.com/oauth/token'
ACLED_DATA_URL = 'https://acleddata.com/api/acled/read'
ACLED_CAST_URL = 'https://acleddata.com/api/cast/read'
ACLED_DELETED_URL = 'https://acleddata.com/api/deleted/read'


class ACLEDClient:
    """Client for ACLED API with OAuth2 authentication."""

    # Event type categories
    EVENT_TYPES = [
        'Battles', 'Explosions/Remote violence', 'Violence against civilians',
        'Protests', 'Riots', 'Strategic developments',
    ]

    def __init__(self, email=None, password=None):
        import keyring
        self.email = email or keyring.get_password('sds.acled', 'email') or os.environ.get('ACLED_EMAIL', '')
        self.password = password or keyring.get_password('sds.acled', 'password') or os.environ.get('ACLED_PASSWORD', '')
        if not self.email or not self.password:
            raise ValueError(
                'ACLED requires email + password. Set via:\n'
                '  keyring.set_password("sds.acled", "email", "...")\n'
                '  keyring.set_password("sds.acled", "password", "...")\n'
                'Or env vars ACLED_EMAIL + ACLED_PASSWORD.\n'
                'Register at https://acleddata.com/')
        self._access_token = None
        self._refresh_token = None

    def _authenticate(self):
        """Get OAuth2 access token via password grant."""
        payload = urlencode({
            'username': self.email,
            'password': self.password,
            'grant_type': 'password',
            'client_id': 'acled',
        }).encode('utf-8')
        req = Request(ACLED_TOKEN_URL, data=payload, method='POST',
                      headers={'User-Agent': USER_AGENT, 'Content-Type': 'application/x-www-form-urlencoded'})
        resp = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
        self._access_token = resp['access_token']
        self._refresh_token = resp.get('refresh_token', '')
        print('  ACLED: OAuth2 token obtained (24h validity)')

    def _ensure_token(self):
        """Authenticate if no token yet."""
        if not self._access_token:
            self._authenticate()

    def _get(self, params, base_url=None):
        """GET request to an ACLED API endpoint with Bearer auth.

        Args:
            params: Query parameters dict.
            base_url: Override base URL (default: ACLED_DATA_URL).
        """
        self._ensure_token()
        url = '{}?{}'.format(base_url or ACLED_DATA_URL, urlencode(params))
        req = Request(url, headers={
            'User-Agent': USER_AGENT,
            'Authorization': 'Bearer {}'.format(self._access_token),
        })
        try:
            resp = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
        except Exception as e:
            # Token may have expired — retry once
            if '401' in str(e) or 'Unauthorized' in str(e):
                print('  ACLED: Token expired, re-authenticating...')
                self._authenticate()
                req = Request(url, headers={
                    'User-Agent': USER_AGENT,
                    'Authorization': 'Bearer {}'.format(self._access_token),
                })
                resp = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
            else:
                raise
        if not resp.get('success', True):
            raise RuntimeError('ACLED API error: {}'.format(resp.get('error', 'unknown')))
        return resp.get('data', [])

    def get_events(self, country, date_from=None, date_to=None,
                   event_type=None, population=False, limit=5000):
        """Fetch conflict events for a country.

        Args:
            country: Country name (e.g., 'Lebanon', 'Sudan')
            date_from: Start date YYYY-MM-DD
            date_to: End date YYYY-MM-DD
            event_type: Filter by event type (see EVENT_TYPES)
            population: If True, request population_best field (ACLED population=TRUE param)
            limit: Max records per page (ACLED max = 5000)

        Returns list of event dicts with: event_date, event_type, sub_event_type,
        disorder_type, actor1, actor2, interaction, inter1, inter2,
        admin1, admin2, admin3, location, latitude, longitude,
        fatalities, civilian_targeting, tags, notes, source, iso3.
        If population=True, also includes population_best.
        """
        all_events = []
        page = 1
        while True:
            params = {
                'country': country,
                # ⚠ SANS CECI, ACLED fait un LIKE avec joker : `country=Sudan` rend
                # AUSSI le Soudan du Sud (verifie 2026-07-25). Le filtre devient une
                # recherche de sous-chaine, et le fichier "Soudan" contient deux pays.
                # `country_where='='` force l'egalite stricte.
                'country_where': '=',
                'limit': limit,
                'page': page,
            }
            if population:
                params['population'] = 'TRUE'
            if date_from:
                params['event_date'] = date_from
                params['event_date_where'] = '>='
            if date_to:
                if 'event_date' in params:
                    # ACLED supports range via |
                    params['event_date'] = '{}|{}'.format(date_from, date_to)
                    params['event_date_where'] = 'BETWEEN'
                else:
                    params['event_date'] = date_to
                    params['event_date_where'] = '<='
            if event_type:
                params['event_type'] = event_type

            data = self._get(params)
            if not data:
                break

            for e in data:
                row = {
                    'event_id': e.get('event_id_cnty', ''),
                    'event_date': e.get('event_date', ''),
                    'year': e.get('year', ''),
                    'event_type': e.get('event_type', ''),
                    'sub_event_type': e.get('sub_event_type', ''),
                    'disorder_type': e.get('disorder_type', ''),
                    'actor1': e.get('actor1', ''),
                    'actor2': e.get('actor2', ''),
                    'interaction': e.get('interaction', ''),
                    'inter1': e.get('inter1', ''),
                    'inter2': e.get('inter2', ''),
                    'admin1': e.get('admin1', ''),
                    'admin2': e.get('admin2', ''),
                    'admin3': e.get('admin3', ''),
                    'location': e.get('location', ''),
                    'latitude': e.get('latitude', ''),
                    'longitude': e.get('longitude', ''),
                    'fatalities': int(e.get('fatalities', 0) or 0),
                    'civilian_targeting': e.get('civilian_targeting', ''),
                    'tags': e.get('tags', ''),
                    'notes': e.get('notes', ''),
                    'source': e.get('source', ''),
                    'source_scale': e.get('source_scale', ''),
                    # ⚠ `iso3` N'EXISTE PAS dans la reponse ACLED : il etait vide sur
                    # 4 862 lignes sur 4 862. On garde `country` (texte) et `iso`
                    # (code NUMERIQUE ISO 3166-1, ex 729 = Soudan), qui existent.
                    'country': e.get('country', ''),
                    'iso': e.get('iso', ''),
                }
                if population:
                    row['population_best'] = int(e.get('population_best', 0) or 0)
                all_events.append(row)

            if len(data) < limit:
                break
            page += 1
            time.sleep(RATE_LIMIT_DELAY)

        # Post-condition : le filtre a-t-il REELLEMENT porte sur un seul pays ?
        # Sans `country_where='='`, ACLED fait un LIKE joker et melange les pays
        # (Sudan -> + South Sudan). Une assertion vaut mieux qu'un fichier douteux.
        seen = sorted({e['country'] for e in all_events if e['country']})
        if len(seen) > 1:
            raise ValueError(
                'ACLED a renvoye {} pays pour la requete {!r} : {}. Le filtre '
                'country_where= n\'a pas porte.'.format(len(seen), country, seen[:5]))
        if all_events and seen and seen[0].strip().lower() != str(country).strip().lower():
            print('  ACLED: demande {!r}, recu {!r} (nom canonique different ?)'
                  .format(country, seen[0]))
        return all_events

    def get_event_summary(self, country, date_from=None, date_to=None):
        """Get summary stats: events and fatalities by type.

        Returns dict: {event_type: {events: N, fatalities: N}}
        """
        events = self.get_events(country, date_from, date_to)
        summary = {}
        for e in events:
            etype = e['event_type']
            if etype not in summary:
                summary[etype] = {'events': 0, 'fatalities': 0}
            summary[etype]['events'] += 1
            summary[etype]['fatalities'] += e['fatalities']
        return summary

    def get_cast_forecasts(self, country, year=None):
        """Fetch CAST conflict forecasts for a country.

        CAST (Conflict Alert System Tool) provides monthly conflict predictions
        at admin1 level: battles, explosions/remote violence, violence against civilians.

        Args:
            country: Country name (e.g., 'Lebanon', 'Sudan')
            year: Optional year filter (int or str)

        Returns list of dicts with: country, admin1, month, year,
        total_forecast, battles_forecast, erv_forecast, vac_forecast,
        total_observed, battles_observed, erv_observed, vac_observed.
        """
        params = {'country': country}
        if year:
            params['year'] = str(year)

        data = self._get(params, base_url=ACLED_CAST_URL)
        forecasts = []
        for row in data:
            forecasts.append({
                'country': row.get('country', ''),
                'admin1': row.get('admin1', ''),
                'month': row.get('month', ''),
                'year': row.get('year', ''),
                'total_forecast': row.get('total_forecast', ''),
                'battles_forecast': row.get('battles_forecast', ''),
                'erv_forecast': row.get('erv_forecast', ''),
                'vac_forecast': row.get('vac_forecast', ''),
                'total_observed': row.get('total_observed', ''),
                'battles_observed': row.get('battles_observed', ''),
                'erv_observed': row.get('erv_observed', ''),
                'vac_observed': row.get('vac_observed', ''),
            })
        return forecasts

    def get_deleted(self, since_timestamp=None):
        """Fetch deleted event IDs.

        Useful for keeping a local mirror in sync — returns events that ACLED
        has removed from the dataset since a given timestamp.

        Args:
            since_timestamp: Unix timestamp (int). If None, returns all deletions.

        Returns list of dicts with: event_id_cnty, deleted_timestamp.
        """
        params = {}
        if since_timestamp is not None:
            params['deleted_timestamp'] = str(since_timestamp)

        data = self._get(params, base_url=ACLED_DELETED_URL)
        deletions = []
        for row in data:
            deletions.append({
                'event_id_cnty': row.get('event_id_cnty', ''),
                'deleted_timestamp': row.get('deleted_timestamp', ''),
            })
        return deletions

    @staticmethod
    def save_csv(events, filepath):
        """Save events to CSV."""
        save_csv(events, filepath)


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    country = sys.argv[1] if len(sys.argv) > 1 else 'Lebanon'
    acled = ACLEDClient()

    print('=== ACLED Direct — {} ==='.format(country))
    summary = acled.get_event_summary(country, date_from='2025-01-01')
    print('Event types: {}'.format(len(summary)))

    for etype, stats in sorted(summary.items()):
        print('  {} — {} events, {} fatalities'.format(
            etype, stats['events'], stats['fatalities']))

    # CAST forecasts
    print('\n=== ACLED CAST Forecasts — {} ==='.format(country))
    try:
        forecasts = acled.get_cast_forecasts(country)
        print('Forecast records: {}'.format(len(forecasts)))
        if forecasts:
            latest = forecasts[-1]
            print('  Latest: {}/{} — {} total forecast (admin1: {})'.format(
                latest['month'], latest['year'],
                latest['total_forecast'], latest['admin1']))
    except Exception as e:
        print('  CAST not available: {}'.format(e))

    # Deleted events (just count)
    print('\n=== ACLED Deleted Events ===')
    try:
        deleted = acled.get_deleted()
        print('Total deleted events tracked: {}'.format(len(deleted)))
    except Exception as e:
        print('  Deleted endpoint not available: {}'.format(e))
