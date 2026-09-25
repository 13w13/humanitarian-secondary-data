"""
Liveuamap Client
================
Scrape conflict events from liveuamap.com regional pages.

Authentication: None (public pages).
Method: Decode base64 'ovens' variable from HTML (page 1),
        then AJAX pagination via act=prevday&id={last_id}.

Each event has: id, name, description, lat, lng, timestamp,
location, cat_id, color_id, source URL, picture, keywords.

Usage:
    from liveuamap_client import LiveuamapClient
    client = LiveuamapClient()
    events = client.get_events('SDN', max_pages=100)
    client.save_csv(events, 'liveuamap_events.csv')

Discovered 2026-03-19. Pagination exhausts naturally (globaltime=0).

Known issue: liveuamap.com (UKR) has aggressive rate-limiting compared to
country subdomains. The client uses adaptive delay + retry with backoff
to handle this. If scraping UKR, use --date-from to limit depth.
"""
import sys
if hasattr(sys.stdout, 'reconfigure'):   # absent in Jupyter, IDLE, captured output
    sys.stdout.reconfigure(encoding='utf-8')

import base64
import json
import re
import time as time_mod
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from config import save_csv, NotCovered, raise_unavailable

# --- Constants -----------------------------------------------------------

LIVEUAMAP_PAGE_DELAY = 1.0   # seconds between pagination requests (country subdomains)
_MAIN_DOMAIN_DELAY = 2.5     # higher delay for liveuamap.com (UKR) - avoids rate-limit retries
LIVEUAMAP_MAX_PAGES = 200    # safety cap per region
_REQUEST_TIMEOUT = 15        # per-request timeout (seconds) - shorter than DEFAULT_TIMEOUT to detect hangs
_MAX_RETRIES = 3             # retries per page on transient errors
_MAX_CONSECUTIVE_ERRORS = 3  # stop pagination after N consecutive failures
_PROGRESS_INTERVAL = 5       # print progress every N pages

# cat_id → human-readable event type (decoded from picpath icon names)
# Full inventory from 10,707 events across 11 HI regions (2026-03-19)
EVENT_TYPES = {
    # --- Armed violence ---
    25: 'armed_clash',          # ak (927)
    27: 'airstrike_shelling',   # bomb (1805)
    26: 'explosion',            # explode (335)
    21: 'drone_strike',         # drone (242)
    7:  'casualties',           # dead (494)
    13: 'shooting',             # gun (1)
    46: 'rocket_attack',        # rocket (46)
    42: 'anti_aircraft',        # aa (249)
    16: 'heavy_weapons',        # heavy (24)
    38: 'landmine',             # mine (5)
    90: 'machinegun',           # machinegun (7)
    80: 'atgm',                 # atgm (1)
    69: 'manpads',              # manpads (1)
    102: 'loitering_munition',  # shahed (108)
    97: 'fpv_drone',            # fpv (6)
    96: 'cruise_missile',       # missile_flying (2) - mapped from old data
    100: 'missile',             # missile_flying (2)
    105: 'air_alert',           # air_alert (7)
    # --- Military operations ---
    6:  'territorial_control',  # capture (165)
    3:  'aviation',             # airplane (151)
    4:  'helicopter',           # helicopter (40)
    19: 'naval',                # ship (98)
    47: 'submarine',            # submarine (2)
    5:  'military_camp',        # camp (5)
    35: 'fortification',        # fort (4)
    87: 'supply_logistics',     # supply (17)
    88: 'flares',               # flares (2)
    95: 'demolition',           # buldozer (6)
    94: 'surveillance_balloon', # balloon (1)
    106: 'aerostat',            # aerostat (2)
    # --- Civilian impact ---
    9:  'fire',                 # fires (71)
    10: 'medical',              # medicine (129)
    56: 'rescue',               # rescue (65)
    60: 'earthquake',           # earthquake (212)
    61: 'flood',                # floods (13)
    66: 'volcano',              # volcano (2)
    67: 'snow_storm',           # snow (1)
    68: 'pollution',            # polution (1)
    74: 'weather',              # sun (1)
    45: 'water_crisis',         # nowater (1)
    52: 'biohazard',            # biohazard (10)
    75: 'transport_attack',     # bus (17)
    1:  'car_attack',           # car (3)
    40: 'railway',              # railway (1)
    # --- Political / social ---
    14: 'political_statement',  # speech (4082)
    22: 'protest',              # rally (321)
    # icon 'elect' = electricity, not elections: every sample row is a grid,
    # substation or blackout event (was mislabelled 'election' until 2026-09).
    51: 'power_infrastructure',  # elect (18)
    12: 'law_enforcement',      # police (210)
    73: 'arrest',               # arrested (161)
    23: 'hostage',              # hostage (14)
    15: 'blockade',             # stop (48)
    17: 'organized_crime',      # thug (14)
    11: 'riot',                 # molotov (1)
    59: 'narcotics',            # drugs (1)
    72: 'smuggling',            # alcohol (1)
    # --- Infrastructure / economy ---
    57: 'resource_conflict',    # natural_resource (33)
    32: 'financial',            # money (64)
    28: 'logistics',            # truck (77)
    39: 'construction',         # crane (3)
    20: 'chemical',             # gas (3)
    55: 'civil_aviation',       # civil_airplane (88)
    # --- Information / media ---
    34: 'communication',        # phone (91)
    33: 'press',                # press (63)
    24: 'cyber',                # wifi (18)
    30: 'imagery',              # picture (13)
    36: 'video_evidence',       # video (15)
    37: 'destruction',          # destroy (15)
    48: 'social_media',         # twitterico (3)
    50: 'social_media',         # facebookico (1)
    58: 'telecom',              # mobile (1)
    # --- Other ---
    65: 'map_update',           # map (28)
    53: 'environmental',        # nature (4)
    8:  'operation',            # op (1)
    29: 'computing',            # comp (1)
    31: 'food_crisis',          # food (12)
    91: 'stun_grenade',         # stun (3)
}

# ISO3 → liveuamap subdomain (106 subdomains confirmed active, 2026-03-19)
# Countries with dedicated subdomains get direct mapping.
# Countries without → mapped to best regional subdomain.
# Note: regional subdomains (sahel, africa, centralafrica) mix multiple countries.
ISO3_TO_SUBDOMAIN = {
    # --- HI priority countries (dedicated subdomains) ---
    'SDN': 'sudan', 'SYR': 'syria', 'YEM': 'yemen',
    'LBN': 'lebanon', 'IRQ': 'iraq', 'AFG': 'afghanistan',
    'COD': 'drcongo', 'PSE': 'israelpalestine', 'MMR': 'myanmar',
    'ETH': 'ethiopia', 'LBY': 'libya', 'UKR': 'liveuamap',
    'SOM': 'somalia', 'IRN': 'iran', 'PAK': 'pakistan',
    'NGA': 'nigeria', 'KEN': 'kenya',
    # --- HI countries → regional fallback ---
    'MLI': 'sahel', 'NER': 'sahel', 'TCD': 'centralafrica',
    'BDI': 'centralafrica', 'SSD': 'sudan', 'MOZ': 'africa',
    'HTI': 'latam',
    # --- Other countries with dedicated subdomains ---
    'DZA': 'algeria', 'ARG': 'argentina', 'BGD': 'bangladesh',
    'BLR': 'belarus', 'BOL': 'bolivia', 'BRA': 'brazil',
    'CMR': 'cameroon', 'CAN': 'canada', 'CHL': 'chile',
    'COL': 'colombia', 'EGY': 'egypt', 'FRA': 'france',
    'GEO': 'georgia', 'DEU': 'germany', 'HND': 'honduras',
    'HKG': 'hongkong', 'HUN': 'hungary', 'IND': 'india',
    'IDN': 'indonesia', 'IRL': 'ireland', 'ISR': 'israelpalestine',
    'ITA': 'italy', 'JPN': 'japan', 'MDV': 'maldives',
    'MLT': 'malta', 'MEX': 'mexico', 'MDA': 'moldova',
    'NIC': 'nicaragua', 'PAN': 'panama', 'PER': 'peru',
    'PHL': 'philippines', 'POL': 'poland', 'PRI': 'puertorico',
    'QAT': 'qatar', 'RUS': 'russia', 'SAU': 'saudiarabia',
    'ZAF': 'southafrica', 'KOR': 'koreas', 'PRK': 'koreas',
    'ESP': 'spain', 'LKA': 'srilanka', 'TWN': 'taiwan',
    'TZA': 'tanzania', 'THA': 'thailand', 'TUN': 'tunisia',
    'TUR': 'turkey', 'UGA': 'uganda', 'ARE': 'emirates',
    'GBR': 'uk', 'USA': 'usa', 'VEN': 'venezuela',
    'VNM': 'vietnam', 'ZWE': 'zimbabwe', 'GUY': 'guyana',
}

# Feeds that are not one country's own. Their events carry no country field, so
# rows from them cannot be attributed to the requested country: `get_events`
# refuses them unless the caller opts in with allow_regional=True.
REGIONAL_FEEDS = {'sahel', 'centralafrica', 'africa', 'latam'}
BINATIONAL_FEEDS = {'israelpalestine': ('ISR', 'PSE'), 'koreas': ('KOR', 'PRK')}
BORROWED_FEEDS = {'SSD': 'sudan'}      # South Sudan read from Sudan's feed


def feed_scope(iso3):
    """'dedicated', 'binational', 'regional', 'borrowed', or None if unmapped."""
    sub = ISO3_TO_SUBDOMAIN.get(iso3)
    if sub is None:
        return None
    if sub in REGIONAL_FEEDS:
        return 'regional'
    if sub in BINATIONAL_FEEDS:
        return 'binational'
    if iso3 in BORROWED_FEEDS:
        return 'borrowed'
    return 'dedicated'


# Thematic/regional subdomains (not mapped to ISO3, use directly)
# 'africa', 'asia', 'baltics', 'caribbean', 'caucasus', 'centralasia',
# 'centralafrica', 'indochina', 'latam', 'northeurope', 'pacific',
# 'sahel', 'westafrica', 'mideast'
# Topics: 'alqaeda', 'alshabab', 'climate', 'corruption', 'cyberwar',
# 'disasters', 'drugwar', 'energy', 'farleft', 'farright', 'health',
# 'hezbollah', 'humanrights', 'isis', 'kashmir', 'kurds', 'migration',
# 'pirates', 'tradewars', 'war', 'weapons', 'wildlife', 'women'

_OVENS_RE = re.compile(r"var\s+ovens\s*=\s*'([^']+)'")
_USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
)


class LiveuamapClient:
    """Client for liveuamap.com conflict event scraping."""

    def __init__(self):
        self.session_cookies = {}
        # What the last get_events() call could NOT say through its return value:
        # whether pagination stopped on errors (a truncated list is not a count),
        # and how old the feed's newest event is (a frozen feed is not calm).
        self.last_meta = {}

    def _get(self, url, headers=None, timeout=None):
        """GET request, returns (status, body_str).

        Uses _REQUEST_TIMEOUT by default (15s) to detect server hangs
        faster than the global DEFAULT_TIMEOUT (30s).
        """
        hdrs = {
            'User-Agent': _USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        if headers:
            hdrs.update(headers)
        if self.session_cookies:
            hdrs['Cookie'] = '; '.join(
                '{}={}'.format(k, v) for k, v in self.session_cookies.items()
            )
        req = Request(url, headers=hdrs)
        resp = urlopen(req, timeout=timeout or _REQUEST_TIMEOUT)
        # Capture cookies
        for hdr in resp.headers.get_all('Set-Cookie') or []:
            name_val = hdr.split(';')[0]
            if '=' in name_val:
                k, v = name_val.split('=', 1)
                self.session_cookies[k.strip()] = v.strip()
        return resp.status, resp.read().decode('utf-8', errors='replace')

    @staticmethod
    def _base_url(subdomain):
        if subdomain == 'liveuamap':
            return 'https://liveuamap.com'
        return 'https://{}.liveuamap.com'.format(subdomain)

    def _decode_ovens(self, html):
        """Extract and decode base64 'ovens' variable from page HTML."""
        match = _OVENS_RE.search(html)
        if not match:
            return None
        try:
            decoded = base64.b64decode(match.group(1)).decode('utf-8')
            return json.loads(decoded)
        except (json.JSONDecodeError, ValueError):
            return None

    def get_events(self, iso3, max_pages=None, date_from=None, date_to=None,
                   allow_regional=False):
        """Fetch all available events for a country.

        Args:
            iso3: Country ISO3 code (e.g. 'SDN', 'SYR')
            max_pages: Max pagination depth (default LIVEUAMAP_MAX_PAGES)
            date_from: Optional YYYY-MM-DD - stop pagination + filter events
            date_to: Optional YYYY-MM-DD - filter events after this date

        Returns:
            List of flat event dicts ready for CSV. See `self.last_meta` for
            truncation and staleness, which a list cannot carry.

        Raises NotCovered when no Liveuamap region maps to the country,
        SourceUnavailable when the first page cannot be reached, and ValueError
        when the page no longer carries the data block (the site changed): none
        of these is "0 events".
        """
        self.last_meta = {'subdomain': None, 'pages': 0, 'stopped_on_errors': False,
                          'newest_event': None, 'stale_days': None}
        subdomain = ISO3_TO_SUBDOMAIN.get(iso3)
        if not subdomain:
            raise NotCovered('Liveuamap has no region mapped to {} (see '
                             'references/liveuamap_iso3_mapping.csv).'.format(iso3))
        scope = feed_scope(iso3)
        if scope in ('regional', 'borrowed') and not allow_regional:
            # A Sahel feed for Mali is mostly not Mali, and nothing in a row says
            # which country it is about: returning it as MLI broke rule 5.
            raise NotCovered(
                'Liveuamap has no feed of its own for {}: "{}" is a {} feed that also '
                'covers other countries, and its events carry no country field. '
                'Pass allow_regional=True to fetch it knowingly.'.format(
                    iso3, subdomain, scope))
        self.last_meta['subdomain'] = subdomain
        self.last_meta['feed_scope'] = scope

        if max_pages is None:
            max_pages = LIVEUAMAP_MAX_PAGES

        # A malformed date used to be ignored, so a typo silently fetched the whole
        # feed with no date filter at all.
        # Dates are read and written in UTC: local time moved events across
        # midnight, and the same run gave different days on different machines.
        date_from_ts = None
        if date_from:
            date_from_ts = int(datetime.strptime(date_from, '%Y-%m-%d')
                               .replace(tzinfo=timezone.utc).timestamp())

        date_to_ts = None
        if date_to:
            # End of day
            date_to_ts = int(datetime.strptime(date_to, '%Y-%m-%d')
                             .replace(tzinfo=timezone.utc).timestamp()) + 86399

        base = self._base_url(subdomain)
        self.session_cookies = {}  # fresh session per region
        _t0 = time_mod.time()

        # --- Page 1: HTML + base64 ---
        try:
            status, html = self._get(base + '/')
        except Exception as e:
            raise_unavailable('Liveuamap {}'.format(subdomain), e)

        if status != 200:
            raise ValueError('Liveuamap {}: unexpected HTTP {} on the first page'
                             .format(subdomain, status))

        data = self._decode_ovens(html)
        if not data:
            raise ValueError(
                'Liveuamap {}: no "ovens" data block in the page. The site layout '
                'changed and the scraper needs updating; this is not an absence '
                'of events.'.format(subdomain))

        all_venues = list(data.get('venues', []))
        seen_ids = {v['id'] for v in all_venues if 'id' in v}
        globaltime = data.get('globaltime', 0)
        page = 1

        # --- Pages 2+: AJAX pagination with retry + adaptive delay ---
        ajax_headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Referer': base + '/',
        }
        # UKR uses the main domain which rate-limits more aggressively.
        # Higher base delay = fewer retries = faster overall.
        delay = _MAIN_DOMAIN_DELAY if subdomain == 'liveuamap' else LIVEUAMAP_PAGE_DELAY
        consecutive_errors = 0

        while page < max_pages and globaltime != 0 and all_venues:
            last_id = all_venues[-1].get('id')
            if not last_id:
                break

            # Stop if we've gone past date_from
            if date_from_ts:
                last_ts = all_venues[-1].get('timestamp', 0)
                if last_ts and last_ts < date_from_ts:
                    break

            time_mod.sleep(delay)
            url = '{}/ajax/do?act=prevday&id={}'.format(base, last_id)

            # Retry loop with exponential backoff
            page_data = None
            for attempt in range(_MAX_RETRIES):
                try:
                    _, body = self._get(url, headers=ajax_headers)
                    page_data = json.loads(body)
                    break
                except (URLError, HTTPError) as e:
                    code = getattr(e, 'code', None)
                    if attempt < _MAX_RETRIES - 1:
                        backoff = delay * (2 ** (attempt + 1))
                        print('  Liveuamap {}: page {} attempt {}/{} failed ({}), retry in {:.0f}s'.format(
                            subdomain, page + 1, attempt + 1, _MAX_RETRIES,
                            'HTTP {}'.format(code) if code else str(e)[:60],
                            backoff), flush=True)
                        time_mod.sleep(backoff)
                    else:
                        print('  Liveuamap {}: page {} failed after {} attempts ({})'.format(
                            subdomain, page + 1, _MAX_RETRIES,
                            'HTTP {}'.format(code) if code else str(e)[:60]),
                            flush=True)
                except json.JSONDecodeError:
                    if attempt < _MAX_RETRIES - 1:
                        backoff = delay * (2 ** (attempt + 1))
                        print('  Liveuamap {}: page {} JSON decode error, retry in {:.0f}s'.format(
                            subdomain, page + 1, backoff), flush=True)
                        time_mod.sleep(backoff)
                    else:
                        print('  Liveuamap {}: page {} JSON decode failed after {} attempts'.format(
                            subdomain, page + 1, _MAX_RETRIES), flush=True)

            if page_data is None:
                consecutive_errors += 1
                if consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
                    print('  Liveuamap {}: {} consecutive errors, stopping pagination '
                          '(collected {} events so far)'.format(
                              subdomain, consecutive_errors, len(all_venues)),
                          flush=True)
                    self.last_meta['stopped_on_errors'] = True
                    break
                # Increase delay after errors
                delay = min(delay * 1.5, 10.0)
                page += 1
                continue

            # Success - reset error counter, ease delay back down
            consecutive_errors = 0
            base_delay = _MAIN_DOMAIN_DELAY if subdomain == 'liveuamap' else LIVEUAMAP_PAGE_DELAY
            if delay > base_delay:
                delay = max(delay * 0.9, base_delay)

            globaltime = page_data.get('globaltime', 0)
            new_venues = page_data.get('venues', [])
            if not new_venues:
                break

            added = 0
            for v in new_venues:
                vid = v.get('id')
                if vid and vid not in seen_ids:
                    seen_ids.add(vid)
                    all_venues.append(v)
                    added += 1

            page += 1
            if added == 0:
                break

            # Progress logging with ETA
            if page % _PROGRESS_INTERVAL == 0:
                elapsed = time_mod.time() - _t0
                sec_per_page = elapsed / page
                remaining_pages = max_pages - page
                eta = sec_per_page * remaining_pages
                last_ts = all_venues[-1].get('timestamp', 0)
                last_dt = ''
                if last_ts:
                    try:
                        last_dt = datetime.fromtimestamp(last_ts, timezone.utc).strftime('%Y-%m-%d')
                    except (ValueError, OSError):
                        pass
                print('  Liveuamap {}: page {}/{} | {} events | oldest {} | {:.0f}s elapsed ~{:.0f}s remaining'.format(
                    subdomain, page, max_pages, len(all_venues),
                    last_dt or '?', elapsed, eta), flush=True)

        # --- Flatten to CSV rows + date filter ---
        # Dropped always-empty fields: description, udescription, keywords,
        # lang, status_id, gi, picx/y, sel_link, target, img_share, runway
        records = []
        for v in all_venues:
            ts = v.get('timestamp')
            if date_from_ts and ts and ts < date_from_ts:
                continue
            if date_to_ts and ts and ts > date_to_ts:
                continue
            cat_id = v.get('cat_id')
            records.append({
                'event_id': v.get('id'),
                'feed': subdomain,
                'feed_scope': scope,
                'datetime': _ts_to_iso(ts),
                'event_type': EVENT_TYPES.get(cat_id, 'other'),
                'cat_id': cat_id,
                'color_id': v.get('color_id'),
                'name': v.get('name', ''),
                'location': v.get('location', ''),
                'lat': v.get('lat'),
                'lng': v.get('lng'),
                'source_url': v.get('source', ''),
                'link': v.get('link', ''),
                'picture': v.get('picture', ''),
            })

        elapsed = time_mod.time() - _t0
        print('  Liveuamap {}: {} events ({} pages, {} after date filter) in {:.0f}s'.format(
            subdomain, len(all_venues), page, len(records), elapsed))

        # STALENESS GUARD (2026-07-24): some regional feeds silently stop
        # updating (sudan frozen at 2026-03-31 while syria/yemen stay live).
        # Warn when the NEWEST event of the whole feed (pre-date-filter) is old,
        # so an empty period-filtered result reads as "feed dead", not "quiet".
        newest_ts = max((v.get('timestamp', 0) or 0 for v in all_venues), default=0)
        self.last_meta['pages'] = page
        # The crawl walks back from today. Stopping at max_pages before reaching
        # date_from used to return a short (or empty) window as if it were complete.
        oldest_ts = min((v.get('timestamp') or 0 for v in all_venues
                         if v.get('timestamp')), default=0)
        if (date_from_ts and oldest_ts and oldest_ts > date_from_ts
                and page >= max_pages and globaltime != 0):
            self.last_meta['truncated_before'] = _ts_to_iso(oldest_ts)[:10]
            print('  Liveuamap {}: max_pages={} reached at {} before --date-from {}: '
                  'the period is only partly covered'.format(
                      subdomain, max_pages, _ts_to_iso(oldest_ts)[:10], date_from))
        if newest_ts:
            age_days = (time_mod.time() - newest_ts) / 86400
            self.last_meta['newest_event'] = _ts_to_iso(newest_ts)[:10]
            self.last_meta['stale_days'] = round(age_days) if age_days > 14 else None
            if age_days > 14:
                print('  Liveuamap {}: WARNING - feed appears STALE: newest event is '
                      '{} ({:.0f} days old). Source-side freeze likely; do not '
                      'interpret 0 recent events as calm.'.format(
                          subdomain, _ts_to_iso(newest_ts)[:10], age_days))
        return records

    @staticmethod
    def save_csv(records, filepath):
        save_csv(records, filepath)


def _ts_to_iso(ts):
    if not ts:
        return ''
    try:
        return datetime.fromtimestamp(ts, timezone.utc).isoformat()
    except (ValueError, OSError, OverflowError):
        return ''


# --- Standalone CLI ---
if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description='Fetch Liveuamap conflict events.')
    p.add_argument('iso3', help='Country ISO3 code (e.g. SDN, SYR, IRN)')
    p.add_argument('--max-pages', type=int, default=200, help='Max pagination depth')
    p.add_argument('--date-from', help='Start date YYYY-MM-DD')
    p.add_argument('--date-to', help='End date YYYY-MM-DD')
    p.add_argument('--output', help='Output CSV path (default: liveuamap_{iso3}.csv)')
    args = p.parse_args()

    iso3 = args.iso3.upper()
    client = LiveuamapClient()
    events = client.get_events(
        iso3, max_pages=args.max_pages,
        date_from=args.date_from, date_to=args.date_to,
    )

    print('Found {} events'.format(len(events)))
    if events:
        # Records use 'datetime' (ISO string), not raw 'timestamp'
        dt_list = sorted(e['datetime'] for e in events if e.get('datetime'))
        if dt_list:
            print('Range: {} -> {}'.format(dt_list[0][:10], dt_list[-1][:10]))
        out = args.output or 'liveuamap_{}.csv'.format(iso3.lower())
        save_csv(events, out)
