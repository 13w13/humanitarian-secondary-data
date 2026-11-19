"""
WHO Disease Outbreak News (DON) client
======================================
Notices that WHO publishes on acute public health events: an outbreak of a known
disease in a new place, an unknown disease, a cluster of deaths. Public feed, no key:
the Sitefinity OData API behind who.int/emergencies/disease-outbreak-news.

What the feed is, and what it is not (CAVEATS below, carried into every fetch):
a DON is an OFFICIAL notification. It goes through the Member State concerned under
the International Health Regulations (IHR 2005), it is published days or weeks after
the event, and what gets published can be political. Use it as an alert to
cross-check with field sources (ReliefWeb, ministries, partners), never as a case
count, and never read "no notice" as "no outbreak".

Country attribution. The default answer carries no country. The countries are in a
field the API hides unless asked for, `regionscountries`, a list of taxonomy GUIDs
that WHO staff tag on each notice; the WHO country taxonomy resolves them (`Name` =
lower-case ISO3 for 214 of 229 entries). This beats matching names in the title,
which fails on the cases that matter: "Sudan virus disease - Uganda" is not Sudan,
"Crimean-Congo haemorrhagic fever - Iraq" is not Congo, and the Ebola notices of
July 2026 were also tagged France and Germany (cases treated there), which the title
does not say. Some notices are tagged at region level only ("poliovirus - Israel"
carries the European Region, not Israel), so `for_country` also reads the title of
the region-tagged and global notices of the window.

Usage:
    from who_don_client import WHODONClient
    don = WHODONClient()
    don.latest(12)                          # the 12 most recent notices, all countries
    don.latest(50, since='2026-09-01')
    don.for_country('COD')                  # last 365 days; NotCovered when none
    don.for_country('SDN', since='2025-01-01')
    don.last_meta                           # window, basis, regional notices not attributed
"""
import sys
if hasattr(sys.stdout, 'reconfigure'):   # absent in Jupyter, IDLE, captured output
    sys.stdout.reconfigure(encoding='utf-8')

import html
import json
import re
from datetime import date, datetime, timedelta
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from config import (DEFAULT_TIMEOUT, USER_AGENT, NotCovered, normalize_iso3,
                    raise_unavailable, save_csv,
                    WHO_DON_BASE, WHO_TAXA_BASE, WHO_COUNTRY_TAXONOMY_ID,
                    WHO_DON_PAGE_URL, WHO_DON_CULTURE, WHO_DON_PAGE_SIZE,
                    WHO_DON_MAX_PAGES, WHO_DON_DEFAULT_DAYS, WHO_DON_SUMMARY_CHARS)

CAVEATS = [
    'Official notifications: a DON goes through the Member State under the IHR (2005), '
    'is published with a delay and can be political. An alert to cross-check, not a '
    'case count.',
    'No notice is not no outbreak: WHO publishes a DON for a fraction of events, after '
    'notification or verification by the State.',
    'N notices are not N outbreaks: updates repeat the same title (2026-DON622 and '
    '2026-DON625 are one Ebola event).',
    'Countries = WHO\'s own tags on each notice. Regional and global notices name no '
    'country and are not attributed to one.',
]

BASIS = ('WHO country tags (hidden field regionscountries, resolved through the WHO '
         'country taxonomy), plus the title of region-tagged and global notices; '
         'not a free-text search')

# Taxonomy entries whose `Name` is not an ISO3 code (read on 2026-11-19).
_NAME_TO_ISO3 = {
    'germany': 'DEU', 'czech-republic': 'CZE', 'occupied-palestinian-territory': 'PSE',
    'réunion': 'REU', 'curaçao': 'CUW', 'faroe-islands': 'FRO',
}

# Short names a title may use instead of the taxonomy label.
_ALIASES = {
    'GBR': ['United Kingdom'], 'USA': ['United States'], 'RUS': ['Russia'],
    'SYR': ['Syria'], 'IRN': ['Iran'], 'COD': ['DRC', 'DR Congo'], 'TZA': ['Tanzania'],
    'PSE': ['Palestine', 'occupied Palestinian territory', 'Gaza', 'West Bank'],
    'KOR': ['South Korea'], 'PRK': ['North Korea'], 'LAO': ['Lao PDR', 'Laos'],
    'VNM': ['Vietnam'], 'TUR': ['Turkey', 'Turkiye'], 'CIV': ["Cote d'Ivoire", 'Ivory Coast'],
    'BOL': ['Bolivia'], 'VEN': ['Venezuela'], 'MDA': ['Moldova'], 'FSM': ['Micronesia'],
    'CZE': ['Czech Republic'], 'SWZ': ['Swaziland'], 'MKD': ['Macedonia'],
    'CPV': ['Cape Verde'], 'REU': ['La Réunion'],
}

# Disease and place names that CONTAIN a country name. Blanked before a title is
# searched for a country. Place confusables of report_figures (South Sudan, Mount
# Lebanon, Guinea-Bissau...) are applied on top, see names_country().
_TITLE_CONFUSABLES = [
    r'Sudan\s+(?:virus|ebola\s*virus|ebolavirus)',   # Sudan virus disease - Uganda
    r'Crimean[\s-]+Congo', r'Congo[\s-]+Crimean',      # CCHF - Iraq
    r'Guinea[\s-]+worm',                               # Dracunculiasis - Ethiopia
    r'Middle\s+East', r'West\s+Nile', r'Rift\s+Valley',
]
_COG_CONFUSABLES = [r'Democratic\s+Republic\s+of\s+(?:the\s+)?Congo', r'\bDR\s*Congo\b',
                    r'\bDRC\b']

_SELECT = ('Id,Title,PublicationDateAndTime,LastModified,UrlName,DonId,Summary,'
           'regionscountries')
_DASHES = re.compile('[‐-―−]')


def _norm(text):
    return _DASHES.sub('-', str(text or '')).replace('’', "'").strip()


def _clean_name(label):
    """Taxonomy label reduced to the words a title uses: no '(...)', no leading 'the'."""
    name = re.sub(r'\s*\(.*?\)\s*', ' ', _norm(label)).strip()
    return re.sub(r'^the\s+', '', name, flags=re.I)


def _plain(text, limit=WHO_DON_SUMMARY_CHARS):
    text = html.unescape(re.sub(r'<[^>]+>', ' ', str(text or '')))
    text = re.sub(r'\s+', ' ', text).strip()
    return text if len(text) <= limit else text[:limit].rstrip() + ' ...'


def _since(since, default_days=None):
    """A date from None / date / datetime / 'YYYY-MM-DD'; None without default."""
    if since is None:
        return date.today() - timedelta(days=default_days) if default_days else None
    if isinstance(since, datetime):
        return since.date()
    if isinstance(since, date):
        return since
    try:
        return datetime.strptime(str(since).strip(), '%Y-%m-%d').date()
    except ValueError:
        raise ValueError('since must be a date or YYYY-MM-DD, got {!r}'.format(since))


def _odata_dt(day):
    return day.strftime('%Y-%m-%dT00:00:00Z')


def split_title(title, place_names=()):
    """(disease, location) read from a DON title. No network: testable.

    Formats seen 1996-2026: 'Disease - Country' (dash, en dash, '- ' or ' -'),
    'Disease, A, B and C' (no dash: cut before the first tagged place),
    'Disease, Multi-country', and before 2004 '1996 - Disease in Country - Update 3'.
    `disease` is the title's own wording, not a harmonised name ('Mpox',
    'Monkeypox' and 'Mpox (monkeypox)' all occur).
    """
    t = _norm(title)
    t = re.sub(r'^\d{4}\s*-\s*', '', t)                              # '2002 - ...'
    t = re.sub(r'\s*-\s*update(?:\s*\d+)?\s*$', '', t, flags=re.I)  # '... - Update 14'
    t = re.sub(r'\s+update\s+\d+\s*$', '', t, flags=re.I).strip()   # '... Update 13'
    m = re.search(r'\s-\s*|\s*-\s', t)       # FIRST spaced dash: '(ex -Sierra Leone)'
    if m:
        return t[:m.start()].strip(' ,:-'), t[m.end():].strip(' ,:-')
    low = t.lower()
    hits = [low.find(n.lower()) for n in place_names if n and n.lower() in low]
    hits = [h for h in hits if h > 0]
    if hits:
        pos = min(hits)
        head = re.sub(r'(?:[\s,&-]|\bin\b)+$', '', t[:pos])
        return head.strip(), t[pos:].strip()
    m = re.search(r',\s*(multi[\s-]?(?:country|locations?)|global\b.*)$', t, re.I)
    if m:
        return t[:m.start()].strip(), m.group(1).strip()
    m = re.search(r'\s+in\s+', t)
    if m:
        return t[:m.start()].strip(), t[m.end():].strip()
    return t, ''


def names_country(title, iso3, label=''):
    """Does the title name this country? Confusables blanked first. No network."""
    iso3 = str(iso3 or '').upper()
    text = _norm(title)
    for pat in _TITLE_CONFUSABLES + (_COG_CONFUSABLES if iso3 == 'COG' else []):
        text = re.sub(pat, ' ', text, flags=re.I)
    from report_figures import _strip_confusables     # South Sudan, Guinea-Bissau...
    text, _ = _strip_confusables(text, iso3)
    names = [n for n in [_clean_name(label)] + _ALIASES.get(iso3, []) if n]
    if not names:
        return False
    pattern = r'\b(?:{})\b'.format('|'.join(re.escape(_norm(n)) for n in names))
    return bool(re.search(pattern, text, re.I))


class NoNoticeInWindow(NotCovered):
    """WHO covers the country but tagged no notice to it in the window.

    A NotCovered for Python callers (the client contract), read as `empty` by the
    pipeline: the source answered, with nothing for this country and period. An
    ISO3 absent from the WHO taxonomy stays a plain NotCovered (`not_covered`).
    """


def _taxon_iso3(name):
    name = str(name or '').strip().lower()
    if name in _NAME_TO_ISO3:
        return _NAME_TO_ISO3[name]
    if len(name) == 3 and name.isascii() and name.isalpha():
        return name.upper()
    return ''


class WHODONClient:
    """WHO Disease Outbreak News (public, no auth). The constructor makes no request."""

    def __init__(self, base=None, taxa_base=None, timeout=DEFAULT_TIMEOUT):
        self.base = base or WHO_DON_BASE
        self.taxa_base = taxa_base or WHO_TAXA_BASE
        self.timeout = timeout
        self.page_size = WHO_DON_PAGE_SIZE
        self.max_pages = WHO_DON_MAX_PAGES
        self.last_saturated = False
        self.last_meta = {}
        self._taxa = None

    def _get_json(self, url, params):
        full = '{}?{}'.format(url, urlencode(params, quote_via=quote, safe="$,'():/"))
        req = Request(full, headers={'User-Agent': USER_AGENT, 'Accept': 'application/json'})
        try:
            with urlopen(req, timeout=self.timeout) as resp:
                body = resp.read()
        except Exception as e:  # noqa: BLE001 - outage -> SourceUnavailable, else re-raised
            raise_unavailable('WHO DON {}'.format(url.rstrip('/').rsplit('/', 1)[-1]), e)
        try:
            return json.loads(body)
        except ValueError:
            raise ValueError('WHO DON: non-JSON answer from {} (a site page instead of '
                             'the API?)'.format(url))

    def taxonomy(self):
        """{guid: {'title', 'iso3', 'kind', 'parent'}} of the WHO country taxonomy.

        Three requests (229 entries on 2026-11-19), kept for the life of the client.
        `kind` is 'country' (ISO3 resolved), 'global', or 'region' (WHO regions,
        Oceania, Headquarters).
        """
        if self._taxa is not None:
            return self._taxa
        rows, skip = [], 0
        for _ in range(10):
            got = self._get_json(self.taxa_base, {
                '$filter': 'TaxonomyId eq {}'.format(WHO_COUNTRY_TAXONOMY_ID),
                '$top': '100', '$skip': str(skip),
                '$select': 'Id,Title,Name,FullUrl,ParentId'}).get('value')
            if not isinstance(got, list):
                raise ValueError('WHO taxonomy: no "value" list (the answer changed shape)')
            rows += got
            if len(got) < 100:
                break
            skip += 100
        if not rows:
            raise ValueError('WHO country taxonomy came back empty: notices cannot be '
                             'attributed to countries')
        taxa = {}
        for r in rows:
            iso3 = _taxon_iso3(r.get('Name'))
            name = str(r.get('Name') or '').strip().lower()
            taxa[r['Id']] = {
                'title': str(r.get('Title') or '').strip(), 'iso3': iso3,
                'kind': 'country' if iso3 else ('global' if name == 'global' else 'region'),
                'parent': r.get('ParentId') or ''}
        self._taxa = taxa
        return taxa

    def _notices(self, filter_expr=None, limit=None):
        """Raw notices, newest first, paged with $skip (never @odata.nextLink: http://)."""
        out, seen, skip, pages = [], set(), 0, 0
        self.last_saturated = False
        while True:
            top = min(self.page_size, WHO_DON_PAGE_SIZE)     # $top > 100 = HTTP 400
            if limit is not None:
                top = min(top, limit - len(out))
            params = {'sf_culture': WHO_DON_CULTURE, '$orderby': 'PublicationDateAndTime desc',
                      '$top': str(top), '$skip': str(skip), '$select': _SELECT}
            if filter_expr:
                params['$filter'] = filter_expr
            got = self._get_json(self.base, params).get('value')
            if not isinstance(got, list):
                raise ValueError('WHO DON: no "value" list (the answer changed shape)')
            for item in got:                         # a tie on the date can repeat a row
                key = item.get('Id') or (item.get('UrlName'), item.get('PublicationDateAndTime'))
                if key not in seen:
                    seen.add(key)
                    out.append(item)
            pages += 1
            if len(got) < top or (limit is not None and len(out) >= limit):
                break
            if pages >= self.max_pages:
                self.last_saturated = True
                break
            skip += top
        return out

    def _record(self, item, taxa, match_basis=''):
        countries, iso3s, regions, unresolved, is_global = [], [], [], 0, False
        for guid in item.get('regionscountries') or []:
            t = taxa.get(guid)
            if not t:
                unresolved += 1
            elif t['kind'] == 'country':
                countries.append(t['title'])
                iso3s.append(t['iso3'])
            else:
                regions.append(t['title'])
                is_global = is_global or t['kind'] == 'global'
        title = str(item.get('Title') or '').strip()
        disease, location = split_title(
            title, [_clean_name(n) for n in countries + regions])
        if is_global:
            scope = 'global'
        elif regions:
            scope = 'multi-country' if iso3s else 'regional'
        elif len(iso3s) > 1:
            scope = 'multi-country'
        elif iso3s:
            scope = 'country'
        else:
            scope = 'untagged'
        return {
            'published': item.get('PublicationDateAndTime') or '',
            'title': title,
            'disease': disease,
            'location_in_title': location,
            'countries_named': ';'.join(countries),
            'iso3_named': ';'.join(iso3s),
            'regions_named': ';'.join(regions),
            'scope': scope,
            'match_basis': match_basis,
            'don_id': item.get('DonId') or '',
            'url': WHO_DON_PAGE_URL.format(item.get('UrlName') or ''),
            'summary': _plain(item.get('Summary')),
            'last_modified': item.get('LastModified') or '',
            'tags_unresolved': unresolved,
        }

    def latest(self, limit=20, since=None):
        """The `limit` most recent notices, all countries, newest first.

        `since` ('YYYY-MM-DD') bounds the window on the server. An empty list here
        is an answer (no notice in the window), not an outage: outages raise
        SourceUnavailable.
        """
        limit = int(limit)
        if limit < 1:
            raise ValueError('limit must be at least 1')
        day = _since(since)
        taxa = self.taxonomy()
        expr = 'PublicationDateAndTime ge {}'.format(_odata_dt(day)) if day else None
        return [self._record(i, taxa) for i in self._notices(expr, limit=limit)]

    def _ancestors(self, guid, taxa):
        out, cur = [], taxa.get(guid, {}).get('parent')
        while cur and cur in taxa and cur not in out:
            out.append(cur)
            cur = taxa[cur]['parent']
        return out

    def for_country(self, iso3, since=None):
        """Notices that concern a country since `since` (default: last 365 days).

        Basis (see BASIS): the notices WHO tagged with the country, plus the
        region-tagged and global notices whose title names it (`match_basis`
        'who_tag' or 'title'). Raises NotCovered when no notice concerns the
        country in the window, with the count of regional notices left unattributed;
        SourceUnavailable on an outage; ValueError if the walk hit the page cap
        before it could conclude. `last_meta` keeps the window and the regional
        notices.
        """
        iso3 = normalize_iso3(iso3)
        day = _since(since, WHO_DON_DEFAULT_DAYS)
        taxa = self.taxonomy()
        node = next((g for g, t in taxa.items() if t['iso3'] == iso3), None)
        if node is None:
            raise NotCovered(
                'No entry for {} in the WHO country taxonomy: DON notices cannot be '
                'attributed to it (a territory WHO files under another country?)'
                .format(iso3))
        label = taxa[node]['title']
        when = _odata_dt(day)
        tagged = self._notices('regionscountries/any(x:x eq {}) and '
                               'PublicationDateAndTime ge {}'.format(node, when))
        saturated = self.last_saturated
        rows = [self._record(i, taxa, 'who_tag') for i in tagged]
        wider = self._ancestors(node, taxa) + [g for g, t in taxa.items()
                                               if t['kind'] == 'global']
        unattributed = []
        if wider:
            expr = '({}) and PublicationDateAndTime ge {}'.format(
                ' or '.join('regionscountries/any(x:x eq {})'.format(g) for g in wider),
                when)
            have = {r['url'] for r in rows}
            for item in self._notices(expr):
                rec = self._record(item, taxa, 'title')
                if rec['url'] in have:
                    continue
                if names_country(rec['title'], iso3, label):
                    rows.append(rec)
                else:
                    rec['match_basis'] = 'region_only'
                    unattributed.append(rec)
            saturated = saturated or self.last_saturated
        rows.sort(key=lambda r: r['published'], reverse=True)
        self.last_saturated = saturated
        self.last_meta = {
            'iso3': iso3, 'country': label, 'since': day.isoformat(), 'basis': BASIS,
            'by_tag': sum(1 for r in rows if r['match_basis'] == 'who_tag'),
            'by_title': sum(1 for r in rows if r['match_basis'] == 'title'),
            'regional_notices': unattributed, 'saturated': saturated,
        }
        if rows:
            return rows
        if saturated:
            raise ValueError(
                'WHO DON walk stopped at the page cap ({} pages) before reaching {}: '
                'cannot conclude that no notice concerns {}. Narrow `since`.'
                .format(self.max_pages, day.isoformat(), iso3))
        # Key facts in the first 110 characters: the pipeline note keeps only those.
        raise NoNoticeInWindow(
            'No DON notice concerns {} ({}) since {}; {} regional or global notices in the '
            'window name no country and are not attributed (read them: last_meta). '
            'Basis: {}. No notice is not no outbreak: DONs cover IHR-notified events '
            'only, with delay.'.format(label, iso3, day.isoformat(), len(unattributed),
                                       BASIS))

    @staticmethod
    def save_csv(records, filepath):
        save_csv(records, filepath)


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    don = WHODONClient()
    if len(sys.argv) > 1:
        code = sys.argv[1]
        since = sys.argv[2] if len(sys.argv) > 2 else None
        try:
            rows = don.for_country(code, since=since)
        except NotCovered as e:
            print(e)
            rows = []
        print('=== WHO DON - {} since {} : {} notice(s) ==='.format(
            code.upper(), don.last_meta.get('since'), len(rows)))
    else:
        rows = don.latest(12)
        print('=== WHO DON - 12 most recent notices ===')
    for r in rows:
        print('  {} {:<12} [{}] {} | {}'.format(
            r['published'][:10], r['don_id'], r['iso3_named'] or r['regions_named'],
            r['disease'][:60], r['match_basis']))
    for c in CAVEATS:
        print('  ! ' + c)
