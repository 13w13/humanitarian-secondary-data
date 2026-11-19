"""
Epidemics: which source signals which outbreak, and since when
==============================================================
    python -X utf8 scripts/epidemic_compare.py SDN
    python -X utf8 scripts/epidemic_compare.py COD --since 2025-06-01 --csv cod_epi.csv
    python -X utf8 scripts/epidemic_compare.py            # every country (ongoing pages)

Five sources, side by side, one row per country and disease:
    reliefweb    ReliefWeb disaster pages of type Epidemic (editors' pages, GLIDE)
    who_don      WHO Disease Outbreak News (official IHR notifications)
    ifrc_go      IFRC GO epidemic emergencies and appeals (DREF, Emergency Appeal)
    africa_cdc   Africa CDC reports, through ReliefWeb (no API of its own)
    who_cholera  WHO weekly cholera cases reported by countries (ArcGIS dashboard)

The point is the GAPS, not a total. Each source sees outbreaks through its own
filter: an official notification (DON) needs the State, a Red Cross appeal needs a
National Society request, a ReliefWeb page needs an editor, and editors do not
always close their pages. Observed on 2026-11-19: WHO's weekly series showed cholera
cases in Sudan every few weeks in 2026, while no DON had been tagged Sudan since
January 2023. A disease missing from one column is a question to ask that source,
never a proof that the outbreak is over.

A source that fails is shown `unavailable` (or `skipped` without a key): never as
an absence. Disease names are matched by keywords (DISEASES below), so a rare
disease outside the list shows as `other: <wording>`.
"""
import argparse
import csv
import os
import re
import sys
from datetime import date, timedelta

if hasattr(sys.stdout, 'reconfigure'):   # absent in Jupyter, IDLE, captured output
    sys.stdout.reconfigure(encoding='utf-8')

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, 'clients'))

SOURCES = ('reliefweb', 'who_don', 'ifrc_go', 'africa_cdc', 'who_cholera')
CORE = ('reliefweb', 'who_don', 'ifrc_go')     # the three a gap is reported against
DEFAULT_DAYS = 365

# (key, pattern). Order matters only for readability; a text can match several keys.
DISEASES = [
    ('cholera', r'cholera|acute watery diarrh'), ('measles', r'measles'),
    ('mpox', r'mpox|monkeypox'),
    ('ebola', r'ebola|bundibugyo|\bbvd\b|\bevd\b|sudan virus|sudan ebolavirus'),
    ('tuberculosis', r'tuberculosis'), ('leishmaniasis', r'kala-azar|leishmaniasis'),
    ('marburg', r'marburg'), ('dengue', r'dengue'), ('yellow fever', r'yellow fever'),
    ('meningitis', r'meningit'), ('diphtheria', r'diphtheria'),
    ('polio', r'polio|cvdpv|wpv1'), ('chikungunya', r'chikungunya'),
    ('lassa fever', r'lassa'), ('anthrax', r'anthrax'), ('plague', r'plague'),
    ('hepatitis e', r'hepatitis e\b'), ('malaria', r'malaria'),
    ('avian influenza', r'avian influenza|\bh5n\d|\bh7n9|\bh9n2|influenza a\s*\(h5'),
    ('mers', r'\bmers\b|middle east respiratory'), ('covid-19', r'covid|sars-cov-2'),
    ('rift valley fever', r'rift valley'), ('cchf', r'crimean-congo|\bcchf\b'),
    ('nipah', r'nipah'), ('oropouche', r'oropouche'), ('zika', r'\bzika'),
    ('typhoid', r'typhoid'), ('pertussis', r'pertussis|whooping cough'),
    ('rabies', r'rabies'), ('hantavirus', r'hantavirus'), ('shigellosis', r'shigell'),
    ('unknown disease', r'undiagnosed|unknown (?:aetiology|etiology|origin|disease)|'
                        r'undetermined etiology'),
]


def disease_keys(text):
    """Canonical disease keys named in a text, sorted. [] when none. No network."""
    low = str(text or '').lower()
    return sorted({key for key, pat in DISEASES if re.search(pat, low)})


def _sig(iso3, disease, source, first, last='', n=1, status='', url='', note=''):
    return {'iso3': iso3, 'disease': disease, 'source': source, 'first': first,
            'last': last or first, 'n': n, 'status': status, 'url': url, 'note': note}


def _keys_or_other(text):
    return disease_keys(text) or ['other: {}'.format(str(text or '?').strip()[:40])]


# ── one function per source: raw rows -> signals (no network, testable) ──

PAST_LOOKBACK_DAYS = 1095    # a closed ReliefWeb page this recent still says something


def signals_reliefweb(rows, since):
    """Ongoing and alert pages whatever their start, and pages closed ('past') that
    started up to PAST_LOOKBACK_DAYS before `since`: Sudan's last cholera page
    started in September 2023 and is closed, while WHO's weekly series still
    showed cases in 2026. Seeing the closed page is the finding."""
    out = []
    floor = ''
    if since:
        y, m, d = (int(x) for x in since.split('-'))
        floor = (date(y, m, d) - timedelta(days=PAST_LOOKBACK_DAYS)).isoformat()
    for r in rows:
        live = r['status'] in ('ongoing', 'alert')
        if not live and floor and r['date_start'] < floor:
            continue
        note = 'page open {} days: may be stale'.format(r['open_days']) if r.get('stale') else ''
        for key in _keys_or_other(r['disease']):
            for iso in [c for c in r['iso3'].split(';') if c]:
                out.append(_sig(iso, key, 'reliefweb', r['date_start'], '', 1,
                                r['status'], r['url'], note))
    return out


def signals_who_don(rows):
    out = []
    for r in rows:
        for key in _keys_or_other(r['disease']):
            for iso in [c for c in r['iso3_named'].split(';') if c]:
                out.append(_sig(iso, key, 'who_don', r['published'][:10], '', 1,
                                r.get('match_basis', ''), r['url']))
    return out


def signals_ifrc(rows):
    out = []
    for r in rows:
        status = r['atype'] or r['kind']
        if r.get('status'):
            status = '{} {}'.format(status, r['status'])
        for key in _keys_or_other(r['name']):
            for iso in [c for c in r['iso3'].split(';') if c]:
                out.append(_sig(iso, key, 'ifrc_go', r['date_start'], '', 1, status,
                                r['url']))
    return out


def signals_africa_cdc(rows):
    """Reports whose TITLE names a disease. The weekly report's title names none:
    those are counted apart (see `untitled`), never attributed to a disease."""
    out, untitled = [], 0
    for r in rows:
        keys = disease_keys(r['title'])
        if not keys:
            untitled += 1
            continue
        for key in keys:
            for iso in [c for c in r['iso3'].split(';') if c]:
                out.append(_sig(iso, key, 'africa_cdc', r['date'], '', 1, r['format'],
                                r['url']))
    return out, untitled


def signals_cholera(rows):
    """One signal per country: first and last week WITH cases in the window."""
    by_iso = {}
    for r in rows:
        if (r.get('cases') or 0) > 0:
            by_iso.setdefault(r['iso3'], []).append(r)
    out = []
    for iso, weeks in by_iso.items():
        weeks.sort(key=lambda w: w['week_start'])
        out.append(_sig(iso, 'cholera', 'who_cholera', weeks[0]['week_start'],
                        weeks[-1]['week_start'], sum(w['cases'] for w in weeks),
                        'cases reported', '', 'cumulative {}'.format(weeks[-1]['cum_cases'])))
    return out


def merge_signals(signals, status_by_source, since=''):
    """One row per (iso3, disease): first/last/n per source, and the gaps.

    `gaps` lists the CORE sources that answered (status 'ok') and say nothing
    while another source signals the outbreak, plus 'reliefweb page closed' when
    ReliefWeb's page is past while another source still signals it since `since`.
    A source that failed is never a gap.
    """
    table = {}
    for s in signals:
        row = table.setdefault((s['iso3'], s['disease']), {})
        cell = row.get(s['source'])
        live = s['source'] == 'reliefweb' and s['status'] in ('ongoing', 'alert')
        if cell is None or (live and cell['status'] not in ('ongoing', 'alert')):
            row[s['source']] = dict(s)      # an open ReliefWeb page wins over closed ones
        elif s['source'] == 'reliefweb' and not live and cell['status'] in ('ongoing', 'alert'):
            continue
        else:
            cell['first'] = min(cell['first'], s['first'])
            cell['last'] = max(cell['last'], s['last'])
            cell['n'] += s['n']
            cell['note'] = cell['note'] or s['note']
            cell['status'] = cell['status'] or s['status']
    out = []
    for (iso, disease), cells in sorted(table.items()):
        gaps = ['no ' + src for src in CORE if src not in cells
                and status_by_source.get(src) == 'ok']
        rw = cells.get('reliefweb')
        if rw and rw['status'] == 'past' and len(cells) == 1:
            gaps = []          # a closed page alone is history, not a missed outbreak
        if rw and rw['status'] == 'past' and any(
                c['last'] >= (since or '') for k, c in cells.items() if k != 'reliefweb'):
            gaps.append('reliefweb page closed')
        stale = [c['note'] for c in cells.values() if c.get('note', '').startswith('page open')]
        out.append({'iso3': iso, 'disease': disease, 'cells': cells, 'gaps': gaps,
                    'stale': stale})
    return out


def _cell_text(cell, source, status):
    if cell is None:
        return '-' if status == 'ok' else status
    if source == 'reliefweb':
        txt = 'since {} ({})'.format(cell['first'], cell['status'])
        return txt + ' STALE?' if cell.get('note') else txt
    if source == 'who_cholera':
        return '{} cases {}..{}'.format(cell['n'], cell['first'][:7], cell['last'][:7])
    span = cell['first'] if cell['first'] == cell['last'] else '{}..{}'.format(
        cell['first'], cell['last'])
    return '{} x {}'.format(cell['n'], span)


def _failure(exc):
    from config import MissingCredential, NotCovered, is_outage
    if isinstance(exc, MissingCredential):
        return 'skipped'
    if isinstance(exc, NotCovered):
        return 'not_covered'
    return 'unavailable' if is_outage(exc) else 'error'


def collect(iso3=None, since=None):
    """Query the five sources. Returns (signals, status_by_source, context)."""
    from reliefweb_client import ReliefWebClient
    from who_don_client import WHODONClient, NoNoticeInWindow
    from ifrcgo_client import IFRCGoClient
    from who_cholera_client import WHOCholeraClient
    since = since or (date.today() - timedelta(days=DEFAULT_DAYS)).isoformat()
    signals, status, context = [], {}, {'since': since}

    def run(name, fn):
        try:
            got = fn()
            status[name] = 'ok'
            return got
        except Exception as e:  # noqa: BLE001 - recorded per source, never a zero
            status[name] = _failure(e)
            context[name + '_error'] = '{}: {}'.format(type(e).__name__, str(e)[:160])
            return None

    rw = ReliefWebClient()
    statuses = ('ongoing', 'alert', 'past') if iso3 else ('ongoing', 'alert')
    got = run('reliefweb', lambda: rw.ongoing_epidemics(iso3, statuses=statuses))
    signals += signals_reliefweb(got or [], since)

    don = WHODONClient()

    def don_rows():
        if not iso3:
            return don.latest(300, since=since)
        try:
            return don.for_country(iso3, since=since)
        except NoNoticeInWindow:          # answered: nothing tagged in the window
            return []
    got = run('who_don', don_rows)
    signals += signals_who_don(got or [])
    context['who_don_regional'] = len((don.last_meta or {}).get('regional_notices') or [])

    got = run('ifrc_go', lambda: IFRCGoClient().epidemics(iso3, since=since))
    signals += signals_ifrc(got or [])

    got = run('africa_cdc', lambda: rw.get_source_reports('Africa CDC', iso3=iso3,
                                                          date_from=since, limit=100))
    acdc, untitled = signals_africa_cdc(got or [])
    signals += acdc
    context['africa_cdc_untitled'] = untitled

    def cholera_rows():
        from config import NotCovered
        try:
            return WHOCholeraClient().weekly(iso3, since=since)
        except NotCovered:                # no cholera reported since 2023: an answer
            return []
    got = run('who_cholera', cholera_rows)
    signals += signals_cholera(got or [])
    return signals, status, context


def compare(iso3=None, since=None):
    """(rows, status_by_source, context). For a country, only its own rows: the
    other countries a multi-country notice or report tags are left out."""
    signals, status, ctx = collect(iso3, since)
    rows = merge_signals(signals, status, ctx['since'])
    if iso3:
        rows = [r for r in rows if r['iso3'] == iso3]
    return rows, status, ctx


def main(argv=None):
    p = argparse.ArgumentParser(description='Which source signals which epidemic.')
    p.add_argument('iso3', nargs='?', help='ISO3 code; omit for every country')
    p.add_argument('--since', help='YYYY-MM-DD (default: 365 days ago)')
    p.add_argument('--csv', help='also write the table to this CSV file')
    args = p.parse_args(argv)
    iso3 = None
    if args.iso3:
        from config import normalize_iso3
        iso3 = normalize_iso3(args.iso3)
    rows, status, ctx = compare(iso3, args.since)

    print('Epidemics {} since {} | sources: {}'.format(
        iso3 or '(all countries)', ctx['since'],
        ', '.join('{} {}'.format(s, status.get(s)) for s in SOURCES)))
    for s in SOURCES:
        if ctx.get(s + '_error'):
            print('  ! {} {}'.format(s, ctx[s + '_error']))
    head = ['ISO3', 'disease'] + list(SOURCES) + ['gaps']
    print(' | '.join(head))
    flat = []
    for r in rows:
        cells = [_cell_text(r['cells'].get(s), s, status.get(s)) for s in SOURCES]
        gaps = ', '.join(r['gaps'])
        print(' | '.join([r['iso3'], r['disease']] + cells + [gaps]))
        flat.append(dict(zip(head, [r['iso3'], r['disease']] + cells + [gaps])))
    print('{} WHO DON regional or global notices name no country; {} Africa CDC '
          'reports name no disease in their title (weekly report): read them.'.format(
              ctx.get('who_don_regional', 0), ctx.get('africa_cdc_untitled', 0)))
    print('A dash is a source that answered without this outbreak: a question to ask '
          'it, not a proof that the outbreak is over.')
    if args.csv and flat:
        with open(args.csv, 'w', newline='', encoding='utf-8') as fh:
            w = csv.DictWriter(fh, fieldnames=head)
            w.writeheader()
            w.writerows(flat)
        print('Saved {} rows -> {}'.format(len(flat), args.csv))
    return 1 if any(v in ('unavailable', 'error') for v in status.values()) else 0


if __name__ == '__main__':
    sys.exit(main())
