"""
Recensement MSNA multi-pays — trois canaux
==========================================
    python -X utf8 scripts/msna_census.py               # la liste de pays par defaut
    python -X utf8 scripts/msna_census.py SDN LBN       # une selection

TROIS CANAUX, parce qu'aucun ne suffit (mesure 2026-07-25)
----------------------------------------------------------
Le catalogue de reference des MSNA est le site **IMPACT Initiatives** (REACH). Quand il
est injoignable, il faut trianguler, et le detour n'est pas neutre : les trois canaux ne
rendent pas la meme chose. Volumes mesures :

    ReliefWeb, source REACH   11 415 produits, dont **488 avec MSNA au titre**  <- le
                              plus large pour DECOUVRIR qui a une MSNA et quand
    Catalogue IOM DTM         17 produits MSNA / needs assessment sur 12 pays testes,
                              **majoritairement OUVERTS** -> donne les FICHIERS
                              (Mozambique MSNA 2025, Haiti mars 2026, AFG 8 produits ;
                              l'Ukraine verrouille les siens)
    HDX CKAN                  49 datasets -> le plus FAIBLE des trois, il sous-couvre

⚠ Ne pas confondre les canaux : ReliefWeb rend des **rapports**, DTM et HDX rendent des
**fichiers**. Pour une analyse WG-SS il faut le fichier, donc ReliefWeb sert a decouvrir
et les deux autres a recuperer.

⚠ Ce script recense, il ne telecharge PAS de microdonnee. La microdonnee MSNA contient
des reponses de menages : elle ne descend que dans un repertoire temporaire hors du
depot et hors git, et se supprime apres le scan des colonnes. Voir `wgss_probe.py`.

Sortie : `analysis/MSNA_CENSUS_{date}.csv` + un resume a l'ecran.
"""
import sys
import os
import json
import re
import time
from urllib.request import urlopen
from urllib.parse import quote

sys.stdout.reconfigure(encoding='utf-8')

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, 'clients'))
from config import save_csv                                        # noqa: E402

CKAN = 'https://data.humdata.org/api/3/action/package_search'

# Liste par defaut : les contextes de crise ou une MSNA a des chances d'exister.
# Passer ses propres ISO3 en argument pour la remplacer.
DEFAULT_COUNTRIES = ['AFG', 'BDI', 'COD', 'ETH', 'HTI', 'IRQ', 'KEN', 'LBN', 'LBY', 'MLI',
                     'MMR', 'MOZ', 'NER', 'NGA', 'PAK', 'PSE', 'SDN', 'SOM', 'SSD', 'SYR',
                     'TCD', 'UKR', 'YEM']

# Formats qui portent de la donnee exploitable (par opposition a un PDF de rapport).
DATA_FORMATS = {'XLSX', 'XLS', 'CSV', 'JSON', 'ZIP', 'SAV', 'DTA'}

# Signature Washington Group dans un nom de ressource ou une description. On ne peut
# pas confirmer la presence des WG-SS sans ouvrir le fichier : ce recensement rend un
# INDICE, pas une certitude, et le dit.
# ⚠ PAS de `\b` de FIN sur les prefixes de colonnes. `\bwgq\b` ne matche PAS
# `wgq_vision` : l'underscore est un caractere de mot, donc il n'y a pas de frontiere
# entre `wgq` et `_`. Or `wgq_*` / `wg_ss_*` est exactement la forme des noms de
# colonnes Washington Group. Le detecteur ratait donc le cas le plus courant, et
# rendait 0 sur les 49 datasets recenses (verifie 2026-07-25).
WG_HINT = re.compile(
    r'washington group|wg[-_ ]?ss|\bwgq|\bwg_|wg[-_ ]?short set|'
    r'difficulty (?:seeing|hearing|walking|remembering|self[- ]?care|communicat)|'
    r'functional difficult|\bdisabilit', re.I)
MSNA_HINT = re.compile(r'\bmsna\b|multi[- ]?sector\w*\s+needs?\s+assessment|'
                       r'multi[- ]?sectoral\s+needs?\s+assessment', re.I)


def ckan(params):
    try:
        raw = urlopen('{}?{}'.format(CKAN, params), timeout=60).read()
        return json.loads(raw).get('result', {}) or {}
    except Exception as e:
        print('  HDX: {}'.format(str(e)[:90]))
        return {}


def census_reliefweb(iso3):
    """DECOUVERTE : produits MSNA publies, sources REACH puis IMPACT Initiatives.

    Canal le plus large : 488 produits REACH portent MSNA au titre, contre 49 datasets
    sur HDX. Il rend des RAPPORTS, pas des fichiers : il sert a savoir QUI a une MSNA et
    QUAND, ce que HDX sous-couvre lourdement.
    """
    from urllib.request import Request
    from config import get_credential
    app = get_credential('sds.reliefweb', 'appname', 'RELIEFWEB_APPNAME')
    rows = []
    for src in ('REACH', 'IMPACT Initiatives'):
        payload = {
            'filter': {'operator': 'AND', 'conditions': [
                {'field': 'country.iso3', 'value': iso3.lower()},
                {'field': 'source.shortname', 'value': src}]},
            'query': {'value': 'MSNA OR "Multi-Sector Needs Assessment" OR '
                               '"Multi-Sectoral Needs Assessment"', 'fields': ['title']},
            'fields': {'include': ['title', 'date.original', 'url', 'file.filename',
                                   'file.mimetype', 'format.name']},
            'sort': ['date.original:desc'], 'limit': 10,
        }
        try:
            data = json.loads(urlopen(Request(
                'https://api.reliefweb.int/v2/reports?appname=' + app,
                data=json.dumps(payload).encode(),
                headers={'Content-Type': 'application/json',
                         'User-Agent': 'humanitarian-secondary-data/1.0'}),
                timeout=45).read())
        except Exception as e:
            print('  ReliefWeb {} {}: {}'.format(iso3, src, str(e)[:60]))
            continue
        for d in data.get('data', []):
            f = d['fields']
            files = f.get('file') or []
            names = [str(x.get('filename', '')).lower() for x in files]
            rows.append({
                'iso3': iso3.upper(), 'channel': 'reliefweb', 'org': src,
                'dataset': '', 'title': str(f.get('title', ''))[:110],
                'date': str((f.get('date') or {}).get('original', ''))[:10],
                'formats': ';'.join(sorted({n.rsplit('.', 1)[-1] for n in names if '.' in n})),
                'n_resources': len(files),
                # un rapport n'est pas un fichier de donnees exploitable
                'has_data_file': any(n.endswith(('.xlsx', '.xls', '.csv', '.zip'))
                                     for n in names),
                'access': 'open', 'wg_hint': False,
                'url': str(f.get('url', ''))[:120],
            })
    return rows


def census_dtm(iso3):
    """FICHIERS : produits MSNA / needs assessment du catalogue IOM DTM.

    IOM publie aussi des MSNA, et elles sont majoritairement OUVERTES : Mozambique 2025
    (janvier 2026), Haiti (mars 2026), 8 produits Afghanistan. L'Ukraine verrouille les
    siennes, coherent avec sa politique sur la microdonnee d'enquete.
    """
    try:
        from dtm_client import DTMClient
        rows = DTMClient().browse_catalogue(iso3, max_pages=3)
    except Exception:
        return []
    out = []
    for r in rows:
        blob = (r['title'] + ' ' + r['activities']).lower()
        if 'msna' not in blob and 'needs assessment' not in blob:
            continue
        out.append({
            'iso3': iso3.upper(), 'channel': 'dtm', 'org': 'IOM DTM',
            'dataset': r['slug'][:70], 'title': r['title'][:110],
            'date': r['published'], 'formats': r['dataset_format'],
            'n_resources': 1, 'has_data_file': r['access'] == 'open',
            'access': r['access'], 'wg_hint': bool(WG_HINT.search(blob)),
            'url': r['url'][:120],
        })
    return out


def census_country(iso3):
    """Datasets MSNA d'un pays, scopes par la facette de groupe CKAN.

    ⚠ `fq=groups:{iso3}` est le SEUL scope pays fiable : une recherche plein texte
    rend d'autres pays (mesure du 2026-07-24 : 30 lignes ETH sur 35 pour une requete
    LBN). Et CKAN n'accepte QU'UN SEUL `fq`, les filtres se combinent dedans.
    """
    res = ckan('fq=groups:{}&q={}&rows=50'.format(
        iso3.lower(), quote('MSNA OR "multi-sector needs assessment" OR '
                            '"multi-sectoral needs assessment"')))
    rows = []
    for pkg in (res.get('results') or []):
        title = str(pkg.get('title', ''))
        notes = str(pkg.get('notes', ''))
        if not MSNA_HINT.search(title + ' ' + notes):
            continue                       # le q= est permissif, on resserre ici
        groups = [str(g.get('name', '')).upper() for g in (pkg.get('groups') or [])]
        if iso3.upper() not in groups:
            continue                       # post-condition : la facette a bien porte
        resources = pkg.get('resources') or []
        fmts = sorted({str(r.get('format', '')).upper() for r in resources if r.get('format')})
        data_res = [r for r in resources
                    if str(r.get('format', '')).upper() in DATA_FORMATS]
        blob = ' '.join([title, notes] + [str(r.get('name', '')) + ' ' +
                                          str(r.get('description', '')) for r in resources])
        years = sorted(set(re.findall(r'\b(20[12]\d)\b', title + ' ' + notes)))
        rows.append({
            'iso3': iso3.upper(),
            'dataset': str(pkg.get('name', ''))[:70],
            'title': title[:110],
            'org': str((pkg.get('organization') or {}).get('title', ''))[:50],
            'years_in_title': ';'.join(years[-3:]),
            'metadata_modified': str(pkg.get('metadata_modified', ''))[:10],
            'data_period': str(pkg.get('dataset_date', '') or '')[:40],
            'n_resources': len(resources),
            'formats': ';'.join(fmts),
            # microdonnee = au moins une ressource dans un format de donnees
            'has_data_file': bool(data_res),
            # `private` ou une note d'acces = demande d'autorisation
            'access': ('private' if pkg.get('private') else
                       ('by-request' if re.search(r'by request|upon request|restricted',
                                                  notes, re.I) else 'open')),
            'wg_hint': bool(WG_HINT.search(blob)),
            'url': 'https://data.humdata.org/dataset/{}'.format(pkg.get('name', '')),
        })
    return rows


def main(argv):
    countries = [a.upper() for a in argv if not a.startswith('-')] or DEFAULT_COUNTRIES
    print('=' * 78)
    print('RECENSEMENT MSNA — {} pays (source : HDX CKAN, facette groups)'.format(
        len(countries)))
    print('=' * 78)
    all_rows = []
    for iso3 in countries:
        rows = []
        for coll in (census_reliefweb, census_dtm, census_country):
            try:
                rows.extend(coll(iso3))
            except Exception as e:
                print('  {} {}: {}'.format(iso3, coll.__name__, str(e)[:60]))
        for r in rows:              # les lignes HDX n'ont pas ces cles
            r.setdefault('channel', 'hdx')
            r.setdefault('org', '')
            r.setdefault('date', r.get('years_in_title', '') or
                         r.get('metadata_modified', ''))
        all_rows.extend(rows)
        with_data = sum(1 for r in rows if r['has_data_file'])
        wg = sum(1 for r in rows if r['wg_hint'])
        by = {}
        for r in rows:
            by[r['channel']] = by.get(r['channel'], 0) + 1
        latest = max([str(r.get('date', ''))[:10] for r in rows if r.get('date')],
                     default='?')
        flag = '' if rows else '   (rien sur les 3 canaux)'
        print('  {:5} {:>3} produits (rw {:>2} / dtm {:>2} / hdx {:>2}) | {:>2} fichiers '
              '| {:>2} WG-SS | + recent {}{}'.format(
                  iso3, len(rows), by.get('reliefweb', 0), by.get('dtm', 0),
                  by.get('hdx', 0), with_data, wg, latest, flag))
        time.sleep(0.3)

    print()
    print('=' * 78)
    print('SYNTHESE')
    print('=' * 78)
    n = len(all_rows)
    nd = sum(1 for r in all_rows if r['has_data_file'])
    nw = sum(1 for r in all_rows if r['wg_hint'])
    pays_avec = sorted({r['iso3'] for r in all_rows})
    pays_data = sorted({r['iso3'] for r in all_rows if r['has_data_file']})
    pays_wg = sorted({r['iso3'] for r in all_rows if r['wg_hint']})
    print('  datasets MSNA recenses            : {}'.format(n))
    print('  avec au moins un fichier de donnees: {}'.format(nd))
    print('  avec un INDICE de WG-SS            : {}'.format(nw))
    print()
    print('  pays avec une MSNA        ({:>2}) : {}'.format(len(pays_avec), ' '.join(pays_avec)))
    print('  pays avec de la donnee    ({:>2}) : {}'.format(len(pays_data), ' '.join(pays_data)))
    print('  pays avec indice WG-SS    ({:>2}) : {}'.format(len(pays_wg), ' '.join(pays_wg)))
    print()
    print('  ⚠ `wg_hint` est un INDICE tire du titre et des descriptions, PAS une')
    print('    confirmation : seule l\'ouverture du fichier tranche. Prochaine etape,')
    print('    sonder les colonnes des pays retenus (microdonnee dans C:\\tmp uniquement).')

    if all_rows:
        out = os.path.join(HERE, '..', 'analysis',
                           'MSNA_CENSUS_{}.csv'.format(time.strftime('%Y-%m-%d')))
        save_csv(all_rows, out)
        print()
        print('  ecrit : {}'.format(os.path.abspath(out)))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
