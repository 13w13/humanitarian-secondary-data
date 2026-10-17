"""
WorldPop (population maillee R2025A) : contrat du client
========================================================
    python -X utf8 scripts/test_worldpop.py
    python -X utf8 scripts/test_worldpop.py --offline      # sans reseau

Deux moities. HORS RESEAU : lecture d'une reponse du hub figee le 2026-10-17, noms
des fichiers age-sexe, et calcul sur un raster synthetique dont on connait la
reponse (rasterio requis, sinon SKIP). RESEAU : le vrai catalogue Soudan, la taille
annoncee, et le total national du raster 1 km (3 Mo) qui doit tomber dans
l'ordre de grandeur de l'UN WPP 2024. Une panne amont fait SKIP, pas FAIL.
"""
import os
import shutil
import sys
import tempfile
import time

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, 'clients'))

PASS, FAIL, SKIP = [], [], []

# Reponse du hub (champs utiles), figee le 2026-10-17 depuis
# /rest/data/pop/G2_CN_POP_R25A_100m?iso3=SDN et age_structures/G2_CN_Age_R25A_100m.
_U = 'https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/{y}/SDN/v1/100m/' \
     'constrained/sdn_pop_{y}_CN_100m_R2025A_v1.tif'
_A = 'https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2025A/2025/SDN/v1/' \
     '100m/constrained/sdn_{}_2025_CN_100m_R2025A_v1.tif'
FROZEN_POP = [
    {'iso3': 'SDN', 'country': 'Sudan', 'popyear': '2025', 'date': '2025-09-01',
     'doi': '10.5258/SOTON/WP00839', 'license': 'https://hub.worldpop.org/data/licence.txt',
     'citation': 'Bondarenko M. ... DOI:10.5258/SOTON/WP00839', 'files': [_U.format(y=2025)]},
    {'iso3': 'SDN', 'country': 'Sudan', 'popyear': '2015', 'date': '2025-09-01',
     'doi': '10.5258/SOTON/WP00839', 'license': 'https://hub.worldpop.org/data/licence.txt',
     'citation': 'x', 'files': [_U.format(y=2015)]},
    # Deux lignes qui ne doivent PAS sortir : un autre pays, une annee illisible.
    {'iso3': 'TCD', 'country': 'Chad', 'popyear': '2025', 'date': '2025-09-01',
     'files': ['https://data.worldpop.org/x/tcd_pop_2025_CN_100m_R2025A_v1.tif']},
    {'iso3': 'SDN', 'country': 'Sudan', 'popyear': 'n/a', 'date': '2025-09-01',
     'files': ['https://data.worldpop.org/x/bad.tif']},
]
FROZEN_AGE = [
    {'iso3': 'SDN', 'country': 'Sudan', 'popyear': '2025', 'date': '2025-09-01',
     'files': [_A.format(s) for s in ('f_80', 't_55', 'm_00', 'T_M', 'T_F', 'f_01')]},
]


def check(label, cond, detail=''):
    (PASS if cond else FAIL).append(label)
    print('  [{}] {} {}'.format('OK  ' if cond else 'FAIL', label, detail))
    return cond


def skip(label, why):
    SKIP.append(label)
    print('  [SKIP] {} ({})'.format(label, why))


def section(title):
    print()
    print('=' * 74)
    print(title)
    print('=' * 74)


# ── Hors reseau ──────────────────────────────────────────────

def t_parse():
    section('1. Catalogue fige : une ligne par fichier, rien d\'un autre pays')
    from worldpop_client import rows_from_records, freshness_from_records
    rows = rows_from_records(FROZEN_POP, 'SDN', 'population', '100m')
    check('2 lignes (TCD et annee illisible ecartes)', len(rows) == 2, str(len(rows)))
    check('triees par annee, annee entiere', [r['year'] for r in rows] == [2015, 2025])
    check('lien direct, licence et DOI portes par la ligne',
          all(r['download_url'].endswith('.tif') and r['license'] and r['doi']
              for r in rows))
    check('filtre years', [r['year'] for r in rows_from_records(
        FROZEN_POP, 'SDN', 'population', '100m', years=[2025])] == [2025])
    f = freshness_from_records(FROZEN_POP, 'SDN')
    check('fraicheur : annees et date de mise en ligne',
          (f['first_year'], f['last_year'], f['published']) == (2015, 2025, '2025-09-01'))
    check('fraicheur : les mises en garde voyagent avec', len(f['caveats']) >= 3 and
          any('deplacement' in c for c in f['caveats']))
    check('pays absent : dit, pas un vide muet',
          freshness_from_records([], 'PSE')['available'] is False)
    # Aucun fichier pour le pays : NotCovered (status not_covered), jamais « 0 ».
    from config import NotCovered
    from worldpop_client import WorldPopClient
    wp = WorldPopClient()
    wp._records = lambda *a, **k: []
    try:
        wp.list_files('PSE')
        check('aucun fichier -> NotCovered', False, 'liste vide rendue')
    except NotCovered:
        check('aucun fichier -> NotCovered', True)


def t_age_sex():
    section('2. Fichiers age-sexe : sexe et tranche lus dans le nom')
    from worldpop_client import parse_age_sex, rows_from_records, select_age_sex
    cases = {'sdn_f_80_2025_CN_100m_R2025A_v1.tif': {'sex': 'f', 'age': '80'},
             'sdn_t_55_2025_CN_100m_R2025A_v1.tif': {'sex': 't', 'age': '55'},
             'sdn_T_M_2025_CN_100m_R2025A_v1.tif': {'sex': 'm', 'age': 'all'},
             'sdn_under_age_18_2025_CN_100m_R2025A_v1.zip': {}}
    for name, want in cases.items():
        check('{} -> {}'.format(name[:12], want or 'rien'), parse_age_sex(name) == want)
    rows = rows_from_records(FROZEN_AGE, 'SDN', 'age_sex', '100m')
    check('6 fichiers', len(rows) == 6)
    got = select_age_sex(rows, 2025, sex='f', ages=[0, 1, 80])
    check('selection femmes 00/01/80', sorted(r['age'] for r in got) == ['01', '80'],
          str([r['age'] for r in got]))
    check('totaux par sexe', len(select_age_sex(rows, 2025, ages=['all'])) == 2)


def t_raster():
    section('3. Raster synthetique : la somme attendue, au centre de maille pres')
    try:
        import rasterio
        import numpy as np
        from rasterio.transform import from_origin
    except ImportError:
        skip('raster synthetique', 'rasterio absent (extra optionnel)')
        return
    from worldpop_client import raster_sum, population_near
    import math
    tmp = tempfile.mkdtemp(prefix='hsd_wp_')
    try:
        path = os.path.join(tmp, 'synth.tif')
        res, west, north, n = 0.01, 24.0, 14.0, 20
        a = np.full((n, n), 2.0, dtype='float32')
        a[0:3, 0:3] = -99999.0                      # 9 mailles nodata
        with rasterio.open(path, 'w', driver='GTiff', height=n, width=n, count=1,
                           dtype='float32', crs='EPSG:4326', nodata=-99999.0,
                           transform=from_origin(west, north, res, res)) as dst:
            dst.write(a, 1)
        tot = raster_sum(path)
        check('total = 2 x (400 - 9 nodata)', abs(tot['population'] - 782) < 1e-6,
              str(tot['population']))
        bb = raster_sum(path, bbox=(24.05, 13.85, 24.10, 13.95))
        # centres en x : 24.055 .. 24.095 (5), en y : 13.855 .. 13.945 (10)
        check('bbox : mailles dont le centre est dedans', bb['cells'] == 50,
              str(bb['cells']))
        lon, lat, r = 24.1, 13.9, 3.0
        kx = 111.32 * math.cos(math.radians(lat))
        want = sum(1 for i in range(n) for j in range(n)
                   if not (i < 3 and j < 3)
                   and math.hypot((west + (j + .5) * res - lon) * kx,
                                  (north - (i + .5) * res - lat) * 111.32) <= r)
        got = population_near(path, lon, lat, r)
        check('rayon 3 km : meme compte qu\'un calcul independant',
              got['cells'] == want and abs(got['population'] - 2 * want) < 1e-6,
              '{} mailles'.format(got['cells']))
        out = population_near(path, 0.0, 0.0, 5)
        check('point hors emprise : zero et une note', out['population'] == 0
              and 'note' in out)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def t_download_guard():
    section('4. Telechargement : schema refuse avant tout acces')
    from worldpop_client import WorldPopClient
    try:
        WorldPopClient().download('file:///C:/Windows/win.ini', tempfile.gettempdir())
        check('file:// refuse', False, 'accepte a tort')
    except ValueError:
        check('file:// refuse', True)


# ── Reseau ───────────────────────────────────────────────────

def t_live():
    section('5. Hub en direct : Soudan, 16 annees, fichiers sur data.worldpop.org')
    from worldpop_client import WorldPopClient
    wp = WorldPopClient(timeout=60)
    rows = wp.list_files('SDN')
    years = [r['year'] for r in rows]
    check('2015 a 2030, une ligne par annee', years == list(range(2015, 2031)),
          '{}..{} ({})'.format(years[:1], years[-1:], len(years)))
    check('liens directs data.worldpop.org',
          all(r['download_url'].startswith('https://data.worldpop.org/') for r in rows))
    f = wp.freshness('SDN')
    check('millesime R2025A publie', f['available'] and f['published'][:4] >= '2025',
          f.get('published', ''))
    one = wp.list_files('SDN', resolution='1km', years=[2025])
    size = wp.file_size(one[0]['download_url']) if one else None
    check('taille annoncee du 1 km (HEAD)', size and 5e5 < size < 5e7,
          '{:,} o'.format(size or 0))
    return wp, one


def t_live_total(wp, one):
    section('6. Total national du 1 km 2025 : ordre de grandeur UN WPP 2024')
    import importlib.util
    if importlib.util.find_spec('rasterio') is None:
        skip('total national', 'rasterio absent (extra optionnel)')
        return
    from worldpop_client import raster_sum
    tmp = tempfile.mkdtemp(prefix='hsd_wp_')
    try:
        path = wp.download(one[0], tmp)
        tot = raster_sum(path)['population']
        # WPP 2024 : Soudan ~50-52 M au 1er janvier 2025. Large fourchette : on teste
        # qu'on lit bien des habitants (pas des densites, pas une valeur nodata).
        check('entre 40 et 65 millions', 40e6 < tot < 65e6, '{:,.0f}'.format(tot))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    from config import is_outage
    t0 = time.time()
    for fn in (t_parse, t_age_sex, t_raster, t_download_guard):
        try:
            fn()
        except Exception as e:
            FAIL.append(fn.__name__)
            print('  [FAIL] {} a leve : {}: {}'.format(fn.__name__, type(e).__name__,
                                                       str(e)[:110]))
    if '--offline' not in argv:
        try:
            wp, one = t_live()
            t_live_total(wp, one)
        except Exception as e:
            if is_outage(e):
                skip('reseau', 'panne amont : {}: {}'.format(type(e).__name__, str(e)[:70]))
            else:
                FAIL.append('reseau')
                print('  [FAIL] reseau : {}: {}'.format(type(e).__name__, str(e)[:110]))
    print()
    print('=' * 74)
    print('{} OK / {} FAIL / {} SKIP en {:.0f} s'.format(
        len(PASS), len(FAIL), len(SKIP), time.time() - t0))
    if FAIL:
        print('ECHECS : {}'.format(', '.join(FAIL)))
    return 1 if FAIL else 0


if __name__ == '__main__':
    sys.exit(main())
