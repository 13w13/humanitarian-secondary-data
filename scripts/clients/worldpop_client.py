"""
WorldPop Client (population maillee, Global 2 R2025A)
=====================================================
Population par maille de 100 m (ou 1 km), une couche par annee 2015-2030, totale
ou par sexe et tranche d'age. API REST publique, sans cle ; fichiers GeoTIFF sur
data.worldpop.org ; licence CC BY 4.0.

Usage:
    from worldpop_client import WorldPopClient, population_near
    wp = WorldPopClient()
    wp.freshness('SDN')                                   # millesime, annees, licence
    rows = wp.list_files('SDN')                           # 16 lignes, une par annee
    path = wp.download(rows[10], dest_dir)                # 2025, 54 Mo au Soudan (100 m)
    population_near(path, lon=24.9, lat=14.1, radius_km=5)   # rasterio requis

CE QUE CETTE COUCHE N'EST PAS (a dire a chaque usage) :
- Aucune annee n'est observee. Le modele repartit des recensements c.2010 et c.2020
  (projections du US Census Bureau ou COD de l'ONU pour les pays en conflit) sur le
  bati GHSL (epoques 2015, 2020, 2025, 2030, interpolees) et cale chaque total
  national sur les estimations au 1er janvier de l'UN WPP 2024, qui sont des
  projections a partir de 2024. L'incertitude croit avec l'ecart aux recensements.
- Elle ne voit PAS le deplacement force : la note de version dit que les donnees ne
  captent ni les deplacements dus aux conflits ni la mobilite saisonniere. Au Soudan,
  la carte ne montre pas ou les gens sont alles apres avril 2023 ; DTM reste la
  reference pour les deplaces.
- Somme de controle : 100 m et 1 km ont le meme total national (Soudan 2025 :
  50 849 352, mesure le 2026-10-17), cale sur l'UN WPP 2024.
- Version "constrained" : personne n'est place hors du bati detecte (GHSL, Google
  Open Buildings v3, Microsoft Building Footprints 2023). Un camp apparu apres ces
  empreintes peut ressortir vide ou sous-peuple.
- Alpha (R2025A) : la note de version previent que les valeurs peuvent changer.

Citation exigee (CC BY 4.0) : champ `citation` de chaque ligne, avec le DOI.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
import math
import os
import re
from urllib.request import Request, urlopen

from config import (
    WORLDPOP_BASE, WORLDPOP_RELEASE, DEFAULT_TIMEOUT, USER_AGENT, NotCovered,
    download_stream, normalize_iso3, raise_unavailable, require_module,
    safe_filename, save_csv,
)

# (produit, resolution) -> (alias, sous-alias) du hub. Lu dans /rest/data le 2026-10-17.
PRODUCTS = {
    ('population', '100m'): ('pop', 'G2_CN_POP_R25A_100m'),
    ('population', '1km'): ('pop', 'G2_CN_POP_R25A_1km'),
    ('age_sex', '100m'): ('age_structures', 'G2_CN_Age_R25A_100m'),
    ('age_sex', '1km'): ('age_structures', 'G2_CN_Age_R25A_1km'),
    ('under18', '100m'): ('age_structures', 'G2_Age_U18_R25A_100m'),
}

CAVEATS = [
    'Annees modelisees, pas observees : recensements c.2010/c.2020 repartis sur le bati, '
    'totaux nationaux cales sur UN WPP 2024 (projections a partir de 2024).',
    'Ne mesure pas le deplacement force (note de version WorldPop) : pour les deplaces, DTM.',
    'Version "constrained" : population placee seulement sur le bati detecte ; un site '
    'recent peut ressortir vide.',
    'R2025A = version alpha, valeurs susceptibles de changer.',
]

# Une ligne du catalogue age-sexe : sdn_f_80_2030_..., sdn_t_55_..., sdn_T_M_2030_...
_AGESEX = re.compile(r'^[a-z]{3}_(?:(?P<sex>[fmt])_(?P<age>\d{2})|T_(?P<tot>[MF]))_'
                     r'(?P<year>\d{4})_', re.I)

MAX_DOWNLOAD_BYTES = 1024 * 1024 * 1024     # 1 Go : le 100 m des plus grands pays


def parse_age_sex(file_name):
    """Sexe et tranche d'age lus dans le nom d'un fichier age-sexe.

    Rend {'sex': 'f'|'m'|'t', 'age': '00'..'90' ou 'all'} ; 't' = deux sexes. Les
    tranches sont 00 (moins d'un an), 01 (1-4 ans), puis 05, 10 ... 90 (90 et plus).
    Rend {} pour un nom hors convention plutot que de deviner.
    """
    m = _AGESEX.match(os.path.basename(file_name or ''))
    if not m:
        return {}
    if m.group('tot'):
        return {'sex': m.group('tot').lower(), 'age': 'all'}
    return {'sex': m.group('sex').lower(), 'age': m.group('age')}


class WorldPopClient:
    """Client du hub WorldPop (public, sans authentification)."""

    def __init__(self, timeout=DEFAULT_TIMEOUT):
        self.base = WORLDPOP_BASE
        self.timeout = timeout

    def _get(self, path, params=''):
        url = '{}/{}{}'.format(self.base, path, params)
        req = Request(url, headers={'User-Agent': USER_AGENT})
        try:
            return json.loads(urlopen(req, timeout=self.timeout).read())
        except Exception as e:
            # Une panne leve SourceUnavailable, jamais une liste vide.
            raise_unavailable('WorldPop {}'.format(path), e)

    def _records(self, iso3, product, resolution):
        key = (product, resolution)
        if key not in PRODUCTS:
            raise ValueError('Produit inconnu {}. Disponibles : {}'.format(
                key, ', '.join('{}/{}'.format(p, r) for p, r in PRODUCTS)))
        alias, sub = PRODUCTS[key]
        data = self._get('{}/{}'.format(alias, sub), '?iso3={}'.format(iso3))
        return (data or {}).get('data') or []

    def list_files(self, iso3, product='population', resolution='100m', years=None):
        """Une ligne par fichier telechargeable, triee par annee.

        `population` : un fichier par annee. `age_sex` : 62 par annee (20 tranches x
        f/m/t, plus les totaux par sexe), d'ou `select_age_sex` pour choisir.
        """
        iso3 = normalize_iso3(iso3)
        rows = rows_from_records(self._records(iso3, product, resolution), iso3,
                                 product, resolution, years)
        if not rows and not years:
            # Pas de fichier = pays non couvert, dit comme tel (jamais un « 0 »).
            raise NotCovered('WorldPop {} : aucun fichier {}/{} pour {}'.format(
                WORLDPOP_RELEASE, product, resolution, iso3))
        return rows

    def freshness(self, iso3, product='population', resolution='100m'):
        """Ce que dit la couche, et de quand. Un seul appel, rien ecrit.

        `published` est la date de mise en ligne du millesime. Les annees 2015-2030
        ne sont pas des dates d'observation : voir CAVEATS.
        """
        iso3 = normalize_iso3(iso3)
        return freshness_from_records(self._records(iso3, product, resolution), iso3)

    def file_size(self, url):
        """Taille annoncee (octets) par une requete HEAD, ou None."""
        req = Request(url, method='HEAD', headers={'User-Agent': USER_AGENT})
        try:
            with urlopen(req, timeout=self.timeout) as resp:
                got = resp.headers.get('Content-Length')
            return int(got) if got and got.isdigit() else None
        except Exception:
            return None                     # information seulement, jamais bloquant

    def download(self, row_or_url, dest_dir, max_bytes=MAX_DOWNLOAD_BYTES):
        """Telecharge un fichier ; rend son chemin.

        Passe par `config.download_stream` : http(s) seulement, redirections
        controlees, plafond, ecriture atomique, fichier tronque ou page HTML refuses,
        panne levee en SourceUnavailable. Le serveur ignore les requetes partielles
        (Range rend 200, constate le 2026-10-17) : on telecharge le fichier entier.
        """
        url = row_or_url['download_url'] if isinstance(row_or_url, dict) else row_or_url
        if not str(url).lower().startswith(('http://', 'https://')):
            raise ValueError('schema refuse (http/https seulement) : {}'.format(str(url)[:80]))
        # Le nom vient d'une reponse d'API : un seul composant de chemin, sans '..'.
        name = safe_filename(os.path.basename(str(url).split('?')[0]),
                             default='worldpop.tif')
        dest = os.path.join(dest_dir, name)
        if os.path.exists(dest):
            return dest
        os.makedirs(dest_dir, exist_ok=True)
        download_stream(url, dest, timeout=300, max_bytes=max_bytes)
        return dest

    @staticmethod
    def save_csv(records, filepath):
        save_csv(records, filepath)


def rows_from_records(records, iso3, product, resolution, years=None):
    """Aplatit la reponse du hub en lignes (une par fichier). Sans reseau : testable."""
    wanted = {int(y) for y in years} if years else None
    rows = []
    for rec in records:
        # Defensif : ne pas etiqueter au pays demande une ligne d'un autre pays.
        if str(rec.get('iso3', '')).upper() != iso3:
            continue
        try:
            year = int(rec.get('popyear'))
        except (TypeError, ValueError):
            continue
        if wanted and year not in wanted:
            continue
        for url in rec.get('files') or []:
            row = {
                'iso3': iso3,
                'country': rec.get('country', ''),
                'product': product,
                'resolution': resolution,
                'release': WORLDPOP_RELEASE,
                'year': year,
                'published': rec.get('date', ''),
                'file_name': os.path.basename(url),
                'download_url': url,
                'doi': rec.get('doi', ''),
                'license': rec.get('license', ''),
                'citation': rec.get('citation', ''),
                'summary_url': rec.get('url_summary', ''),
            }
            if product == 'age_sex':
                # Colonnes presentes seulement ici : vides sur le produit total,
                # elles feraient lever l'alerte « colonne vide a 100 % » de save_csv.
                row.update({'sex': '', 'age': ''}, **parse_age_sex(url))
            rows.append(row)
    return sorted(rows, key=lambda r: (r['year'], r.get('sex', ''), r.get('age', '')))


def freshness_from_records(records, iso3):
    years = sorted({int(r['popyear']) for r in records
                    if str(r.get('iso3', '')).upper() == iso3
                    and str(r.get('popyear', '')).isdigit()})
    if not years:
        return {'iso3': iso3, 'available': False, 'release': WORLDPOP_RELEASE,
                'note': 'aucun fichier {} pour ce pays'.format(WORLDPOP_RELEASE)}
    dates = sorted({r.get('date', '') for r in records if r.get('date')})
    first = records[0]
    return {
        'iso3': iso3, 'available': True, 'release': WORLDPOP_RELEASE,
        'published': dates[-1] if dates else '',
        'first_year': years[0], 'last_year': years[-1], 'n_years': len(years),
        'doi': first.get('doi', ''), 'license': first.get('license', ''),
        'caveats': list(CAVEATS),
    }


def select_age_sex(rows, year, sex=None, ages=None):
    """Filtre les lignes age-sexe : `sex` in f/m/t, `ages` = liste de '00'..'90' ou 'all'."""
    ages = {str(a).zfill(2) if str(a).isdigit() else str(a) for a in ages} if ages else None
    return [r for r in rows if r['year'] == int(year)
            and (sex is None or r['sex'] == sex)
            and (ages is None or r['age'] in ages)]


# ── Lecture des rasters (extra optionnel : rasterio) ─────────────────────────

def _open(path):
    rasterio = require_module('rasterio', 'La lecture des rasters WorldPop')
    np = require_module('numpy', 'La lecture des rasters WorldPop')
    return rasterio, np, rasterio.open(path)


def _cells(src, np, bbox):
    """Mailles de la fenetre qui couvre `bbox` (ouest, sud, est, nord).

    Rend (valeurs masquees, x des centres, y des centres), ou None si la bbox est
    hors du raster. Fenetre calculee a la main, arrondie vers l'exterieur : les
    arrondis de `windows.from_bounds` changent selon la version de rasterio et
    peuvent perdre la maille du bord.
    """
    from_window = __import__('rasterio.windows', fromlist=['Window']).Window
    t = src.transform                      # nord en haut : t.e < 0
    w, s, e, n = bbox
    c0 = max(int(math.floor((w - t.c) / t.a)), 0)
    c1 = min(int(math.ceil((e - t.c) / t.a)), src.width)
    r0 = max(int(math.floor((n - t.f) / t.e)), 0)
    r1 = min(int(math.ceil((s - t.f) / t.e)), src.height)
    if c0 >= c1 or r0 >= r1:
        return None
    a = src.read(1, window=from_window(c0, r0, c1 - c0, r1 - r0), masked=True)
    rows, cols = np.indices(a.shape)
    xs = t.c + (c0 + cols + 0.5) * t.a
    ys = t.f + (r0 + rows + 0.5) * t.e
    return a, xs, ys


def raster_sum(path, bbox=None):
    """Habitants d'un raster entier, ou des mailles dont le centre est dans `bbox`.

    `bbox` = (ouest, sud, est, nord) en degres. Sans bbox, lecture bloc par bloc :
    le 100 m du Soudan fait ~340 millions de mailles, il ne tient pas en memoire
    d'un coup. Les mailles nodata sont exclues.
    """
    rasterio, np, src = _open(path)
    with src:
        total, cells = 0.0, 0
        if bbox is None:
            for _, win in src.block_windows(1):
                a = src.read(1, window=win, masked=True)
                if a.count():
                    total += float(a.sum(dtype='float64'))
                    cells += int(a.count())
        else:
            got = _cells(src, np, bbox)
            if got:
                a, xs, ys = got
                w, s, e, n = bbox
                inside = (xs >= w) & (xs <= e) & (ys >= s) & (ys <= n)
                keep = inside & ~np.ma.getmaskarray(a)
                total = float(a.data[keep].sum(dtype='float64'))
                cells = int(keep.sum())
        return {'population': total, 'cells': cells, 'bbox': bbox,
                'resolution_deg': src.res[0], 'file': os.path.basename(path)}


def population_near(path, lon, lat, radius_km):
    """Habitants des mailles dont le centre est a moins de `radius_km` du point.

    Distance equirectangulaire (erreur negligeable sous ~50 km). Pour un
    denominateur de couverture autour d'un site : citer le rayon, l'annee et la
    version, et rappeler que la couche ne voit pas les deplaces (CAVEATS).
    """
    rasterio, np, src = _open(path)
    kx = 111.32 * max(math.cos(math.radians(lat)), 1e-6)    # km par degre de longitude
    dlat, dlon = radius_km / 111.32, radius_km / kx
    with src:
        got = _cells(src, np, (lon - dlon, lat - dlat, lon + dlon, lat + dlat))
        if not got:
            return {'population': 0.0, 'cells': 0, 'radius_km': radius_km,
                    'lon': lon, 'lat': lat, 'note': "point hors de l'emprise du raster"}
        a, xs, ys = got
        d = np.hypot((xs - lon) * kx, (ys - lat) * 111.32)
        keep = (d <= radius_km) & ~np.ma.getmaskarray(a)
        return {'population': float(a.data[keep].sum(dtype='float64')),
                'cells': int(keep.sum()), 'radius_km': radius_km, 'lon': lon,
                'lat': lat, 'file': os.path.basename(path)}


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    iso3 = sys.argv[1] if len(sys.argv) > 1 else 'SDN'
    wp = WorldPopClient()
    f = wp.freshness(iso3)
    print('=== WorldPop {} — {} ==='.format(f['release'], f['iso3']))
    if not f['available']:
        print(f['note'])
        sys.exit(0)
    print('publie {} | annees {}-{} | {}'.format(
        f['published'], f['first_year'], f['last_year'], f['license']))
    rows = wp.list_files(iso3, years=[2025])
    for r in rows:
        size = wp.file_size(r['download_url'])
        print('  {} {} ({} Mo)'.format(r['year'], r['file_name'],
                                       round(size / 1e6, 1) if size else '?'))
    for c in CAVEATS:
        print('  - ' + c)
