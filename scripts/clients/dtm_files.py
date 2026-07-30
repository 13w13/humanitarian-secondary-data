"""
DTM Files — lire un fichier DTM téléchargé, et valider sa lecture
=================================================================
Entre « télécharger le fichier » et « répondre à la question », il manquait un
lecteur. Les fichiers DTM ne se lisent pas avec `read_excel(path)` :

  - la **première feuille est souvent un « Read Me »** (disclaimer). `sheet=0` rend
    une colonne `DISCLAIMER` et 17 lignes. Vécu sur le Soudan.
  - l'**en-tête est sur plusieurs lignes** : bandeau de titre en r1, groupes fusionnés
    en r3, vrais noms de colonnes en **r4**, tags **HXL en r5**, données à partir de
    r6. `header=0` rend le bandeau.
  - certains fichiers sont au contraire une **série longue mono-feuille** avec
    l'en-tête en r1 (Flow Monitoring Counting : 12 242 × 10).

D'où `describe()` qui dit la forme, et `read_sheet()` qui détecte l'en-tête.

La somme de contrôle
--------------------
Un rapport DTM est le **rendu** du fichier. Reproduire un chiffre publié valide donc
la lecture (bonne feuille, bonne ligne d'en-tête, bonne colonne, bon filtre) AVANT de
calculer autre chose. Vérifié 2026-07-25 sur l'Afghanistan, 4 fois sur 4 :

    somme(Total Headcount, Direction=Inflow, 05→18 juil 2026)  = 120 099  = publié
    somme(..., Direction=Outflow)                              =  38 979  = publié
    période précédente (21 juin→04 juil)                       =  91 492  = 120 099 − 28 607
    idem sorties                                               =  32 940  =  38 979 −  6 039

Un écart n'est pas une donnée, c'est une erreur de lecture à résoudre avant de citer.

Usage :
    from dtm_files import describe, read_sheet, sum_by, checksum

    print(describe(path))                       # feuilles, en-tête, HXL, colonnes
    cols, hxl, recs = read_sheet(path)          # ignore le « Read Me », trouve r4/r5
    total = sum_by(recs, 'Total Headcount',
                   {'Direction': 'Inflow'}, date_col='ReportingDate',
                   date_from='2026-07-05', date_to='2026-07-18')
    checksum(total, 120099, 'TOTAL INFLOWS 05-18 Jul')   # -> True si EXACT
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import datetime as _dt
import os
import re

# Feuilles à ne jamais prendre pour la donnée. Liste établie sur 6 fichiers réels,
# un par famille de produit (2026-07-25) : au-delà du « Read Me » soudanais, on trouve
# des tableaux croisés (« Analysis », « Summary », « Shelter Type », « Period of
# Displacement »), de la doc (« Methodology », « Definitions ») et des annexes
# (« Contacts & Partners », « Admin2 », « admin2_new » = tables de référence pcodes).
_NON_DATA_SHEET = re.compile(
    r'^\s*(read\s*me|readme|disclaimer|notes?|about|methodology|method|definitions?|'
    r'limitations?|cover|contents?|index|analysis|summary.*|contacts?.*|partners?|'
    r'admin\d.*|dashboard|pivot|charts?|graphs?|glossary|codebook)\s*$', re.I)
# Colonnes dont la somme n'a pas de sens (part, taux, moyenne, score).
_NOT_SUMMABLE = re.compile(r'(%|percent|pct|share|rate|ratio|average|mean|median|'
                           r'score|index)', re.I)


def _cells(row):
    """Cellules en texte, espaces normalisés.

    Les noms de colonnes DTM contiennent des retours à la ligne
    (`'Location name\\nin English'`) : sans normalisation, toute recherche de colonne
    par son nom échoue.
    """
    return [re.sub(r'\s+', ' ', '' if c is None else str(c)).strip() for c in row]


def _pick_data_sheet(wb):
    """Feuille de données choisie par la FORME, pas par le nom ni la position.

    Mesuré sur 6 fichiers réels (2026-07-25) : la donnée n'est presque jamais la
    première feuille. Iraq Master List → la donnée est `DTM Dataset` (2 557 × 61)
    alors que la 1re feuille `Round 133` fait 35 × 14 avec des colonnes vides ; Yemen
    RDT → la donnée est `IDPs` (874 × 46) alors que la 1re feuille `Analysis` est un
    tableau croisé de 33 lignes. Prendre la 1re feuille, ou la 1re dont le nom n'est
    pas « Read Me », donne le mauvais tableau dans les deux cas.

    On garde donc la feuille la plus volumineuse (lignes d'abord, colonnes ensuite)
    parmi celles dont le nom n'est pas manifestement méta.
    """
    cands = [(n, wb[n].max_row or 0, wb[n].max_column or 0) for n in wb.sheetnames]
    keep = [c for c in cands if not _NON_DATA_SHEET.match(c[0])]
    pool = keep or cands
    best = max(pool, key=lambda c: (c[1], c[2]))
    why = {'candidates': [(c[0], c[1], c[2]) for c in cands],
           'chosen_because': 'plus grand volume ({} lignes x {} colonnes) parmi {} '
                             'feuille(s) non-meta'.format(best[1], best[2], len(pool))}
    # La liste des noms « méta » a été établie sur 6 fichiers : elle ne peut pas être
    # exhaustive. Si une feuille ÉCARTÉE est nettement plus grosse que celle retenue,
    # le nom a peut-être menti (un vrai jeu de données appelé « Summary of ... »).
    # On garde le choix mais on le rend VISIBLE au lieu de le taire.
    biggest = max(cands, key=lambda c: (c[1], c[2]))
    if biggest[0] != best[0] and biggest[1] > max(1, best[1]) * 3:
        why['warning'] = (
            'la feuille écartée {!r} est bien plus grosse ({} lignes contre {}) : '
            'son nom l\'a fait passer pour une feuille méta. Vérifier, et au besoin '
            'passer sheet={!r} explicitement.'.format(
                biggest[0], biggest[1], best[1], biggest[0]))
        print('  dtm_files: {}'.format(why['warning']))
    return best[0], why


def _load(path, sheet=None, max_row=None):
    from config import require_module
    openpyxl = require_module('openpyxl', 'Reading a downloaded workbook')
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    names = wb.sheetnames
    why = None
    if sheet is None:
        sheet, why = _pick_data_sheet(wb)
    elif isinstance(sheet, int):
        sheet = names[sheet]
    ws = wb[sheet]
    return wb, ws, sheet, names, why


def _detect_header(rows):
    """Ligne d'en-tête (1-indexée) et ligne HXL si présente.

    L'en-tête est la ligne la plus « textuelle » du haut du fichier : beaucoup de
    cellules courtes contenant des lettres. On pénalise la profondeur pour préférer
    la première ligne plausible, et on ignore les bandeaux d'une seule cellule.
    """
    hxl = None
    for i, r in enumerate(rows):
        cells = [c for c in r if c]
        if cells and sum(1 for c in cells if c.startswith('#')) >= max(2, len(cells) * 0.5):
            hxl = i + 1
            break
    best, best_score = 0, -1e9
    for i, r in enumerate(rows):
        if hxl and i + 1 >= hxl:      # l'en-tête précède toujours la ligne HXL
            break
        cells = [c for c in r if c]
        if len(cells) < 2:
            continue
        texty = sum(1 for c in cells if re.search(r'[A-Za-z]', c) and len(c) < 60)
        score = texty + len(cells) * 0.5 - i * 0.3
        if score > best_score:
            best, best_score = i, score
    return best + 1, hxl


def describe(path, peek=14):
    """Forme réelle d'un fichier DTM, pour ne pas lire à l'aveugle."""
    wb, _ws, chosen, names, why = _load(path)
    out = {'file': os.path.basename(path), 'bytes': os.path.getsize(path),
           'sheets': [], 'data_sheet': chosen, 'why_data_sheet': why,
           'skipped_sheets': [n for n in names if _NON_DATA_SHEET.match(n)]}
    for name in names:
        ws = wb[name]
        rows = [_cells(r) for i, r in enumerate(ws.iter_rows(max_row=peek, values_only=True))]
        hdr, hxl = _detect_header(rows)
        out['sheets'].append({
            'sheet': name,
            'is_chosen': name == chosen,
            'rows': ws.max_row, 'cols': ws.max_column,
            'is_data': not bool(_NON_DATA_SHEET.match(name)),
            'header_row': hdr, 'hxl_row': hxl,
            'columns': [c for c in rows[hdr - 1] if c] if len(rows) >= hdr else [],
            'hxl_tags': [c for c in rows[hxl - 1] if c] if hxl and len(rows) >= hxl else [],
        })
    wb.close()
    return out


def read_sheet(path, sheet=None):
    """(colonnes, tags HXL, enregistrements) d'une feuille de données.

    Ignore les feuilles « Read Me » quand `sheet` n'est pas précisé, détecte la ligne
    d'en-tête et saute la ligne HXL. Les noms de colonnes dupliqués sont suffixés
    `_2`, `_3` : les masterlists DTM répètent des libellés dans la matrice
    origine × déplacement, et un dict écraserait silencieusement les colonnes.
    """
    wb, ws, name, _names, _why = _load(path, sheet)
    rows = [_cells(r) for r in ws.iter_rows(values_only=True)]
    wb.close()
    if not rows:
        return [], [], []
    hdr, hxl = _detect_header(rows[:14])
    raw = rows[hdr - 1]
    cols, seen = [], {}
    for j, c in enumerate(raw):
        c = c or 'col_{}'.format(j + 1)
        if c in seen:
            seen[c] += 1
            c = '{}_{}'.format(c, seen[c])
        else:
            seen[c] = 1
        cols.append(c)
    tags = rows[hxl - 1] if hxl and len(rows) >= hxl else []
    start = (hxl if hxl else hdr)          # 0-indexé sur la ligne suivante
    recs = []
    for r in rows[start:]:
        if not any(x for x in r):
            continue
        recs.append({cols[j]: (r[j] if j < len(r) else '') for j in range(len(cols))})
    return cols, tags, recs


def _as_date(v):
    if isinstance(v, _dt.datetime):
        return v.date()
    if isinstance(v, _dt.date):
        return v
    s = str(v or '').strip()
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', s)
    if m:
        return _dt.date(*(int(x) for x in m.groups()))
    for fmt in ('%d/%m/%Y', '%m/%d/%Y', '%d-%b-%Y', '%b %d, %Y', '%d %B %Y'):
        try:
            return _dt.datetime.strptime(s[:24], fmt).date()
        except ValueError:
            continue
    return None


def _num(v):
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v or '').replace(',', '').replace(' ', '').strip()
    if s in ('', '-', 'n/a', 'N/A', 'NA', '..'):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def find_column(cols, records, pattern, min_numeric_share=0.6):
    """Trouve une colonne par motif, en exigeant qu'elle soit NUMÉRIQUE.

    La leçon vient d'un test raté (Yémen RDT, 2026-07-25) : le motif 'household'
    matche AUSSI la colonne question « How do households in the community mostly
    access food? » (des réponses texte), et la somme rendait 0 sur 0 ligne. Un motif
    seul ne suffit pas : parmi les candidates, on garde celle dont les valeurs sont
    effectivement des nombres (part mesurée sur les 120 premières lignes).

    Retourne (nom_de_colonne, part_numérique) ou (None, 0.0) si aucune candidate
    n'atteint `min_numeric_share` — auquel cas l'appelant doit le DIRE, pas deviner.
    """
    rx = re.compile(pattern, re.I)
    cands = [c for c in cols if rx.search(c)]
    if not cands:
        return None, 0.0
    def share(col):
        vals = [_num(r.get(col)) for r in records[:120]]
        return sum(1 for v in vals if v is not None) / max(1, len(vals))
    best = max(cands, key=share)
    s = share(best)
    if s < min_numeric_share:
        return None, s
    return best, s


def sum_by(records, value_col, filters=None, date_col=None, date_from=None,
           date_to=None, group_by=None, strict=True):
    """Somme d'une colonne, avec les garde-fous qui manquaient.

    - refuse de sommer une colonne de pourcentage / taux / score (`strict=True`) :
      c'est le bug ACAPS (un score 0-10 pris pour un effectif) rejoué sur un fichier.
    - renvoie aussi le nombre de lignes retenues et de valeurs illisibles : une somme
      sur 0 ligne doit se voir, pas passer pour un zéro.
    - `group_by` rend un dict trié par valeur décroissante.
    """
    if strict and _NOT_SUMMABLE.search(value_col or ''):
        raise ValueError(
            '{!r} ressemble à une part ou un score : sommer n\'a pas de sens. '
            'Passer strict=False pour forcer.'.format(value_col))
    a = _as_date(date_from) if date_from else None
    b = _as_date(date_to) if date_to else None
    groups, kept, unreadable = {}, 0, 0
    for r in records:
        if filters:
            skip = False
            for k, v in filters.items():
                cur = str(r.get(k, '')).strip()
                ok = (cur in v) if isinstance(v, (list, tuple, set)) else (cur == str(v))
                if not ok:
                    skip = True
                    break
            if skip:
                continue
        if date_col and (a or b):
            x = _as_date(r.get(date_col))
            if not x or (a and x < a) or (b and x > b):
                continue
        val = _num(r.get(value_col))
        if val is None:
            unreadable += 1
            continue
        key = str(r.get(group_by, '')).strip() if group_by else '__total__'
        groups[key] = groups.get(key, 0.0) + val
        kept += 1
    result = {'rows': kept, 'unreadable': unreadable,
              'total': sum(groups.values()) if groups else 0.0}
    if group_by:
        result['groups'] = dict(sorted(groups.items(), key=lambda kv: -kv[1]))
    if kept == 0:
        result['warning'] = ('0 ligne retenue : vérifier les filtres et la période, '
                             'ne pas présenter ce 0 comme une valeur')
    return result


def checksum(computed, published, label='', tol=0.0):
    """Le calcul reproduit-il le chiffre publié ?

    À faire AVANT toute autre agrégation : c'est ce qui valide qu'on lit la bonne
    feuille, la bonne colonne et la bonne période. Un écart est une erreur de
    lecture, pas une donnée.
    """
    c = _num(computed if not isinstance(computed, dict) else computed.get('total'))
    p = _num(published)
    if c is None or p is None:
        print('  [?]     {:36} valeur illisible (calcule={!r}, publie={!r})'.format(
            label[:36], computed, published))
        return False
    diff = c - p
    ok = abs(diff) <= max(tol, 0.5)
    pct = (diff / p * 100) if p else 0.0
    print('  [{}] {:36} calcule={:>14,.0f} | publie={:>14,.0f}{}'.format(
        'EXACT' if ok else ' ECART', label[:36], c, p,
        '' if ok else '  ecart {:+,.0f} ({:+.2f} %)'.format(diff, pct)))
    return ok


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('usage: dtm_files.py <fichier.xlsx> [feuille]')
        sys.exit(1)
    info = describe(sys.argv[1])
    print('{} ({:,} octets)'.format(info['file'], info['bytes']))
    if info['skipped_sheets']:
        print('  feuilles ignorees (non-donnee) :', info['skipped_sheets'])
    for s in info['sheets']:
        print('  {} {!r} {}x{} | en-tete r{} | HXL r{}'.format(
            '>>>>' if s['is_chosen'] else ('data' if s['is_data'] else 'skip'),
            s['sheet'][:38], s['rows'], s['cols'], s['header_row'], s['hxl_row']))
        print('      colonnes : {}'.format(', '.join(s['columns'][:12])[:170]))
        if s['hxl_tags']:
            print('      HXL      : {}'.format(', '.join(s['hxl_tags'][:12])[:170]))
    print('  FEUILLE RETENUE : {!r} ({})'.format(
        info['data_sheet'], (info['why_data_sheet'] or {}).get('chosen_because', '')))
    cols, tags, recs = read_sheet(sys.argv[1],
                                 sys.argv[2] if len(sys.argv) > 2 else None)
    print('  -> {} enregistrements, {} colonnes'.format(len(recs), len(cols)))
    print('  -> colonnes : {}'.format(', '.join(cols[:14])[:200]))
