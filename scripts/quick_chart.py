"""
Graphique rapide depuis un fichier DTM — le parcours de l'IM qui investigue
===========================================================================
    python -X utf8 scripts/quick_chart.py <fichier.xlsx> [--out chart.png]

Demonstration du parcours complet : fichier -> lecture sure (dtm_files) ->
agregation -> graphique aux couleurs HI. Matplotlib est la SEULE dependance hors
stdlib, et uniquement pour ce script (les clients restent stdlib).

Ce que le script decide tout seul, et dit :
  - la feuille de donnees (par la forme, pas la position)
  - la colonne de date, la colonne de valeur (motif + part numerique)
  - l'agregation hebdomadaire si la serie est quotidienne

Regle : le titre du graphique porte la SOURCE et la PERIODE. Un graphique sans
millesime n'est pas presentable (grille HI).
"""
import sys
import os
import datetime as dt

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, 'clients'))

from dtm_files import describe, read_sheet, find_column, _as_date, _num   # noqa: E402

# Palette HI (charte 2022)
HI_BLUE = '#0077C8'
HI_MINERAL = '#002E43'
HI_AZUR = '#5BC2E7'
HI_ORANGE = '#D34607'
HI_BG = '#FFFFFF'


def weekly(records, date_col, value_col, group_col=None, group_val=None):
    """Somme hebdomadaire (lundi) d'une colonne, filtrable sur une modalite."""
    buckets = {}
    for r in records:
        if group_col and str(r.get(group_col, '')).strip() != group_val:
            continue
        d = _as_date(r.get(date_col))
        v = _num(r.get(value_col))
        if not d or v is None:
            continue
        monday = d - dt.timedelta(days=d.weekday())
        buckets[monday] = buckets.get(monday, 0.0) + v
    return sorted(buckets.items())


def main(argv):
    args = [a for a in argv if not a.startswith('--')]
    out = 'chart.png'
    for i, a in enumerate(argv):
        if a == '--out' and i + 1 < len(argv):
            out = argv[i + 1]
    if not args:
        print(__doc__)
        return 1
    path = args[0]

    info = describe(path)
    cols, _hxl, recs = read_sheet(path)
    print('fichier  : {}'.format(os.path.basename(path)))
    print('feuille  : {!r} ({} lignes)'.format(info['data_sheet'], len(recs)))

    date_col, _ = find_column(cols, recs, r'date', min_numeric_share=0)
    # une colonne de date n'est pas numerique : on la choisit par motif + parsabilite
    date_cands = [c for c in cols if 'date' in c.lower()]
    date_col = None
    for c in date_cands:
        okd = sum(1 for r in recs[:60] if _as_date(r.get(c)))
        if okd > 30:
            date_col = c
            break
    # Motifs par PRIORITE : a part numerique egale, find_column rend la premiere
    # candidate, et `Total Male` precede `Total Headcount` dans le fichier AFG.
    # On essaie donc du plus specifique au plus large.
    val_col, share = None, 0.0
    for pat in (r'total.?headcount', r'headcount', r'\btotal idp', r'individuals?\b',
                r'^total\b', r'idp|individual|total'):
        val_col, share = find_column(cols, recs, pat)
        if val_col:
            break
    if not date_col or not val_col:
        print('colonnes introuvables (date={}, valeur={}) : ouvrir le fichier et '
              'choisir a la main'.format(date_col, val_col))
        return 2
    print('colonnes : date={!r}, valeur={!r} ({:.0%} numerique)'.format(
        date_col, val_col, share))

    # Y a-t-il une colonne de sens (Inflow/Outflow) a tracer en 2 series ?
    dir_col = next((c for c in cols if c.lower() in ('direction', 'sens')), None)

    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    fig, ax = plt.subplots(figsize=(11, 5.2), dpi=150)
    fig.patch.set_facecolor(HI_BG)

    period = None
    if dir_col:
        for label, color in (('Inflow', HI_BLUE), ('Outflow', HI_ORANGE)):
            series = weekly(recs, date_col, val_col, dir_col, label)
            if not series:
                continue
            xs, ys = zip(*series)
            ax.plot(xs, ys, color=color, lw=2.2, label=label)
            period = (xs[0], xs[-1])
    else:
        series = weekly(recs, date_col, val_col)
        if series:
            xs, ys = zip(*series)
            ax.plot(xs, ys, color=HI_BLUE, lw=2.2, label=val_col[:30])
            period = (xs[0], xs[-1])

    if not period:
        print('aucune serie tracable')
        return 2

    src = os.path.basename(path).rsplit('.', 1)[0][:60]
    ax.set_title('{}\nWeekly totals, {} to {}  (IOM DTM, downloaded {})'.format(
        src, period[0], period[1], dt.date.today()),
        fontsize=11, color=HI_MINERAL, loc='left')
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: '{:,.0f}'.format(v)))
    ax.spines[['top', 'right']].set_visible(False)
    ax.spines[['left', 'bottom']].set_color('#A0B0C0')
    ax.tick_params(colors=HI_MINERAL, labelsize=9)
    ax.grid(axis='y', color='#E6ECF1', lw=0.8)
    ax.legend(frameon=False, fontsize=10, labelcolor=HI_MINERAL)
    ax.margins(x=0.01)
    fig.tight_layout()
    fig.savefig(out, facecolor=HI_BG)
    print('ecrit    : {}'.format(os.path.abspath(out)))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
