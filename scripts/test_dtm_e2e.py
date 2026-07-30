"""
Test de régression DTM, bout en bout : question -> réponse sourcée
==================================================================
Trois chemins, trois cas réels. À relancer après toute modification de
`dtm_client.py`, `dtm_files.py` ou `report_figures.py`.

    python -X utf8 scripts/test_dtm_e2e.py

Ancre chiffrée stable : le fichier Afghanistan Flow Monitoring Counting est
**cumulatif depuis le 10 janvier 2024**, donc la période 05-18 juillet 2026 y reste
présente quand de nouveaux rounds sont publiés. Les 4 sommes de contrôle ci-dessous
doivent rester EXACTES indéfiniment. Si l'une casse, c'est la lecture du fichier qui
a régressé (feuille, ligne d'en-tête, parsing de date ou de nombre), pas la donnée.

Ce que le test vérifie, au-delà des sommes :
  - le catalogue refuse un pays qu'il ne couvre pas au lieu de chercher au hasard
  - un dataset verrouillé lève DatasetGated au lieu d'être téléchargé
  - le Liban reste 0 ouvert sur ses datasets, le Soudan reste ouvert
  - `citable` ne contient QUE des phrases de portée nationale (le bug du 2026-07-25 :
    Kordofan et El Fasher remontaient comme caseload national)
"""
import os
import sys
import time

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'clients'))

from dtm_client import DTMClient, UnmappedCountry, DatasetGated       # noqa: E402
from dtm_files import describe, read_sheet, sum_by, checksum          # noqa: E402
from report_figures import latest_figures                             # noqa: E402

CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '_report_cache')
PASS, FAIL = [], []


def check(label, cond, detail=''):
    (PASS if cond else FAIL).append(label)
    print('  [{}] {} {}'.format('OK  ' if cond else 'FAIL', label, detail))
    return cond


def main():
    t0 = time.time()
    d = DTMClient()

    print('=' * 76)
    print('1. Le catalogue refuse ce qu\'il ne couvre pas')
    print('=' * 76)
    for bad in ('PSE', 'MMR'):
        try:
            d.browse_catalogue(bad, max_pages=1)
            check('refus ' + bad, False, 'a répondu au lieu de lever')
        except UnmappedCountry:
            check('refus ' + bad, True, '-> UnmappedCountry')

    print()
    print('=' * 76)
    print('2. Scope pays réel, et statut d\'accès par pays')
    print('=' * 76)
    lbn = d.browse_catalogue('LBN', max_pages=1)
    sdn = d.browse_catalogue('SDN', max_pages=1)
    check('LBN scope', {r['country_slug'] for r in lbn if r['country_slug']} <= {'lebanon'},
          '{} lignes'.format(len(lbn)))
    check('LBN tout verrouillé', all(r['access'] == 'gated' for r in lbn),
          '{}/{} verrouillés'.format(sum(1 for r in lbn if r['access'] == 'gated'), len(lbn)))
    check('SDN majoritairement ouvert',
          sum(1 for r in sdn if r['access'] == 'open') >= len(sdn) - 1,
          '{}/{} ouverts'.format(sum(1 for r in sdn if r['access'] == 'open'), len(sdn)))
    try:
        d.download_dataset(lbn[0], os.path.join(CACHE, 'nope'))
        check('refus de télécharger un verrouillé', False, 'a téléchargé')
    except DatasetGated:
        check('refus de télécharger un verrouillé', True, '-> DatasetGated')

    print()
    print('=' * 76)
    print('3. Lecture d\'un fichier + SOMME DE CONTRÔLE (ancre stable)')
    print('=' * 76)
    rows = d.browse_catalogue('AFG', max_pages=1, open_only=True)
    fm = [r for r in rows if 'flow monitoring' in r['title'].lower()]
    if not check('dataset Flow Monitoring AFG trouvé', bool(fm)):
        return finish(t0)
    path = d.download_dataset(fm[0], os.path.join(CACHE, 'afg'))
    info = describe(path)
    chosen = [s for s in info['sheets'] if s['is_chosen']][0]
    cols, _hxl, recs = read_sheet(path)
    check('feuille et en-tête détectés', chosen['header_row'] == 1 and len(recs) > 10000,
          'feuille {!r}, en-tête r{}, {} lignes'.format(
              info['data_sheet'][:30], chosen['header_row'], len(recs)))

    P1 = ('2026-07-05', '2026-07-18')      # période du Flow Monitoring Update publié
    P0 = ('2026-06-21', '2026-07-04')      # période précédente (deltas 28 607 / 6 039)
    for lab, filt, per, pub in [
            ('TOTAL INFLOWS 05-18 Jul', {'Direction': 'Inflow'}, P1, 120099),
            ('TOTAL OUTFLOWS 05-18 Jul', {'Direction': 'Outflow'}, P1, 38979),
            ('INFLOWS période précédente', {'Direction': 'Inflow'}, P0, 91492),
            ('OUTFLOWS période précédente', {'Direction': 'Outflow'}, P0, 32940)]:
        r = sum_by(recs, 'Total Headcount', filt, date_col='ReportingDate',
                   date_from=per[0], date_to=per[1])
        check('checksum ' + lab, checksum(r, pub, lab))

    # Garde-fou : refuser de sommer une colonne qui n'est pas sommable
    try:
        sum_by(recs, 'Male Under 5 %', {})
        check('refus de sommer un pourcentage', False, 'a accepté')
    except ValueError:
        check('refus de sommer un pourcentage', True, '-> ValueError')

    print()
    print('=' * 76)
    print('4. Résolveur : portée des phrases citables')
    print('=' * 76)
    for iso, expect_gated in (('LBN', True), ('SDN', False)):
        dos = latest_figures(iso, topic='dtm', cache_dir=CACHE)
        cit = dos['citable']
        check('{} phrases citables'.format(iso), bool(cit), '{} phrase(s)'.format(len(cit)))
        check('{} toutes de portée nationale'.format(iso),
              all(c['scope'] == 'national' for c in cit))
        check('{} aucune historique ni tronquée'.format(iso),
              all(not c['historical'] and not c['truncated'] for c in cit))
        # Assertion de FOND, pas d'étiquette : une phrase peut être nationale, datée
        # et non tronquée tout en portant un chiffre absurde. Vécu SDN : « decreased
        # by 4,643,703 IDPs compared to the end of May 2026 » alors que la variation
        # réelle du mois est de ~120 k. Elle passait les 3 tests ci-dessus.
        check('{} aucun chiffre incohérent dans citable'.format(iso),
              not any(c.get('magnitude_flag') for c in cit),
              '{} écartée(s) vers other_sentences'.format(
                  sum(1 for c in dos['other_sentences'] if c.get('magnitude_flag'))))
        check('{} verrou {}'.format(iso, 'détecté' if expect_gated else 'absent'),
              bool(dos['gated']) == expect_gated)
        if cit:
            print('      « {} »'.format(cit[0]['sentence'][:132]))
    finish(t0)


def finish(t0):
    print()
    print('=' * 76)
    print('{} OK / {} FAIL en {:.0f} s'.format(len(PASS), len(FAIL), time.time() - t0))
    if FAIL:
        print('ECHECS : {}'.format(', '.join(FAIL)))
    sys.exit(1 if FAIL else 0)


if __name__ == '__main__':
    main()
