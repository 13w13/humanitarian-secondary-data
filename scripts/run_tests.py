"""
Runner de tests du toolkit — stdlib, zero dependance
====================================================
    python -X utf8 scripts/run_tests.py            # toutes les suites
    python -X utf8 scripts/run_tests.py dtm p0     # seulement celles nommees
    python -X utf8 scripts/run_tests.py --list

Pourquoi pas pytest. Les clients sont stdlib par doctrine (portabilite terrain,
anti-fragilite : voir BENCHMARK section cimetiere). Une suite de tests qui exige
`pip install pytest` casserait cette propriete pour le seul confort du runner. Le
motif `test_*_e2e.py` (un module autonome, `main()` qui rend 0 ou 1, assertions
lisibles avec le contexte du bug) a prouve qu'il suffit : 23 assertions sur DTM.

Chaque suite appelle de VRAIES API : compter en minutes, pas en secondes. Un
identifiant manquant produit un SKIP, pas un FAIL.
"""
import importlib
import os
import sys
import time

sys.stdout.reconfigure(encoding='utf-8')

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, 'clients'))

# nom court -> (module, description)
SUITES = {
    'dtm': ('test_dtm_e2e',
            'Catalogue DTM, lecture de fichiers, somme de controle, resolveur'),
    'p0': ('test_p0_e2e',
           'Non-regression des correctifs P0 de la revue de juillet 2026'),
    # Lent (~4 min, beaucoup d'API reelles) : ne PAS l'inclure par defaut.
    'skill50': ('test_skill_50',
                '50 situations du skill : chiffres, analyse, dispo, exploration, accompagnement'),
}


def main(argv):
    if '--list' in argv:
        print('Suites disponibles :')
        for name, (mod, desc) in SUITES.items():
            print('  {:6} {:18} {}'.format(name, mod, desc))
        return 0

    # Par defaut : les suites RAPIDES. skill50 (~4 min) se demande explicitement.
    wanted = [a for a in argv if not a.startswith('-')] or ['dtm', 'p0']
    unknown = [w for w in wanted if w not in SUITES]
    if unknown:
        print('Suite(s) inconnue(s) : {}. Disponibles : {}'.format(
            unknown, ', '.join(SUITES)))
        return 2

    t0 = time.time()
    results = []
    for name in wanted:
        mod_name, desc = SUITES[name]
        print()
        print('#' * 76)
        print('# SUITE {} — {}'.format(name.upper(), desc))
        print('#' * 76)
        try:
            mod = importlib.import_module(mod_name)
        except Exception as e:
            print('  IMPORT IMPOSSIBLE : {}: {}'.format(type(e).__name__, str(e)[:110]))
            results.append((name, 'IMPORT KO'))
            continue
        # Les suites sortent via sys.exit(code) : on l'intercepte pour enchainer.
        try:
            code = mod.main()
        except SystemExit as e:
            code = e.code or 0
        except Exception as e:
            print('  SUITE INTERROMPUE : {}: {}'.format(type(e).__name__, str(e)[:110]))
            code = 1
        results.append((name, 'VERT' if not code else 'ROUGE'))

    print()
    print('=' * 76)
    print('RECAPITULATIF  ({:.0f} s)'.format(time.time() - t0))
    for name, verdict in results:
        print('  {:6} {}'.format(name, verdict))
    ko = [n for n, v in results if v != 'VERT']
    print('=' * 76)
    if ko:
        print('SUITES EN ECHEC : {}'.format(', '.join(ko)))
    return 1 if ko else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
