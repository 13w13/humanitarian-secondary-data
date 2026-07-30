"""
Robustesse : la seule suite qui tourne SANS RESEAU
==================================================
    python -X utf8 scripts/test_hardening.py

Les trois autres suites appellent de vraies API : elles sont lentes et elles
dependent de la disponibilite d'autrui. Celle-ci ne fait aucune requete, donc elle
tourne en moins d'une seconde, dans un CI, dans un avion. C'est le filet a poser en
premier quand on doute d'une modification.

Elle couvre les correctifs de robustesse du 2026-07-30, et surtout elle contient une
assertion STATIQUE (test 5) qui empeche la regression de la cause racine : un import
optionnel non protege, qui transforme un extra manquant en traceback.
"""
import io
import os
import re
import sys

sys.stdout.reconfigure(encoding='utf-8')

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, 'clients'))

PASS, FAIL = [], []


def check(label, cond, detail=''):
    (PASS if cond else FAIL).append(label)
    print('  [{}] {} {}'.format('OK  ' if cond else 'FAIL', label, detail))
    return cond


def section(n, title):
    print()
    print('=' * 74)
    print('{}. {}'.format(n, title))
    print('=' * 74)


# ── 1 : validation du code pays ───────────────────────────────
def t_iso3():
    section(1, 'normalize_iso3 : le code pays devient un nom de repertoire')
    from config import normalize_iso3
    for good, want in (('sdn', 'SDN'), ('SDN', 'SDN'), (' lbn ', 'LBN')):
        check('accepte {!r}'.format(good), normalize_iso3(good) == want)
    # Le cas qui compte : un agent qui resout un pays depuis du texte non fiable.
    for bad in ('../../../tmp/evil', '../etc', 'SD', 'SDNX', 'SD1', '', None,
                'caf\u00e9', 'SDN/..', '.'):
        try:
            normalize_iso3(bad)
            check('refuse {!r}'.format(bad), False, 'accepte a tort')
        except ValueError:
            check('refuse {!r}'.format(bad), True)


# ── 2 : credentials sans keyring ──────────────────────────────
def t_credentials():
    section(2, 'get_credential : pas de dependance dure a keyring')
    import builtins
    from config import get_credential
    real = builtins.__import__

    def no_keyring(name, *a, **k):
        if name == 'keyring':
            raise ImportError('simule absent')
        return real(name, *a, **k)

    os.environ.pop('HSD_TEST_CRED', None)
    builtins.__import__ = no_keyring
    try:
        check('sans keyring ni env -> chaine vide',
              get_credential('sds.test', 'field', 'HSD_TEST_CRED') == '')
        os.environ['HSD_TEST_CRED'] = 'depuis-env'
        check('sans keyring, repli sur env',
              get_credential('sds.test', 'field', 'HSD_TEST_CRED') == 'depuis-env')
        check('plusieurs env : le premier renseigne gagne',
              get_credential('sds.test', 'f', 'HSD_ABSENT', 'HSD_TEST_CRED')
              == 'depuis-env')
    finally:
        builtins.__import__ = real
        os.environ.pop('HSD_TEST_CRED', None)


# ── 3 : message d'un extra manquant ───────────────────────────
def t_require_module():
    section(3, 'require_module : une instruction, pas un traceback')
    import builtins
    from config import require_module
    real = builtins.__import__

    def absent(name, *a, **k):
        if name == 'ZZZnotamodule':
            raise ImportError('simule')
        return real(name, *a, **k)

    builtins.__import__ = absent
    try:
        try:
            require_module('ZZZnotamodule', 'Une fonctionnalite optionnelle')
            check('leve bien une ImportError', False, 'aucune exception')
        except ImportError as e:
            msg = str(e)
            check('nomme la fonctionnalite', 'Une fonctionnalite optionnelle' in msg)
            check('donne la commande pip', 'pip install ZZZnotamodule' in msg)
            check('rassure sur le reste', 'standard library only' in msg)
    finally:
        builtins.__import__ = real
    # Un module present doit revenir normalement.
    check('un module present est rendu', require_module('json', 'x').__name__ == 'json')


# ── 4 : telechargeur ──────────────────────────────────────────
def t_downloader():
    section(4, 'telechargeur : schema, plafond, ecriture atomique')
    from download_catalogue import _download_file, MAX_DOWNLOAD_BYTES, DOWNLOAD_CHUNK
    ok, err = _download_file('file:///C:/Windows/win.ini', 'C:/tmp/hsd_no.bin')
    check('file:// refuse', not ok and 'refused scheme' in str(err), str(err)[:56])
    ok, err = _download_file('ftp://example.org/x.csv', 'C:/tmp/hsd_no.bin')
    check('ftp:// refuse', not ok and 'refused scheme' in str(err))
    check('plafond defini et raisonnable',
          16 * 1024 * 1024 <= MAX_DOWNLOAD_BYTES <= 2 * 1024 ** 3,
          '{:,} o'.format(MAX_DOWNLOAD_BYTES))
    check('lecture par morceaux, pas .read() global', 0 < DOWNLOAD_CHUNK <= 4 * 1024 ** 2)
    # L'ecriture atomique se lit dans le code : os.replace depuis un .part.
    src = io.open(os.path.join(HERE, 'download_catalogue.py'), encoding='utf-8').read()
    check('ecrit dans un .part', ".part" in src)
    check('publie via os.replace', 'os.replace(tmp_path, dest_path)' in src)
    check('nettoie le partiel en cas d\'echec', 'os.remove(tmp_path)' in src)


# ── 5 : L'ASSERTION QUI COMPTE ────────────────────────────────
def t_no_unguarded_optional_import():
    section(5, 'aucun import optionnel non protege (garde anti-regression)')
    # Cause racine du 2026-07-30 : 11 des 18 imports non-stdlib etaient nus dans une
    # fonction, donc un extra manquant sortait en ModuleNotFoundError brut au milieu
    # d'un constructeur. Le README promet « no pip install » : cette assertion tient
    # la promesse dans le temps, ce qu'une relecture ne fait pas.
    OPTIONAL = ('keyring', 'openpyxl', 'matplotlib', 'fitz', 'pymupdf',
                'pandas', 'numpy', 'requests')
    offenders = []
    for root, _, files in os.walk(HERE):
        if '__pycache__' in root:
            continue
        for fn in sorted(files):
            if not fn.endswith('.py'):
                continue
            path = os.path.join(root, fn)
            lines = io.open(path, encoding='utf-8', errors='replace').readlines()
            for i, line in enumerate(lines):
                m = re.match(r'(\s+)(?:import|from)\s+(\w+)', line)
                if not m or m.group(2) not in OPTIONAL:
                    continue
                # Acceptable : sous try/except, ou passe par require_module.
                ctx = ''.join(lines[max(0, i - 4):i + 1])
                if re.search(r'try\s*:', ctx) or 'require_module' in ctx:
                    continue
                offenders.append('{}:{} ({})'.format(fn, i + 1, m.group(2)))
    check('zero import optionnel nu', not offenders,
          '; '.join(offenders[:4]) if offenders else '')
    # Et aucun au niveau module, qui casserait l'import du toolkit entier.
    hard = []
    for root, _, files in os.walk(HERE):
        if '__pycache__' in root:
            continue
        for fn in sorted(files):
            if not fn.endswith('.py'):
                continue
            for i, line in enumerate(io.open(os.path.join(root, fn),
                                             encoding='utf-8', errors='replace')):
                m = re.match(r'(?:import|from)\s+(\w+)', line)
                if m and m.group(1) in OPTIONAL:
                    hard.append('{}:{}'.format(fn, i + 1))
    check('zero dependance dure au niveau module', not hard, '; '.join(hard[:4]))


# ── 6 : le core s'importe vraiment sans les extras ────────────
def t_core_imports_bare():
    section(6, 'les 16 clients s\'importent sans aucun extra installe')
    import builtins
    import importlib
    real = builtins.__import__
    blocked = ('keyring', 'openpyxl', 'matplotlib', 'fitz', 'pandas', 'numpy',
               'requests')

    def bare(name, *a, **k):
        if name.split('.')[0] in blocked:
            raise ImportError('extra simule absent : ' + name)
        return real(name, *a, **k)

    mods = [f[:-3] for f in sorted(os.listdir(os.path.join(HERE, 'clients')))
            if f.endswith('_client.py')]
    builtins.__import__ = bare
    broken, uninstantiable = [], []
    try:
        for m in mods:
            try:
                mod = importlib.reload(importlib.import_module(m))
            except ImportError as e:
                broken.append('{} ({})'.format(m, str(e)[:40]))
                continue
            except Exception:
                continue      # une autre erreur n'est pas un probleme d'import
            # Importer NE SUFFIT PAS. La resolution des credentials vit dans les
            # __init__ : le 2026-07-30, un `from config import (...)` multi-ligne
            # oublie a fait passer l'import et casse le constructeur en NameError.
            # Un test qui n'importe pas ET n'instancie pas laisse passer ce bug.
            for attr in dir(mod):
                cls = getattr(mod, attr)
                if not (isinstance(cls, type) and attr.endswith('Client')
                        and cls.__module__ == m):
                    continue
                try:
                    cls()
                except (NameError, AttributeError, TypeError) as e:
                    # Ces trois-la sont des defauts de code, pas un manque de cle.
                    uninstantiable.append('{}.{} ({}: {})'.format(
                        m, attr, type(e).__name__, str(e)[:40]))
                except Exception:
                    pass      # cle absente, reseau coupe : hors sujet ici
    finally:
        builtins.__import__ = real
    check('{} clients importables sans extras'.format(len(mods)), not broken,
          '; '.join(broken[:3]))
    check('constructeurs sains (NameError/AttributeError/TypeError)',
          not uninstantiable, '; '.join(uninstantiable[:3]))


def main():
    for fn in (t_iso3, t_credentials, t_require_module, t_downloader,
               t_no_unguarded_optional_import, t_core_imports_bare):
        try:
            fn()
        except Exception as e:
            FAIL.append(fn.__name__)
            print('  [FAIL] {} a leve : {}: {}'.format(
                fn.__name__, type(e).__name__, str(e)[:110]))
    print()
    print('=' * 74)
    print('{} OK / {} FAIL  (aucune requete reseau)'.format(len(PASS), len(FAIL)))
    if FAIL:
        print('ECHECS : {}'.format(', '.join(FAIL)))
    return 1 if FAIL else 0


if __name__ == '__main__':
    sys.exit(main())
