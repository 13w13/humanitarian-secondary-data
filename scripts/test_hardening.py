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

Les tests 7 et 8 (2026-09-25) couvrent l'autre cause racine : une panne lue comme une
absence. Le test 8 coupe le reseau au niveau socket et fait tourner le pipeline
complet : rien ne sort de la machine, et aucune source ne doit se dire `ok`.
"""
import io
import os
import re
import sys

if hasattr(sys.stdout, 'reconfigure'):   # absent in Jupyter, IDLE, captured output
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
            except Exception as e:
                # Toute erreur a l'import casse le client (SyntaxError, AttributeError
                # sur sys.stdout.reconfigure...) : elle passait avant sans bruit.
                broken.append('{} ({}: {})'.format(m, type(e).__name__, str(e)[:40]))
                continue
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


# ── 7 : une panne n'est jamais un zero ────────────────────────
def t_outage_taxonomy():
    section(7, 'is_outage : panne amont ou bug chez nous ?')
    import socket
    from urllib.error import HTTPError, URLError
    from config import SourceUnavailable, MissingCredential, NotCovered, is_outage

    def http(code):
        return HTTPError('https://example.org', code, 'x', {}, None)
    for exc, want, label in (
            (URLError('Tunnel connection failed: 403 Forbidden'), True, 'proxy qui refuse'),
            (socket.timeout('timed out'), True, 'timeout'),
            (ConnectionResetError('reset'), True, 'connexion coupee'),
            (http(503), True, 'HTTP 503'), (http(429), True, 'HTTP 429'),
            (http(404), False, 'HTTP 404 = URL fausse, notre bug'),
            (http(403), False, 'HTTP 403 du fournisseur = notre bug'),
            (KeyError('flows'), False, 'KeyError = schema change'),
            (SourceUnavailable('x'), True, 'SourceUnavailable'),
            (MissingCredential('x'), False, 'cle absente = SKIP, pas une panne'),
            (NotCovered('x'), False, 'pays non couvert')):
        check(label, is_outage(exc) is want)
    check('MissingCredential reste un ValueError (compatibilite)',
          issubclass(MissingCredential, ValueError))
    from gdacs_client import GDACSUnavailable
    from dtm_client import UnmappedCountry, NoApiAccess
    check('GDACSUnavailable est un SourceUnavailable',
          issubclass(GDACSUnavailable, SourceUnavailable))
    check('UnmappedCountry est un NotCovered', issubclass(UnmappedCountry, NotCovered))
    check('NoApiAccess est un MissingCredential', issubclass(NoApiAccess, MissingCredential))


def t_outage_is_never_zero():
    section(8, 'panne reseau simulee : aucune source ne rend « 0 » (constat du 2026-09-24)')
    # Le 2026-09-24, avec le reseau coupe, le pipeline sortait en succes, n'ecrivait
    # aucun fichier, et affichait « 0 plans, 0 flows ($0 total 2026) », « Stock 0
    # IDPs », « 0 conflict events » pour le Soudan. Ici on coupe le reseau au niveau
    # socket (rien ne sort), on neutralise les attentes, et on exige qu'aucune
    # source ne se declare `ok` ou `empty`.
    import contextlib
    import csv
    import shutil
    import socket
    import tempfile
    import time as time_mod

    def refused(*a, **k):
        raise ConnectionRefusedError('panne simulee par test_hardening')

    real_conn, real_sleep, real_argv = socket.create_connection, time_mod.sleep, sys.argv
    tmp = tempfile.mkdtemp(prefix='hsd_outage_')
    socket.create_connection = refused
    time_mod.sleep = lambda *a, **k: None
    try:
        import fetch_country_data as f
        sys.argv = ['fetch_country_data.py', 'YEM', '--output-dir', tmp]
        with contextlib.redirect_stdout(io.StringIO()):
            code = f.main()
        with open(os.path.join(tmp, 'fetch_summary.csv'), encoding='utf-8') as fh:
            rows = list(csv.DictReader(fh))
        import recipes
        with contextlib.redirect_stdout(io.StringIO()):
            fund = recipes.get_funding('SDN')
            pin = recipes.get_pin('SDN')
    finally:
        socket.create_connection, time_mod.sleep, sys.argv = real_conn, real_sleep, real_argv
        shutil.rmtree(tmp, ignore_errors=True)

    check('code de sortie 1 (run incomplet)', code == 1, 'code {}'.format(code))
    check('fetch_summary.csv : une ligne par source', len(rows) == len(f.ALL_SOURCES),
          '{} lignes'.format(len(rows)))
    liars = ['{}={}'.format(r['source'], r['status']) for r in rows
             if r['status'] in ('ok', 'empty', 'partial')]
    check('aucune source ok/empty/partial sans reseau', not liars, ', '.join(liars))
    bugs = ['{}: {}'.format(r['source'], r['note'][:60]) for r in rows
            if r['status'] == 'error']
    check('une panne n\'est classee "error" nulle part', not bugs, '; '.join(bugs[:2]))
    check('au moins une source "unavailable"',
          any(r['status'] == 'unavailable' for r in rows))
    check('recette financement : valeur None + PANNE dite',
          fund['value'] is None and any('PANNE' in c for c in fund['caveats']),
          (fund['caveats'] or [''])[0][:60])
    check('recette PiN : valeur None + PANNE dite',
          pin['value'] is None and any('PANNE' in c for c in pin['caveats']))


def t_downloader_behaviour():
    section(9, 'telechargeur, en vrai : serveur local (127.0.0.1, rien ne sort)')
    # Les tests 4 lisent le code source ; ceux-ci le font tourner. Un serveur HTTP
    # local sert des reponses piegees : corps tronque, page HTML a la place d'un
    # xlsx, redirection vers ftp://, nom reel dans Content-Disposition.
    import shutil
    import tempfile
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from download_catalogue import _download_file

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            if self.path == '/short.xlsx':         # annonce 1000 o, en envoie 10
                self.send_response(200)
                self.send_header('Content-Length', '1000')
                self.end_headers()
                self.wfile.write(b'PK\x03\x04' + b'x' * 6)
                self.wfile.flush()
                self.close_connection = True
            elif self.path == '/login.xlsx':       # page de login en HTTP 200
                body = b'<!DOCTYPE html><html><body>Sign in</body></html>'
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif self.path == '/to-ftp.xlsx':      # 302 vers un autre schema
                self.send_response(302)
                self.send_header('Location', 'ftp://127.0.0.1/secret.csv')
                self.send_header('Content-Length', '0')
                self.end_headers()
            elif self.path == '/track/100046':     # tracker DTM sans extension
                body = b'%PDF-1.4 fake'
                self.send_response(200)
                self.send_header('Content-Disposition',
                                 'attachment; filename="../SDN snapshot.pdf"')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.send_header('Content-Length', '0')
                self.end_headers()

    srv = HTTPServer(('127.0.0.1', 0), H)
    port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    tmp = tempfile.mkdtemp(prefix='hsd_dl_')
    base = 'http://127.0.0.1:{}'.format(port)
    try:
        ok, err = _download_file(base + '/short.xlsx', os.path.join(tmp, 'a.xlsx'))
        check('corps tronque refuse', not ok and 'truncated' in str(err), str(err)[:60])
        ok, err = _download_file(base + '/login.xlsx', os.path.join(tmp, 'b.xlsx'))
        check('page HTML refusee comme xlsx', not ok and 'HTML' in str(err), str(err)[:60])
        ok, err = _download_file(base + '/to-ftp.xlsx', os.path.join(tmp, 'c.xlsx'))
        check('redirection vers ftp:// refusee', not ok and 'non-http' in str(err),
              str(err)[:60])
        ok, _ = _download_file(base + '/track/100046', os.path.join(tmp, '100046'))
        names = sorted(os.listdir(tmp))
        check('nom reel lu dans Content-Disposition, sans ../', ok and names ==
              ['100046_SDN snapshot.pdf'], str(names))
        check('aucun .part ni fichier partiel laisse',
              not [n for n in names if n.endswith('.part') or n[0] in 'abc'])
    finally:
        srv.shutdown()
        shutil.rmtree(tmp, ignore_errors=True)


def t_golden_sentences():
    section(10, 'resolveur : phrases de reference (portee, pays, historique, variation)')
    # Chaque phrase est un cas reel ou un contre-exemple de la revue de 2026-09. Le
    # classement se fait hors reseau : c'est la partie du toolkit qui decide ce qui
    # est citable, elle doit etre testee a chaque modification.
    from report_figures import figure_sentences, _names_country, _change_value

    def one(text, iso3):
        got = figure_sentences(text, iso3=iso3, report_year='2026')
        return got[0] if got else {}

    cases = [
        ('LBN', 'As of 22 July 2026, IOM\'s DTM recorded 375,090 internally displaced '
                'persons (IDPs) across Lebanon, representing a nine per cent decrease '
                'compared to 15 July.', True, 'le titre de reference du README'),
        ('LBN', 'As of 22 July 2026, DTM recorded 375,090 IDPs across Lebanon, a nine '
                'per cent decrease from the prior week.', True, '"prior week" n\'est pas historique'),
        ('SDN', 'As of 30 June 2026, DTM recorded 2,026,000 IDPs across South Sudan.',
         False, 'Soudan du Sud n\'est pas le Soudan'),
        ('LBN', 'DTM recorded 120,500 IDPs in Mount Lebanon.', False, 'Mont-Liban = gouvernorat'),
        ('SDN', 'DTM identified 1,815,000 IDPs in South Darfur State, Sudan.', False,
         'un Etat soudanais'),
        ('SYR', 'An estimated 2,900,000 IDPs remain in north-west Syria.', False,
         'nord-ouest = une partie du pays'),
        ('COD', 'More than 5,700,000 people are displaced in eastern DRC.', False,
         'est de la RDC'),
        ('PSE', 'An estimated 1,900,000 people are displaced across Gaza.', False,
         'Gaza = une partie du territoire'),
        ('SDN', 'In January 2025, the number of IDPs in Sudan reached its highest level, '
                'with 11,585,384 IDPs recorded.', False, 'un plus-haut n\'est pas le courant'),
        ('COD', 'DTM estimated 6,200,000 IDPs across the Democratic Republic of the Congo.',
         True, 'la RDC reste nationale'),
    ]
    for iso3, text, want, label in cases:
        got = one(text, iso3)
        check('{} {}'.format(iso3, label), bool(got.get('headline_ok')) is want,
              'scope={} hist={} sup={}'.format(got.get('scope'), got.get('historical'),
                                               got.get('superlative')))
    check('rapport "South Sudan" hors sujet pour SDN',
          not _names_country('DTM South Sudan: Mobility Update', 'SDN'))
    check('"Sudan and South Sudan" nomme bien le Soudan',
          _names_country('Sudan and South Sudan crisis', 'SDN'))
    check('variation = le nombre apres "by"',
          _change_value('decreased by 120,233 to 8,685,273 IDPs', 8685273) == 120233)


def main():
    for fn in (t_iso3, t_credentials, t_require_module, t_downloader,
               t_no_unguarded_optional_import, t_core_imports_bare,
               t_outage_taxonomy, t_outage_is_never_zero, t_downloader_behaviour,
               t_golden_sentences):
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
