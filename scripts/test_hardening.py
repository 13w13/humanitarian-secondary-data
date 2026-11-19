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

Le test 12 (2026-11-19) sert un jeu enregistre de l'API WHO Disease Outbreak News
(`fixtures/who_don_sample.json`) depuis 127.0.0.1 : rattachement par les tags pays de
l'OMS, titres pieges (« Sudan virus disease - Uganda »), pagination, pannes.
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
                'pandas', 'numpy', 'requests', 'rasterio', 'tifffile')
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
    section(6, 'les clients s\'importent sans aucun extra installe')
    import builtins
    import importlib
    real = builtins.__import__
    blocked = ('keyring', 'openpyxl', 'matplotlib', 'fitz', 'pandas', 'numpy',
               'requests', 'rasterio', 'tifffile')

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


def t_period_filter():
    section(11, 'filtre de periode : chevauchement, et un zero qui dit pourquoi')
    # Constat du run reel Soudan du 2026-09-25, --date-from 2026-08-01 : le HNO
    # 2026 (01-01 -> 12-31) etait ecarte parce qu'il COMMENCE avant la fenetre, et
    # le resume disait "no disability disaggregation" alors que 6 515 lignes
    # handicap existaient hors fenetre.
    from fetch_country_data import filter_by_period
    rows = [
        {'date_start': '2026-01-01', 'date_end': '2026-12-31', 'category': 'total'},
        {'date_start': '2025-01-01', 'date_end': '2025-12-08', 'category': 'Disability'},
        {'date_start': '2026-06-30', 'date_end': '2026-06-30'},     # round DTM
        {'date_start': '2026-09-01', 'date_end': ''},               # prix du mois
        {'date_start': '', 'date_end': ''},                         # sans date
    ]
    kept, outside = filter_by_period(rows, '2026-08-01')
    check('le HNO annuel en cours est garde', rows[0] in kept)
    check('une ligne sans date est gardee', rows[4] in kept)
    check('une date de debut seule suffit', rows[3] in kept)
    check('ce qui finit avant la fenetre est ecarte',
          rows[1] not in kept and rows[2] not in kept, '{} gardees'.format(len(kept)))
    check('le zero dit ce qui existe hors fenetre',
          outside == '2 outside the period, covering 2025-01-01 to 2026-06-30', outside)
    kept, outside = filter_by_period(rows, None, '2025-12-31')
    check('--date-to seul ecarte ce qui commence apres',
          len(kept) == 2 and outside.startswith('3 outside'), outside)
    kept, outside = filter_by_period(rows)
    check('sans periode : tout est garde, aucune note', len(kept) == len(rows) and not outside)


def t_who_don():
    section(12, 'WHO DON : tags pays de l\'OMS, pieges du titre, pagination (127.0.0.1)')
    # Fixture enregistree le 2026-11-19 sur l'API publique (scripts/fixtures/), servie
    # par un mini-OData local : $top plafonne a 100 comme le vrai serveur, $skip,
    # filtre date et filtre tag pays `regionscountries/any(x:x eq <guid>)`.
    import json
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from urllib.error import HTTPError
    from urllib.parse import parse_qs, urlsplit
    from config import NotCovered, SourceUnavailable
    import who_don_client as w

    with io.open(os.path.join(HERE, 'fixtures', 'who_don_sample.json'), encoding='utf-8') as fh:
        fx = json.load(fh)
    seen = {'tops': [], 'cultures': set(), 'don_requests': 0}

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, obj):
            body = json.dumps(obj).encode('utf-8')
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            u = urlsplit(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            if u.path.startswith('/down/'):
                return self._send(503, {'error': 'simulated outage'})
            top, skip = int(q.get('$top', '50')), int(q.get('$skip', '0'))
            seen['tops'].append(top)
            if top > 100:                       # what the real server answers
                return self._send(400, {'error': {'code': 'BadRequest'}})
            if u.path == '/taxa':
                rows = fx['taxa']
            elif u.path == '/don':
                seen['don_requests'] += 1
                seen['cultures'].add(q.get('sf_culture'))
                flt = q.get('$filter', '')
                guids = set(re.findall(r'any\(x:x eq ([0-9a-f-]{36})\)', flt))
                m = re.search(r'PublicationDateAndTime ge (\S+)', flt)
                since = m.group(1) if m else ''
                rows = sorted((n for n in fx['notices']
                               if n['PublicationDateAndTime'] >= since
                               and (not guids or guids & set(n['regionscountries']))),
                              key=lambda n: n['PublicationDateAndTime'], reverse=True)
            else:
                return self._send(404, {'error': 'no such path'})
            return self._send(200, {'value': rows[skip:skip + top]})

    # Hors reseau : decoupage du titre et noms qui CONTIENNENT un pays.
    st = w.split_title
    check('titre en liste : maladie coupee avant le 1er pays tague',
          st('Ebola disease caused by Bundibugyo virus, Democratic Republic of the Congo, '
             'Kenya and Nigeria', ['Democratic Republic of the Congo', 'Kenya', 'Nigeria'])[0]
          == 'Ebola disease caused by Bundibugyo virus')
    check('tiret colle "(H9N2)- India"', st('Avian Influenza A (H9N2)- India', ['India'])[0]
          == 'Avian Influenza A (H9N2)')
    check('tiret sans espace "Diphtheria-Nigeria"', st('Diphtheria-Nigeria', ['Nigeria'])[0]
          == 'Diphtheria')
    check('premier tiret, pas le dernier "(ex -Sierra Leone)"',
          st('Lassa Fever – The Netherlands (ex –Sierra Leone)')[0] == 'Lassa Fever')
    check('titre 2002 : annee et "Update 14" retires',
          st('2002 - Ebola haemorrhagic fever in Gabon/The Republic of the Congo - Update 14')
          == ('Ebola haemorrhagic fever', 'Gabon/The Republic of the Congo'))
    nc = w.names_country
    for title, iso3, label, want, why in (
            ('Sudan virus disease – Uganda', 'SDN', 'Sudan', False, 'Sudan virus != Soudan'),
            ('Sudan virus disease – Uganda', 'UGA', 'Uganda', True, 'mais bien l\'Ouganda'),
            ('Acute hepatitis E – South Sudan', 'SDN', 'Sudan', False, 'South Sudan != Soudan'),
            ('Acute hepatitis E – South Sudan', 'SSD', 'South Sudan', True, 'Soudan du Sud'),
            ('Crimean-Congo Hemorrhagic Fever - Iraq', 'COG', 'Congo', False, 'Crimee-Congo'),
            ('Ebola disease, Democratic Republic of the Congo', 'COG', 'Congo', False,
             'RDC != Congo'),
            ('Dracunculiasis (Guinea worm disease) – Ethiopia', 'GIN', 'Guinea', False,
             'ver de Guinee'),
            ('Rift Valley Fever – Republic of the Sudan', 'SDN', 'Sudan', True,
             '"Republic of the Sudan"')):
        check('titre : {}'.format(why), nc(title, iso3, label) is want)

    srv = HTTPServer(('127.0.0.1', 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = 'http://127.0.0.1:{}'.format(srv.server_address[1])

    def client(**kw):
        c = w.WHODONClient(base=base + '/don', taxa_base=base + '/taxa')
        for k, v in kw.items():
            setattr(c, k, v)
        return c
    try:
        rows = client().latest(3)
        check('latest(3) : 3 avis, du plus recent au plus ancien',
              len(rows) == 3 and rows[0]['published'] >= rows[1]['published'] >= rows[2]['published'])
        need = {'title', 'published', 'url', 'disease', 'countries_named', 'iso3_named',
                'don_id', 'scope', 'summary'}
        check('contrat de sortie complet', need <= set(rows[0]), str(sorted(need - set(rows[0]))))
        check('url = page publique de l\'avis', rows[0]['url'] ==
              'https://www.who.int/emergencies/disease-outbreak-news/item/2026-DON627')
        by_don = {r['don_id']: r for r in client().latest(50)}
        check('Ebola multi-pays : COD;KEN;NGA, jamais COG',
              by_don['2026-DON625']['iso3_named'] == 'COD;KEN;NGA',
              by_don['2026-DON625']['iso3_named'])
        check('tags hors titre (France, Allemagne) et "germany" -> DEU',
              by_don['2026-DON613']['iso3_named'] == 'COD;UGA;FRA;DEU',
              by_don['2026-DON613']['iso3_named'])
        check('avis "Global" : aucun pays, portee global',
              by_don['2026-DON624']['scope'] == 'global'
              and not by_don['2026-DON624']['countries_named'])
        check('UrlName en slug : url et don_id corrects',
              by_don['2026-DON618']['url'].endswith('---kingdom-of-saudi-arabia')
              and by_don['2026-DON618']['iso3_named'] == 'SAU')
        n0 = seen['don_requests']
        paged = client(page_size=2).latest(5)
        check('pagination $skip : 5 avis distincts en 3 requetes',
              len({r['url'] for r in paged}) == 5 and seen['don_requests'] - n0 == 3,
              '{} requetes'.format(seen['don_requests'] - n0))
        client(page_size=500).latest(5)
        check('$top jamais au-dessus de 100 (400 cote serveur)', max(seen['tops']) <= 100,
              'max {}'.format(max(seen['tops'])))
        check('toujours sf_culture=en (le flux fr s\'arrete en 2024)',
              seen['cultures'] == {'en'}, str(seen['cultures']))

        cod = client().for_country('COD', since='2025-11-19')
        check('for_country COD : les 4 avis tagues, par tag OMS',
              len(cod) == 4 and all(r['match_basis'] == 'who_tag' and 'COD' in r['iso3_named']
                                    for r in cod), str(len(cod)))
        sdn_c = client()
        sdn = sdn_c.for_country('SDN', since='2019-01-01')
        check('for_country SDN : ni "Sudan virus - Uganda" ni "South Sudan"',
              sorted(r['don_id'] for r in sdn) == ['2019-DON205', '2023-DON362'],
              str([r['don_id'] for r in sdn]))
        check('les avis regionaux sans pays nomme sont comptes a part',
              len(sdn_c.last_meta['regional_notices']) == 2)
        isr = client().for_country('ISR', since='2022-01-01')
        check('avis tague region seulement, pays dans le titre : retrouve par le titre',
              [(r['don_id'], r['match_basis']) for r in isr] == [('2022-DON366', 'title')])
        try:
            client().for_country('SDN', since='2025-11-19')
            check('aucun avis : NotCovered', False, 'liste rendue')
        except NotCovered as e:
            msg = str(e)
            check('aucun avis : NotCovered qui dit sa base et les avis regionaux',
                  'regionscountries' in msg and '1 regional or global' in msg, msg[:70])
        try:
            client().for_country('XKX')
            check('code absent de la taxonomie OMS : NotCovered', False)
        except NotCovered:
            check('code absent de la taxonomie OMS : NotCovered', True)
        try:
            client(page_size=1, max_pages=1).for_country('SDN', since='2025-11-19')
            check('marche tronquee sans resultat : pas de NotCovered', False)
        except NotCovered:
            check('marche tronquee sans resultat : pas de NotCovered', False, 'NotCovered')
        except ValueError as e:
            check('marche tronquee sans resultat : pas de NotCovered', 'page cap' in str(e))
        try:
            w.WHODONClient(base=base + '/down/don', taxa_base=base + '/down/taxa').latest(3)
            check('HTTP 503 : SourceUnavailable', False)
        except SourceUnavailable:
            check('HTTP 503 : SourceUnavailable (panne, pas zero)', True)
        try:
            w.WHODONClient(base=base + '/nope', taxa_base=base + '/taxa').latest(3)
            check('HTTP 404 : notre bug, pas une panne', False)
        except SourceUnavailable:
            check('HTTP 404 : notre bug, pas une panne', False, 'SourceUnavailable')
        except HTTPError as e:
            check('HTTP 404 : notre bug, pas une panne', e.code == 404)
        try:
            client().latest(3, since='2026-13-01')
            check('date invalide refusee', False)
        except ValueError:
            check('date invalide refusee', True)
        check('mise en garde RSI en tete des CAVEATS', 'IHR' in w.CAVEATS[0])

        # Pipeline : pays couvert sans avis = `empty` (decision du 19/11), code hors
        # taxonomie = `not_covered`. Le client garde son contrat (NotCovered).
        import contextlib
        import shutil
        import tempfile
        import fetch_country_data as f
        saved = (w.WHO_DON_BASE, w.WHO_TAXA_BASE)
        w.WHO_DON_BASE, w.WHO_TAXA_BASE = base + '/don', base + '/taxa'
        tmp = tempfile.mkdtemp(prefix='hsd_don_')
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                sdn_r = f.fetch_who_don('SDN', tmp, '2025-11-19')
                xkx_r = f.fetch_who_don('XKX', tmp)
                cod_r = f.fetch_who_don('COD', tmp, '2025-11-19')
            check('pipeline SDN sans avis tague : empty, pas not_covered',
                  sdn_r['status'] == 'empty', sdn_r['status'])
            check('... avec la raison et les avis regionaux sur disque',
                  'No DON notice concerns Sudan' in sdn_r['note']
                  and 'who_don_regional.csv' in sdn_r['files'], sdn_r['files'])
            check('pipeline code hors taxonomie OMS : not_covered',
                  xkx_r['status'] == 'not_covered', xkx_r['status'])
            check('pipeline COD : ok, 4 avis', cod_r['status'] == 'ok'
                  and cod_r['total_records'] == 4, str(cod_r['total_records']))
            check('NoNoticeInWindow reste un NotCovered (contrat du client)',
                  issubclass(w.NoNoticeInWindow, NotCovered))
        finally:
            w.WHO_DON_BASE, w.WHO_TAXA_BASE = saved
            shutil.rmtree(tmp, ignore_errors=True)
    finally:
        srv.shutdown()


def t_epidemics():
    section(13, 'epidemies : ReliefWeb, IFRC GO, cholera OMS, comparaison (hors reseau)')
    from config import NotCovered
    from reliefweb_client import ReliefWebClient, parse_disaster_name
    from ifrcgo_client import IFRCGoClient
    from who_cholera_client import WHOCholeraClient
    import epidemic_compare as ec

    for name, want in (('Sudan: Cholera Outbreak - Aug 2024', 'Cholera'),
                       ('DR Congo: Cholera and Measles Outbreaks - 2013-2026',
                        'Cholera and Measles'),
                       ('South Sudan: Measles - Dec 2022', 'Measles')):
        check('nom ReliefWeb -> maladie : {}'.format(want), parse_disaster_name(name) == want)

    # ReliefWeb : reponse enregistree le 19/11/2026 (deux fiches reelles, abregees).
    rw = ReliefWebClient(appname='test')
    sent = []

    def fake_post(endpoint, payload):
        sent.append((endpoint, payload))
        return {'totalCount': 2, 'data': [
            {'fields': {'name': 'DR Congo: Cholera and Measles Outbreaks - 2013-2026',
                        'status': 'ongoing', 'date': {'event': '2013-01-01T00:00:00+00:00'},
                        'country': [{'iso3': 'cod'}], 'primary_country': {'iso3': 'cod'},
                        'glide': 'EP-2013-000175-COD', 'url': 'https://reliefweb.int/x'}},
            {'fields': {'name': 'Sudan: Cholera Outbreak - Sep 2023', 'status': 'past',
                        'date': {'event': '2023-09-26T00:00:00+00:00'},
                        'country': [{'iso3': 'sdn'}], 'primary_country': {'iso3': 'sdn'},
                        'url': 'https://reliefweb.int/y'}}]}
    rw._post = fake_post
    rows = rw.ongoing_epidemics('SDN', statuses=('ongoing', 'alert', 'past'))
    conds = sent[0][1]['filter']['conditions']
    check('ReliefWeb : endpoint disasters, type EP, filtre country.iso3',
          sent[0][0] == 'disasters' and {'field': 'type.code', 'value': 'EP'} in conds
          and {'field': 'country.iso3', 'value': 'sdn'} in conds)
    check('fiche RDC ouverte depuis 2013 : signalee stale', rows[0]['stale'] is True
          and rows[0]['iso3'] == 'COD')
    check('fiche fermee : ni stale ni duree ouverte', rows[1]['stale'] is False
          and rows[1]['open_days'] is None)

    # IFRC GO : evenements + appels, dtype 1 envoye, appel trop ancien ecarte.
    go = IFRCGoClient()
    calls = []

    def fake_get(endpoint, params=None):
        calls.append((endpoint, dict(params or {})))
        if endpoint == 'event/':
            return {'next': None, 'results': [{
                'id': 7, 'name': 'NGA: Epidemic - 11-2026 - Diphtheria Outbreak',
                'dtype': {'name': 'Epidemic'}, 'disaster_start_date': '2026-11-05T00:00:00Z',
                'countries': [{'name': 'Nigeria', 'iso3': 'NGA'}]}]}
        return {'next': None, 'results': [
            {'code': 'MDRSO028', 'name': 'Somalia - Cholera Outbreak', 'atype_display': 'DREF',
             'status_display': 'Active', 'country': {'name': 'Somalia', 'iso3': 'SOM'},
             'dtype': {'name': 'Epidemic'}, 'start_date': '2026-10-22T00:00:00Z'},
            {'code': 'OLD', 'name': 'Old appeal', 'atype_display': 'DREF',
             'status_display': 'Closed', 'country': {'iso3': 'SOM'},
             'start_date': '2019-01-01T00:00:00Z'}]}
    go._get = fake_get
    epi = go.epidemics(since='2026-01-01')
    check('IFRC GO : dtype 1 (Epidemic) envoye aux deux endpoints',
          all(p.get('dtype') == '1' for _, p in calls), str(calls)[:80])
    check('IFRC GO : un evenement et un appel, l\'ancien ecarte',
          [(r['kind'], r['iso3']) for r in epi] == [('event', 'NGA'), ('appeal', 'SOM')])

    # Cholera OMS : dates ArcGIS en millisecondes, pays absent = NotCovered.
    ch = WHOCholeraClient()
    ch._query = lambda params: ({'count': 0} if params.get('returnCountOnly') else
                                {'features': [{'attributes': {
                                    'iso_3_code': 'SDN', 'date_wk': 1791158400000,
                                    'cases': 20, 'deaths': 0, 'cum_case': 138260}}]}
                                if "'SDN'" in params['where'] else {'features': []})
    wk = ch.weekly('SDN')
    check('cholera : semaine lue depuis l\'epoch ArcGIS', wk[0]['week_start'] == '2026-10-05'
          and wk[0]['cum_cases'] == 138260, wk[0]['week_start'])
    try:
        ch.weekly('FRA')
        check('cholera : pays sans serie = NotCovered', False)
    except NotCovered:
        check('cholera : pays sans serie = NotCovered', True)

    # Comparaison : le cas Soudan du 19/11/2026, reconstitue.
    check('maladie : "Sudan virus" = ebola, pas le Soudan',
          ec.disease_keys('Sudan virus disease – Uganda') == ['ebola'])
    check('maladie : deux maladies dans un nom',
          ec.disease_keys('DR Congo: Cholera and Measles Outbreaks') == ['cholera', 'measles'])
    check('maladie : AWD compte comme cholera',
          ec.disease_keys('Acute Watery Diarrhoea (AWD) Outbreak') == ['cholera'])
    sdn_rw = [{'disease': 'Cholera', 'iso3': 'SDN', 'date_start': '2023-09-26',
               'status': 'past', 'stale': False, 'open_days': None, 'url': 'u'}]
    sdn_ch = [{'iso3': 'SDN', 'week_start': '2026-09-28', 'cases': 0, 'cum_cases': 138240},
              {'iso3': 'SDN', 'week_start': '2026-10-05', 'cases': 20, 'cum_cases': 138260}]
    sig = ec.signals_reliefweb(sdn_rw, '2025-11-19') + ec.signals_cholera(sdn_ch)
    ok_all = {s: 'ok' for s in ec.SOURCES}
    row = ec.merge_signals(sig, ok_all, '2025-11-19')[0]
    check('Soudan : ni DON ni IFRC, fiche ReliefWeb fermee', row['gaps'] ==
          ['no who_don', 'no ifrc_go', 'reliefweb page closed'], str(row['gaps']))
    check('Soudan : la semaine a 0 cas ne compte pas comme debut',
          row['cells']['who_cholera']['first'] == '2026-10-05')
    row = ec.merge_signals(sig, dict(ok_all, who_don='unavailable'), '2025-11-19')[0]
    check('source en panne : jamais comptee comme un ecart', 'no who_don' not in row['gaps'])
    both = ec.signals_reliefweb([dict(sdn_rw[0], date_start='2024-01-01'),
                                 dict(sdn_rw[0], status='ongoing', date_start='2026-03-01')],
                                '2025-11-19')
    cell = ec.merge_signals(both, ok_all, '2025-11-19')[0]['cells']['reliefweb']
    check('fiche ouverte prioritaire sur une fiche fermee',
          cell['status'] == 'ongoing' and cell['first'] == '2026-03-01')


def main():
    for fn in (t_iso3, t_credentials, t_require_module, t_downloader,
               t_no_unguarded_optional_import, t_core_imports_bare,
               t_outage_taxonomy, t_outage_is_never_zero, t_downloader_behaviour,
               t_golden_sentences, t_period_filter, t_who_don, t_epidemics):
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
