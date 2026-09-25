"""
Sonde Washington Group — ouvrir un fichier MSNA et regarder ses COLONNES
========================================================================
    python -X utf8 scripts/wgss_probe.py <url_ou_chemin.xlsx>

Pourquoi cette sonde existe. Le recensement par metadonnee (msna_census.py)
SOUS-DETECTE massivement : 1 dataset sur 264 porte un indice WG-SS dans son titre,
alors qu'on SAIT que la MSNA soudanaise contient les WG-SS (brief d'avril 2026). La
metadonnee HDX ne mentionne quasi jamais le module meme quand le fichier l'a. **La
seule facon de savoir, c'est d'ouvrir le fichier et de lire les noms de colonnes.**

⚠ SECURITE. La microdonnee MSNA contient des reponses de menages, donc des donnees a
caractere personnel. Ce script :
  - telecharge UNIQUEMENT dans un repertoire temporaire hors du depot et hors git,
  - ne lit QUE les noms de colonnes et un decompte, JAMAIS les valeurs,
  - propose de supprimer le fichier apres le scan.
Ne jamais committer une microdonnee MSNA, ni la ranger dans un dossier synchronise.

Sortie : les colonnes Washington Group trouvees, par domaine fonctionnel.
"""
import sys
import os
import re

if hasattr(sys.stdout, 'reconfigure'):   # absent in Jupyter, IDLE, captured output
    sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, 'clients'))

# Downloads go to a fresh tempfile.mkdtemp() directory, deleted when the probe
# ends (see probe). It used to be the literal r'C:\tmp\msna_probe': a RELATIVE
# path on Linux and macOS, so household microdata landed in the repository.

# Les 6 domaines fonctionnels du Washington Group Short Set. Une MSNA avec les WG-SS
# a typiquement une colonne par domaine, nommee de facon variable selon l'ONG.
WG_DOMAINS = {
    'vision': r'wg.?q?.?vis|vision|seeing|\bsee\b|difficulty.*see',
    'hearing': r'wg.?q?.?hear|hearing|\bhear\b|difficulty.*hear',
    'mobility': r'wg.?q?.?(mob|walk)|mobility|walking|\bwalk\b|climbing',
    'cognition': r'wg.?q?.?(cog|rem)|cognition|remember|concentrat',
    'selfcare': r'wg.?q?.?(care|self)|self.?care|washing|dressing',
    'communication': r'wg.?q?.?(comm|und)|communicat|understanding|being understood',
}
# Prefixe generique WG, quand la colonne ne nomme pas le domaine (wgq_1, wg_ss_a...)
WG_GENERIC = re.compile(r'\bwg.?q?.?(ss)?[_\- ]?\d|\bwg[_\- ]?ss\b|washington', re.I)


def _read_headers(path):
    """Noms de colonnes de chaque feuille, SANS lire les valeurs."""
    from config import require_module
    openpyxl = require_module('openpyxl', 'Reading a workbook\'s column names')
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    sheets = {}
    for name in wb.sheetnames:
        ws = wb[name]
        rows = [[re.sub(r'\s+', ' ', str(c)).strip() for c in row if c]
                for row in ws.iter_rows(min_row=1, max_row=6, values_only=True)]
        widest = max((len(r) for r in rows), default=0)
        # The FIRST wide row is the header. Taking the widest one could pick a
        # household record (answers) when the header has unnamed helper columns,
        # and the probe would then print answers: it must only ever see names.
        sheets[name] = next((r for r in rows if widest and len(r) >= widest / 2), [])
    wb.close()
    return sheets


def _download_to_tmp(url, tmp_dir):
    from config import download_stream, safe_filename
    name = safe_filename(url.split('/')[-1].split('?')[0])
    if not name.lower().endswith(('.xlsx', '.xls', '.csv', '.zip')):
        name += '.xlsx'
    path = os.path.join(tmp_dir, name)
    # HDX et plusieurs hotes rejettent un UA trop generique -> UA navigateur par defaut.
    ua = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
          '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')
    try:
        from dtm_client import PORTAL_HEADERS
        hdr = dict(PORTAL_HEADERS) if 'dtm.iom.int' in url else {'User-Agent': ua}
    except Exception:
        hdr = {'User-Agent': ua}
    size = download_stream(url, path, headers=hdr, timeout=180)
    return path, size


def probe(path_or_url):
    """Probe one workbook. A downloaded file lives in a fresh temporary directory
    outside the repository and is deleted when the probe ends, whatever happens."""
    import shutil
    import tempfile
    is_url = path_or_url.startswith(('http://', 'https://'))
    if not is_url:
        return _probe_file(path_or_url, is_url=False)
    tmp_dir = tempfile.mkdtemp(prefix='msna_probe_')
    try:
        print('telechargement vers {} (hors depot, hors git, supprime a la fin)...'
              .format(tmp_dir))
        path, size = _download_to_tmp(path_or_url, tmp_dir)
        print('  {} ({:,} o)'.format(os.path.basename(path), size))
        return _probe_file(path, is_url=True)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        print('microdonnee supprimee : {}'.format(tmp_dir))


def _probe_file(path, is_url):
    if not path.lower().endswith(('.xlsx', '.xlsm')):
        # openpyxl reads only xlsx: a .csv/.xls crashed and a .zip returned early,
        # both after the download, and both before the cleanup reminder.
        print('  {} : seuls les .xlsx sont lus ici (dezipper / convertir, puis '
              'relancer sur un chemin local)'.format(os.path.basename(path)))
        return None

    sheets = _read_headers(path)
    print()
    print('feuilles : {}'.format(list(sheets.keys())))
    all_cols = [c for cols in sheets.values() for c in cols]
    print('colonnes totales : {}'.format(len(all_cols)))
    print()

    found = {}
    for dom, pat in WG_DOMAINS.items():
        rx = re.compile(pat, re.I)
        hits = [c for c in all_cols if rx.search(c)]
        if hits:
            found[dom] = hits[:3]
    generic = [c for c in all_cols if WG_GENERIC.search(c)]

    print('=== WASHINGTON GROUP ===')
    if found:
        print('  {} domaine(s) fonctionnel(s) detecte(s) :'.format(len(found)))
        for dom, cols in found.items():
            print('    {:14} <- {}'.format(dom, cols))
    if generic:
        print('  colonnes WG generiques ({}) : {}'.format(len(generic), generic[:5]))
    if not found and not generic:
        print('  AUCUNE colonne Washington Group detectee dans les noms.')
        print('  -> soit le module WG-SS est absent, soit les colonnes sont codees')
        print('     de facon opaque (q_501...). Ouvrir le questionnaire pour trancher.')
    else:
        n = len(found)
        verdict = ('les 6 domaines' if n >= 6 else
                   '{}/6 domaines nommes'.format(n) if n else 'colonnes WG generiques')
        print()
        print('  VERDICT : module WG-SS probablement PRESENT ({}).'.format(verdict))
        print('  Prochaine etape : desagreger la prevalence niveau 3+ par secteur de')
        print('  besoin (seuil standard, prevalence < 16 % normale).')

    return found, generic


def main(argv):
    args = [a for a in argv if not a.startswith('--')]
    if not args:
        print(__doc__)
        return 1
    probe(args[0])
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
