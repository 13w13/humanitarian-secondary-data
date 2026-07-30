"""
Report Figures — le résolveur « dernier chiffre »
=================================================
Répond à « quel est le dernier chiffre disponible pour {pays} » en interrogeant les
couches dans l'ordre, et en rendant des **phrases citables** plutôt que des nombres.

Pourquoi des phrases. Un appariement nombre↔libellé par expression régulière sur un
PDF donne des faux : mesuré le 2026-07-25 sur les Flow Monitoring Updates afghans,
`2026 Jul` sort comme un effectif, `11,279 BORDER POINT BREAKDOWN` prend un titre de
section pour un libellé, et `25,001 – 75,000` est une borne de légende de carte.
L'appariement est une tâche de LECTURE. Le code livre donc le texte et les phrases
porteuses de chiffres ; l'interprétation revient à l'appelant, qui doit citer la
phrase avec le chiffre. Aucune fonction d'ici ne renvoie « le » chiffre.

Les 4 couches, et pourquoi aucune ne suffit (vérifié AFG + LBN, 2026-07-25) :

    A. API du producteur   séries typées, mais EN RETARD (DTM v3 : AFG au 2026-01-31,
                           soit 6 mois, en déclarant l'opération « Active »)
    B. Commons (HAPI)      comparable entre pays, mais latence de publication
    C. Portail producteur  les FICHIERS, les plus frais en brut (AFG 21 juil.),
                           mais parfois verrouillés (LBN : 93 datasets, 0 ouvert)
    D. Publication         ReliefWeb : les CHIFFRES les plus frais (AFG et LBN au
                           23 juil.), universel, sans clé, mais en PDF et avec doublons

Règle : le plus frais et le plus structuré ne sont jamais la même couche.

Usage :
    from report_figures import latest_figures, latest_reports, figure_sentences

    d = latest_figures('LBN', topic='dtm')
    for c in d['citable']:
        print(c['as_of'], c['sentence'])        # à citer tel quel
    print(d['freshness'])                       # ce que dit chaque couche, et sa date
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
import os
import re
from urllib.request import Request, urlopen

from config import DEFAULT_TIMEOUT, USER_AGENT

RELIEFWEB_REPORTS = 'https://api.reliefweb.int/v2/reports'

# Sujet → comment le trouver. `rw_source` = source.shortname ReliefWeb,
# `rw_query` = requête sur le titre, `catalogue` = résolveur de la couche C.
TOPICS = {
    'dtm': {
        'label': 'déplacement / mobilité (IOM DTM)',
        'rw_source': 'IOM',
        'rw_query': ('DTM OR "Displacement Tracking" OR "Mobility Snapshot" OR '
                     '"Flow Monitoring" OR "Rapid Displacement Tracking" OR '
                     '"Emergency Trend Tracking"'),
        'catalogue': 'dtm',
    },
    'unhcr': {
        'label': 'réfugiés (UNHCR)',
        'rw_source': 'UNHCR',
        'rw_query': 'UNHCR OR refugee OR "Operational Update"',
        'catalogue': None,
    },
    'ocha': {
        'label': 'besoins / situation (OCHA)',
        'rw_source': 'OCHA',
        'rw_query': '"Situation Report" OR "Humanitarian Needs" OR Flash OR Snapshot',
        'catalogue': None,
    },
    'acaps': {
        'label': 'analyse de crise (ACAPS)',
        'rw_source': 'ACAPS',
        'rw_query': ('"Thematic Report" OR "Briefing Note" OR "Anticipatory note" OR '
                     '"Short note" OR "Risk Report" OR ACAPS'),
        'catalogue': None,
        # ⚠ Statut epistemique DIFFERENT des autres sujets : un produit ACAPS est une
        # ANALYSE de tierce partie (interpretation, causalite, anticipation), pas une
        # mesure. Voir `analytical_findings()`.
        'epistemic': 'analysis',
    },
}

# Lignes à écarter : bornes de légende de carte (« 25,001 – 75,000 ») et séries de
# nombres nus sans phrase autour. Elles portent des chiffres mais aucun sens.
_LEGEND = re.compile(r'^\s*\d[\d,]*\s*[–\-—]\s*\d[\d,]*\s*$')
_NUMBER = re.compile(r'\b\d{1,3}(?:,\d{3})+\b|\b\d{4,}\b')
# Un millésime seul n'est pas un effectif.
_YEARISH = re.compile(r'^(19|20)\d{2}$')


def _appname():
    """appname ReliefWeb pré-approuvé. Depuis 2025-11-01 un appname libre est 403."""
    try:
        import keyring
        a = keyring.get_password('sds.reliefweb', 'appname')
        if a:
            return a
    except Exception:
        pass
    return os.environ.get('RELIEFWEB_APPNAME', 'humanitarian-secondary-data')


def _post(payload):
    req = Request('{}?appname={}'.format(RELIEFWEB_REPORTS, _appname()),
                  data=json.dumps(payload).encode('utf-8'),
                  headers={'Content-Type': 'application/json', 'User-Agent': USER_AGENT})
    return json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT + 15).read())


def _dedup_key(report):
    """Clé d'identité d'un PRODUIT, pour écarter les doublons de ReliefWeb.

    ReliefWeb indexe deux fois le même produit quand l'agence republie une version
    corrigée, et le titre peut porter une plage de dates FAUSSE. Cas vécu (AFG) :
      2026-07-23  « Flow Monitoring Update (05–18 July 2026) »  IOM DTM FM 05 - 18 Jul 2026 Afghanistan_20260721_Eng_0.pdf
      2026-07-21  « Flow Monitoring Update (21 - 19 July 2026) »  IOM DTM FM 05 - 18 Jul 2026 Afghanistan_20260720_Eng.pdf
    Plage « 21 - 19 July » est impossible ; les deux fichiers couvrent la même
    période et rendent les mêmes chiffres. On dédoublonne donc sur le nom de fichier
    débarrassé de ses horodatages et suffixes de version, pas sur le titre.
    """
    files = report.get('file') or []
    if files:
        n = str(files[0].get('filename', '')).lower()
        n = re.sub(r'\.(pdf|xlsx?|docx?)$', '', n)
        n = re.sub(r'_?\d{8}_?', '_', n)              # _20260721_
        n = re.sub(r'_(v?\d+|eng|en|fr|ar|final|rev\d*)\b', '', n)
        n = re.sub(r'[^a-z0-9]+', ' ', n).strip()
        if n:
            return n
    return re.sub(r'[^a-z0-9]+', ' ', str(report.get('title', '')).lower()).strip()


def latest_reports(iso3, topic=None, query=None, source=None, limit=6,
                   include_all_countries=True):
    """Derniers rapports publiés pour un pays, doublons écartés.

    `include_all_countries=True` filtre sur `country.iso3` et non
    `primary_country.iso3` : un rapport régional (« Escalation in the Middle East »)
    a un seul pays primaire mais concerne bien le Liban. Le client ReliefWeb
    historique utilise `primary_country`, plus étroit.

    Retourne une liste de dicts : title, date, url, files, body, dedup_key,
    plus `duplicates` (les entrées écartées, pour traçabilité).
    """
    spec = TOPICS.get(topic or '', {})
    source = source or spec.get('rw_source')
    query = query or spec.get('rw_query')

    conds = [{'field': 'country.iso3' if include_all_countries
              else 'primary_country.iso3', 'value': iso3.lower()}]
    if source:
        conds.append({'field': 'source.shortname', 'value': source})
    payload = {
        'filter': ({'operator': 'AND', 'conditions': conds} if len(conds) > 1
                   else conds[0]),
        'fields': {'include': ['title', 'body', 'date.original', 'url',
                               'file.url', 'file.filename', 'file.mimetype',
                               'file.filesize', 'format.name', 'source.shortname',
                               'primary_country.iso3']},
        'sort': ['date.original:desc'],
        'limit': max(limit * 3, 12),      # marge pour absorber les doublons
    }
    if query:
        payload['query'] = {'value': query, 'fields': ['title']}

    try:
        resp = _post(payload)
    except Exception as e:
        print('  ReliefWeb: {}'.format(str(e)[:100]))
        return {'reports': [], 'duplicates': [], 'off_topic': [], 'total': 0}

    kept, dups, off_topic, seen = [], [], [], set()
    for entry in resp.get('data', []):
        f = entry.get('fields', {})
        rec = {
            'title': f.get('title', ''),
            'date': str((f.get('date') or {}).get('original', ''))[:10],
            'url': f.get('url', ''),
            'source': [s.get('shortname') for s in (f.get('source') or [])],
            'formats': [x.get('name') for x in (f.get('format') or [])],
            'body': f.get('body') or '',
            'file': f.get('file') or [],
        }
        rec['dedup_key'] = _dedup_key(f)
        # `country.iso3` est inclusif par choix (il attrape les produits régionaux),
        # mais ReliefWeb tague un rapport avec TOUS les pays qu'il mentionne : une
        # requête PSE ramenait « DTM Montenegro: Migration Data and Routes Quarterly
        # Report ». On vérifie donc que le pays est nommé dans le titre, ou que
        # `primary_country` est bien le pays demandé. Sinon on écarte : c'est la même
        # famille de bug que le `search=lebanon` qui rend de la Syrie.
        # Forme variable : demander `primary_country.iso3` peut rendre une liste de
        # chaînes, une liste de dicts, ou une chaîne seule selon le rapport.
        rec['primary_iso3'] = _iso_list(f.get('primary_country'))
        rec['country_relevant'] = (
            iso3.upper() in rec['primary_iso3']
            or _names_country(rec['title'], iso3)
            or _names_country(rec['body'][:1200], iso3))
        if rec['dedup_key'] in seen:
            dups.append(rec)
            continue
        seen.add(rec['dedup_key'])
        if not rec['country_relevant']:
            off_topic.append(rec)
            continue
        kept.append(rec)
        if len(kept) >= limit:
            break
    return {'reports': kept, 'duplicates': dups, 'off_topic': off_topic,
            'total': resp.get('totalCount', 0)}


def _iso_list(val):
    """Normalise un champ pays ReliefWeb en liste d'ISO3 majuscules."""
    if not val:
        return []
    if isinstance(val, str):
        return [val.upper()]
    out = []
    for v in (val if isinstance(val, (list, tuple)) else [val]):
        if isinstance(v, dict):
            code = v.get('iso3') or v.get('value') or ''
        else:
            code = str(v)
        if code:
            out.append(code.upper())
    return out


def _names_country(text, iso3):
    """Le texte nomme-t-il le pays ? (alias + libellé officiel DTM)"""
    names = list(_ALIASES.get((iso3 or '').upper(), []))
    lab = DTM_LABELS.get((iso3 or '').upper())
    if lab:
        names.append(lab)
    if not names:
        return False
    return bool(re.search(r'\b({})\b'.format('|'.join(re.escape(n) for n in names)),
                          text or '', re.I))


def report_text(report, cache_dir=None):
    """Texte exploitable d'un rapport : `body` s'il est fourni, sinon le PDF.

    ⚠ Le `body` du rapport le PLUS RÉCENT est souvent VIDE alors que le PDF est là
    (mesuré : les 2 derniers Flow Monitoring AFG et le round 109 LBN, 0 caractère).
    Ne jamais conclure « pas de chiffre publié » sur un body vide.

    Sur Windows le Read tool échoue sur les PDF (pas de Poppler) : on passe par
    `fitz` (PyMuPDF). S'il manque, on le dit au lieu de rendre du vide.
    """
    body = report.get('body') or ''
    if len(re.sub(r'<[^>]+>', '', body).strip()) > 200:
        return {'text': re.sub(r'<[^>]+>', ' ', body), 'origin': 'body ReliefWeb',
                'url': report.get('url', ''), 'pages': None, 'bytes': len(body)}

    pdfs = [x for x in (report.get('file') or [])
            if 'pdf' in str(x.get('mimetype', '')).lower()
            or str(x.get('filename', '')).lower().endswith('.pdf')]
    if not pdfs:
        return {'text': '', 'origin': 'aucun texte ni PDF', 'url': report.get('url', ''),
                'pages': None, 'bytes': 0}

    url, name = pdfs[0].get('url'), str(pdfs[0].get('filename', 'report.pdf'))
    path = None
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)
        path = os.path.join(cache_dir, re.sub(r'[<>:"/\\|?*]', '_', name)[:110])
    try:
        if path and os.path.exists(path):
            blob = open(path, 'rb').read()
        else:
            blob = urlopen(Request(url, headers={'User-Agent': USER_AGENT}),
                           timeout=180).read()
            if blob[:5] != b'%PDF-':
                return {'text': '', 'origin': 'pièce jointe non PDF ({} o)'.format(len(blob)),
                        'url': report.get('url', ''), 'pages': None, 'bytes': len(blob)}
            if path:
                open(path, 'wb').write(blob)
    except Exception as e:
        return {'text': '', 'origin': 'échec téléchargement PDF: {}'.format(str(e)[:70]),
                'url': report.get('url', ''), 'pages': None, 'bytes': 0}

    try:
        import fitz
    except ImportError:
        return {'text': '', 'origin': 'PDF présent mais PyMuPDF (fitz) absent: '
                                      'pip install pymupdf',
                'url': report.get('url', ''), 'pages': None, 'bytes': len(blob)}
    import io
    doc = fitz.open(stream=io.BytesIO(blob), filetype='pdf')
    txt = '\n'.join(p.get_text() for p in doc)
    pages = doc.page_count
    doc.close()
    return {'text': txt, 'origin': 'PDF ({} pages)'.format(pages),
            'url': (pdfs[0].get('url') or ''), 'pages': pages, 'bytes': len(blob)}


# Marqueurs de PORTÉE PARTIELLE. Une phrase qui en porte un ne décrit pas un total
# national, même si elle est datée, verbale et sourcée. C'est le filtre qui manquait :
# sur le Soudan, les 5 phrases les mieux notées étaient toutes des sous-ensembles ou
# des références historiques, pendant que le caseload national (8 805 506) était
# ailleurs. Sourcé + plausible + faux = exactement ce que ce module doit empêcher.
_SUBSET_GEO = re.compile(
    r'\b(region|regions|governorate|governorates|district|districts|town|towns|'
    r'village|villages|camp|camps|locality|localities|province|provinces|county|'
    r'counties|neighbourhood|sub-?district|from locations across the|state of)\b', re.I)
# Portée nationale explicite (hors nom de pays, testé séparément).
_SCOPE_NATIONAL = re.compile(
    r'\b(nationwide|countrywide|country-?wide|across the country|nationally|'
    r'at the national level|in the country)\b', re.I)
# Références au passé : « prior to », « pre-crisis », un millésime nettement antérieur.
_HISTORICAL = re.compile(
    r'\b(prior to|prior|before the|pre-?(crisis|conflict|war|2\d{3})|'
    r'previously|formerly|used to|baseline of|as of \d{1,2} \w+ 20(1\d|2[0-3]))\b', re.I)

try:                                   # libellés pays officiels, pour le test de portée
    from dtm_client import DTM_CATALOGUE_COUNTRIES
    DTM_LABELS = {k: v[0] for k, v in DTM_CATALOGUE_COUNTRIES.items()}
except Exception:
    DTM_LABELS = {}

_ALIASES = {
    'SYR': ['Syria', 'Syrian Arab Republic'], 'COD': ['DRC', 'Congo', 'DR Congo'],
    'SSD': ['South Sudan'], 'PSE': ['Palestine', 'oPt', 'Gaza', 'West Bank'],
    'LBN': ['Lebanon'], 'SDN': ['Sudan'], 'AFG': ['Afghanistan'],
    'YEM': ['Yemen'], 'UKR': ['Ukraine'], 'IRQ': ['Iraq'], 'SOM': ['Somalia'],
}


def _join_wrapped(text):
    """Recolle les lignes coupées par la mise en page avant de découper en phrases.

    Sans ça, on cite des bribes : « hosted an estimated 3,820,772 IDPs. After 15
    April 2023, Sudan quickly » n'est pas une citation, c'est un collage, et il perd
    le qualificatif qui disait que le chiffre est historique. Tout le module repose
    sur « citer la phrase telle quelle » : une bribe casse cette promesse.
    """
    lines = [l.strip() for l in re.sub(r'[ \t]+', ' ', text or '').split('\n')]
    out = []
    for line in lines:
        if not line:
            out.append('')
            continue
        if (out and out[-1] and not re.search(r'[.!?:;]$', out[-1])
                and re.match(r'^[a-z(\[]', line)):
            out[-1] = out[-1] + ' ' + line
        else:
            out.append(line)
    return '\n'.join(out)


def figure_sentences(text, min_value=1000, limit=25, iso3=None, country_label=None,
                     report_year=None):
    """Phrases porteuses de chiffres, classées par PORTÉE, à citer telles quelles.

    Écarte les bornes de légende de carte (« 25,001 – 75,000 »), les millésimes
    seuls, les lignes de nombres nus. Ne tente AUCUN appariement nombre↔libellé.

    Chaque phrase reçoit :
      scope      'national' (portée nationale explicite ou nom du pays, sans
                 marqueur de sous-ensemble) · 'subset' (région, ville, camp…) ·
                 'unclear' (ni l'un ni l'autre : ne pas présenter comme un total)
      historical True si la phrase renvoie à une période nettement antérieure
      truncated  True si la phrase ne finit pas par une ponctuation forte : elle
                 n'est PAS citable comme citation

    Seules les `scope == 'national'`, non historiques et non tronquées peuvent
    servir de chiffre de tête. Exemple de ce qu'on veut voir remonter :
      « As of 22 July 2026, IOM's DTM recorded 375,090 internally displaced
        persons (IDPs) across Lebanon, representing a nine per cent decrease
        compared to 15 July. »
    """
    names = list(_ALIASES.get((iso3 or '').upper(), []))
    if country_label:
        names.append(country_label)
    name_re = (re.compile(r'\b({})\b'.format('|'.join(re.escape(n) for n in names)), re.I)
               if names else None)

    chunks = []
    for line in _join_wrapped(text).split('\n'):
        line = line.strip()
        if not line:
            continue
        parts = re.split(r'(?<=[.!?])\s+', line) if len(line) > 120 else [line]
        chunks.extend(p.strip() for p in parts if p.strip())

    out, seen = [], set()
    for c in chunks:
        if _LEGEND.match(c):
            continue
        nums = [n for n in _NUMBER.findall(c) if not _YEARISH.match(n)]
        vals = [int(n.replace(',', '')) for n in nums if int(n.replace(',', '')) >= min_value]
        if not vals:
            continue
        letters = len(re.findall(r'[A-Za-z]', c))
        if letters < 12:
            continue
        key = (tuple(sorted(vals))[:3], c[:40].lower())
        if key in seen:
            continue
        seen.add(key)

        is_subset = bool(_SUBSET_GEO.search(c))
        names_country = bool(name_re.search(c)) if name_re else False
        if is_subset:
            scope = 'subset'
        elif _SCOPE_NATIONAL.search(c) or names_country:
            scope = 'national'
        else:
            scope = 'unclear'

        historical = bool(_HISTORICAL.search(c))
        if not historical and report_year:
            years = [int(y) for y in re.findall(r'\b(20[0-2]\d)\b', c)]
            if years and max(years) <= int(report_year) - 2:
                historical = True

        truncated = not re.search(r'[.!?]$', c)
        # Un pic, un record ou un plus-haut n'est pas le chiffre courant, même quand
        # la phrase est nationale et datée de cette année. Vécu Soudan : « the number
        # of IDPs reached a peak in January 2025, with an estimated 11,585,384 IDPs »
        # arrivait en tête, devant les 8 685 273 de fin juin 2026.
        superlative = bool(re.search(
            r'\b(peak|peaked|highest[- ]?ever|record high|all[- ]time|maximum|'
            r'at its highest)\b', c, re.I))
        has_date = bool(re.search(
            r'\b(as of|as at|between|during|since|\d{1,2}\s+\w+\s+20\d\d)\b', c, re.I))
        has_verb = bool(re.search(
            r'\b(recorded|reported|identified|estimated|tracked|displaced|returned|'
            r'assessed|observed|counted|remain|include[sd]?|represent\w*)\b', c, re.I))
        out.append({
            'sentence': c[:400],
            'figures': nums,
            'max_value': max(vals),
            'scope': scope,
            'historical': historical,
            'truncated': truncated,
            'superlative': superlative,
            'headline_ok': (scope == 'national' and not historical
                            and not truncated and not superlative),
            'score': (3 if scope == 'national' else (-3 if scope == 'subset' else 0))
                     + (-4 if historical else 0) + (-2 if truncated else 0)
                     + (-4 if superlative else 0)
                     + (2 if has_date else 0) + (2 if has_verb else 0)
                     + (1 if letters > 60 else 0),
        })
    out.sort(key=lambda r: (-r['score'], -r['max_value']))
    return out[:limit]


def render_report_pages(report, out_dir, pages=(0, 1), zoom=2.0, cache_dir=None):
    """Rend les premières pages d'un rapport PDF en PNG, pour LECTURE VISUELLE.

    Pourquoi. Une part écrasante des produits DTM sont des **infographies** (2 035
    des 2 375 entrées du catalogue sont typées « Infosheet »), pas de la prose. Dans
    une infographie, le nombre et son libellé sont dans des blocs séparés, et
    l'appariement automatique échoue de façon non détectable. Mesuré sur le Flow
    Monitoring Update afghan (page 1, 79 blocs) :
      - `120,099` est DANS un bloc qui contient les DEUX libellés
        « TOTAL OUTFLOWS | TOTAL INFLOWS » : ambigu même à l'intérieur du bloc
      - le plus proche voisin de `38,979` est « >5 YRS 86% », qui est faux
      - le plus proche voisin de `6,039` est « 10% », une annotation, pas un libellé
    Le plus proche voisin géométrique n'est donc pas le libellé. En revanche
    l'infographie est lisible par un lecteur : on rend la page et on la LIT.

    Retourne la liste des PNG écrits. L'appelant doit les ouvrir et lire les
    chiffres avec leur libellé, puis citer « libellé = valeur, page N du rapport ».
    """
    pdfs = [x for x in (report.get('file') or [])
            if 'pdf' in str(x.get('mimetype', '')).lower()
            or str(x.get('filename', '')).lower().endswith('.pdf')]
    if not pdfs:
        return []
    try:
        import fitz
    except ImportError:
        print('  PyMuPDF (fitz) absent : pip install pymupdf')
        return []
    import io
    # Réutiliser le PDF que report_text a déjà mis en cache : sans ça on retélécharge
    # ~1 Mo par rapport et par appel.
    cached = None
    if cache_dir:
        cached = os.path.join(cache_dir, re.sub(
            r'[<>:"/\\|?*]', '_', str(pdfs[0].get('filename', '')))[:110])
    try:
        if cached and os.path.exists(cached):
            blob = open(cached, 'rb').read()
        else:
            blob = urlopen(Request(pdfs[0]['url'], headers={'User-Agent': USER_AGENT}),
                           timeout=180).read()
    except Exception as e:
        print('  rendu PDF: {}'.format(str(e)[:80]))
        return []
    if blob[:5] != b'%PDF-':
        return []
    os.makedirs(out_dir, exist_ok=True)
    stem = re.sub(r'[<>:"/\\|?*]', '_', os.path.splitext(
        str(pdfs[0].get('filename', 'report')))[0])[:70]
    doc = fitz.open(stream=io.BytesIO(blob), filetype='pdf')
    # Un PDF chiffré ou corrompu fait lever fitz : un seul mauvais rapport ne doit
    # pas faire tomber tout le dossier (vécu sur le chemin UNHCR).
    out = []
    try:
        for i in pages:
            if i >= doc.page_count:
                break
            pix = doc[i].get_pixmap(matrix=fitz.Matrix(zoom, zoom))
            path = os.path.join(out_dir, '{}_p{}.png'.format(stem, i + 1))
            pix.save(path)
            out.append(path)
    except Exception as e:
        print('  rendu page: {}'.format(str(e)[:80]))
    finally:
        doc.close()
    return out


# Un produit analytique se lit autrement qu'une fiche de chiffres. Marqueurs de la
# section a puces des Briefing Notes / Thematic Reports ACAPS.
_FINDINGS_HEAD = re.compile(r'\*\*\s*(key findings?|main findings?|key messages?|'
                            r'anticipated impacts?|key points?)\s*\*\*', re.I)
_BULLET = re.compile(r'(?:^|\s)[•·▪–—]\s*')
# Citation en ligne : « (OCHA 29/06/2026; El Tiempo 29/06/2026) ». C'est la signature
# du travail ACAPS : chaque affirmation porte ses sources.
_INLINE_CITE = re.compile(r'\(([^()]{0,200}?\d{2}/\d{2}/\d{4}[^()]{0,200}?)\)')
# Boilerplate de methode / d'avertissement : ce n'est pas un constat. Vecu sur l'ACAPS
# Sudan Risk Analysis, dont les "constats" extraits etaient la description de la
# methodologie ("the ACAPS team reviewed publicly available data...").
_BOILERPLATE = re.compile(
    r'(secondary data review|key informant interviews?|external reviews?|'
    r'this (?:report|analysis|note) (?:is|was|aims|provides)|'
    r'methodolog\w+|limitations?|about this report|acaps (?:team|analysts?) '
    r'(?:reviewed|conducted)|for more information|feedback|disclaimer|'
    r'copyright|creative commons|subscribe)', re.I)
# Groupes de population nommes : c'est la que le handicap apparait quand il apparait.
_GROUPS = re.compile(
    r'\b(persons? with disabilit\w+|people with disabilit\w+|disabilit\w+|'
    r'older (?:persons?|people)|elderly|children|women|girls|boys|'
    r'indigenous (?:communities|peoples?)|migrants?|refugees?|'
    r'internally displaced|IDPs?|female[- ]headed households?|'
    r'pregnant|lactating|unaccompanied minors?|host communit\w+)\b', re.I)


def analytical_findings(iso3, source='ACAPS', topic='acaps', cache_dir=None,
                        max_reports=3):
    """Constats d'un produit ANALYTIQUE (ACAPS et assimiles).

    Pourquoi une fonction separee de `latest_figures`. Un produit DTM repond
    « combien » ; un produit ACAPS repond « pourquoi, pour qui, et ce qui risque
    d'arriver ». Deux consequences mesurees le 2026-07-25 :

    1. **La valeur est souvent dans des enonces SANS CHIFFRE.** Une Briefing Note est
       structuree en `**Key findings**` a puces : « The 2026 elections pose high risks
       of armed coercion », « Shifting political dynamics are triggering spillover
       effects ». `figure_sentences()` les rate TOUS puisqu'il exige un nombre.
    2. **Chaque affirmation porte ses sources EN LIGNE** : « at least 2,595 people had
       died, over 12,400 had been injured (OCHA 29/06/2026; El Tiempo 29/06/2026;
       IFRC 26/06/2026; CNN; The Guardian) ». C'est un cadeau pour la tracabilite :
       la provenance voyage avec la phrase. Un produit ACAPS est donc deja une
       TRIANGULATION de plusieurs sources, ce qui est sa valeur propre.

    ⚠ **STATUT EPISTEMIQUE : `analysis`, pas `measurement`.** Un constat ACAPS est
    l'interpretation d'un tiers, parfois prospective. Le presenter avec la meme
    autorite qu'un « DTM recorded 375,090 » serait une faute. Le dict retourne porte
    `epistemic_status` et chaque constat porte `attribution` : citer « ACAPS estime
    que... », jamais « il y a... ».

    Retourne {'iso3', 'source', 'epistemic_status', 'products': [...], 'caveats': [...]}
    ou chaque produit a : title, date, url, key_findings[], cited_figures[],
    population_groups[], mentions_disability.
    """
    iso3 = iso3.upper()
    rw = latest_reports(iso3, topic=topic, source=source, limit=max_reports)
    out = {'iso3': iso3, 'source': source,
           'epistemic_status': 'analysis (interpretation de tierce partie, pas une '
                               'mesure) — attribuer explicitement, ne jamais servir '
                               'comme un chiffre observe',
           'products_indexed': rw['total'], 'products': [], 'caveats': []}
    if rw.get('off_topic'):
        out['caveats'].append('{} produit(s) ecarte(s) : ne nomment pas {}'.format(
            len(rw['off_topic']), iso3))

    for rep in rw['reports']:
        # ⚠ Pour ACAPS, le `body` ReliefWeb est une AMORCE, pas le rapport. Mesure :
        # le Thematic Report Venezuela sur le seisme fait 1 658 caracteres de body
        # (l'introduction) alors que l'analyse, les groupes affectes et les constats
        # sont dans le PDF. `report_text` prefere le body des qu'il depasse 200
        # caracteres : ici on force le PDF quand le body est visiblement tronque.
        got = report_text(rep, cache_dir=cache_dir)
        body_len = len(re.sub(r'<[^>]+>', '', rep.get('body') or '').strip())
        has_pdf = any('pdf' in str(x.get('mimetype', '')).lower()
                      for x in (rep.get('file') or []))
        teaser = False
        if got['origin'].startswith('body') and body_len < 4000 and has_pdf:
            stub = dict(rep)
            stub['body'] = ''            # force le chemin PDF
            deeper = report_text(stub, cache_dir=cache_dir)
            if len(deeper['text']) > len(got['text']) * 1.5:
                got, teaser = deeper, True
        text = got['text']
        if not text:
            out['caveats'].append('{} : {}'.format(rep['title'][:50], got['origin']))
            continue
        flat = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', text))

        # Constats a puces : on part du titre de section quand il existe, sinon on
        # prend les puces du debut du document (le resume executif).
        m = _FINDINGS_HEAD.search(flat)
        zone = flat[m.end():m.end() + 4000] if m else flat[:3000]
        findings = []
        for chunk in _BULLET.split(zone)[1:]:
            chunk = chunk.strip(' *•–—')
            if 40 < len(chunk) < 700 and not _BOILERPLATE.search(chunk[:120]):
                findings.append(chunk[:500])
        # Repli : pas de puces (Thematic Report en prose) -> phrases analytiques
        if not findings:
            for sent in re.split(r'(?<=[.!])\s+', zone):
                if 60 < len(sent) < 500 and re.search(
                        r'\b(likely|expected|anticipat\w+|risk\w*|could|may|would|'
                        r'increas\w+|deteriorat\w+|driver|because|due to)\b', sent, re.I):
                    findings.append(sent.strip()[:500])
                if len(findings) >= 8:
                    break

        cited = [{'sentence': s['sentence'], 'figures': s['figures'],
                  'inline_sources': _INLINE_CITE.findall(s['sentence'])[:3]}
                 for s in figure_sentences(flat, limit=8, iso3=iso3,
                                           report_year=(rep['date'][:4] or None))]
        groups = sorted({g.lower() for g in _GROUPS.findall(flat)})
        out['products'].append({
            'title': rep['title'],
            'date': rep['date'],
            'url': rep['url'],
            'formats': rep['formats'],
            'origin': got['origin'],
            # `body_was_teaser` = le resume ReliefWeb etait tronque, on a lu le PDF.
            # A dire : la profondeur de lecture change ce qu'on peut affirmer.
            'body_was_teaser': teaser,
            'chars_read': len(flat),
            'key_findings': findings[:10],
            'cited_figures': cited,
            'population_groups': groups,
            'mentions_disability': any('disabilit' in g for g in groups),
            'attribution': '{} — {} ({})'.format(source, rep['title'][:70], rep['date']),
        })

    if not out['products']:
        out['caveats'].append('aucun produit analytique exploitable : le dire, ne pas '
                              'combler avec une interpretation maison')
    else:
        out['caveats'].append(
            'ACAPS mentionne le handicap dans ~3,0 % de ses produits (61 sur 2 039), '
            'contre 1,9 % toutes sources ReliefWeb : mieux que la moyenne, marginal '
            'quand meme. Ne pas conclure a une absence sur un seul produit.')
    return out


def latest_figures(iso3, topic='dtm', cache_dir=None, max_reports=3,
                   render_dir=None):
    """Dossier « dernier chiffre » : ce que dit chaque couche, et de quand ça date.

    NE CHOISIT PAS de chiffre. Rend :
      freshness : ce que chaque couche propose et sa date (pour comparer)
      citable   : phrases porteuses de chiffres, avec leur source et leur date
      gated     : si la couche C est verrouillée, le dire et donner le contact
      caveats   : ce qu'il ne faut pas conclure

    L'appelant cite une phrase de `citable` en la reproduisant, avec sa date et son
    URL. Un chiffre sans sa phrase ne doit pas sortir d'ici.
    """
    iso3 = iso3.upper()
    spec = TOPICS.get(topic, TOPICS['dtm'])
    # `citable` ne contient QUE des phrases de portée nationale, non historiques et
    # non tronquées. Tout le reste va dans `other_sentences` : utile pour du détail
    # local, jamais présentable comme un total.
    dossier = {'iso3': iso3, 'topic': topic, 'topic_label': spec['label'],
               'freshness': {}, 'citable': [], 'other_sentences': [],
               'gated': None, 'caveats': []}

    # ── Couche D : la publication (la plus fraîche en chiffres) ──
    rw = latest_reports(iso3, topic=topic, limit=max_reports)
    dossier['freshness']['D_publication'] = {
        'source': 'ReliefWeb',
        'reports_indexed': rw['total'],
        'latest_date': rw['reports'][0]['date'] if rw['reports'] else None,
        'latest_title': rw['reports'][0]['title'] if rw['reports'] else None,
        'duplicates_dropped': len(rw['duplicates']),
    }
    if rw['duplicates']:
        dossier['caveats'].append(
            '{} rapport(s) doublon écarté(s) : ReliefWeb réindexe les versions '
            'corrigées, et le titre peut porter une plage de dates fausse.'
            .format(len(rw['duplicates'])))

    dossier['visual_read'] = []
    for rep in rw['reports']:
        got = report_text(rep, cache_dir=cache_dir)
        if not got['text']:
            dossier['caveats'].append('{} : {}'.format(rep['title'][:52], got['origin']))
            continue
        sents = figure_sentences(
            got['text'], limit=12, iso3=iso3,
            country_label=(DTM_LABELS.get(iso3) if DTM_LABELS else None),
            report_year=(rep['date'][:4] if rep['date'] else None))
        for s in sents:
            row = {
                'sentence': s['sentence'],
                'figures': s['figures'],
                'as_of': rep['date'],
                'report': rep['title'],
                'origin': got['origin'],
                'url': rep['url'] or got['url'],
                'scope': s['scope'],
                'historical': s['historical'],
                'truncated': s['truncated'],
                'score': s['score'],
            }
            (dossier['citable'] if s['headline_ok']
             else dossier['other_sentences']).append(row)
        # Produit INFOGRAPHIQUE, ou rapport long sans phrase de portée nationale :
        # dans les deux cas on ne devine pas, on rend la page pour lecture.
        headline_here = [s for s in sents if s['headline_ok']]
        if not headline_here and _NUMBER.search(got['text'] or ''):
            pngs = (render_report_pages(rep, render_dir, pages=(0, 1),
                                        cache_dir=cache_dir) if render_dir else [])
            dossier['visual_read'].append({
                'report': rep['title'], 'as_of': rep['date'],
                'url': rep['url'], 'images': pngs,
                'why': ('aucune phrase de portée NATIONALE, non historique et non '
                        'tronquée. Soit une infographie (nombre et libellé dans des '
                        'blocs séparés), soit un rapport long dont les phrases '
                        'décrivent des sous-ensembles. Lire les images ; ne PAS '
                        'apparier automatiquement.'),
                'rejected': [{'sentence': s['sentence'][:150], 'scope': s['scope'],
                              'historical': s['historical'], 'truncated': s['truncated']}
                             for s in sents[:5]],
            })
    dossier['citable'].sort(key=lambda r: (r['as_of'], r['score']), reverse=True)

    # ── Couche C : le portail producteur (les fichiers) ──
    if spec.get('catalogue') == 'dtm':
        try:
            from dtm_client import DTMClient, UnmappedCountry
            dtm = DTMClient()
            try:
                summary = dtm.catalogue_summary(iso3)
                latest, latest_open = summary['latest'], summary['latest_open']
                dossier['freshness']['C_portal'] = {
                    'source': 'dtm.iom.int/datasets',
                    'datasets_seen': summary['datasets_seen'],
                    'open': summary['open'], 'gated': summary['gated'],
                    'latest_date': latest['published'] if latest else None,
                    'latest_title': latest['title'] if latest else None,
                    'latest_access': latest['access'] if latest else None,
                    'latest_open_date': latest_open['published'] if latest_open else None,
                    'download_url': (latest_open or {}).get('download_url') or None,
                }
                if latest and latest['access'] == 'gated':
                    dossier['gated'] = {
                        'dataset': latest['title'],
                        'url': latest['url'],
                        'message': ('le fichier le plus récent est derrière '
                                    '« Request access » : le CHIFFRE reste publié '
                                    'dans le rapport, seule la granularité fine '
                                    '(admin2/admin3) demande une autorisation'),
                        'how_to_ask': 'contact de la mission sur la fiche du dataset',
                    }
            except UnmappedCountry as e:
                dossier['freshness']['C_portal'] = {'unavailable': str(e)[:150]}
            except Exception as e:
                dossier['freshness']['C_portal'] = {'error': str(e)[:150]}

            # ── Couche A : l'API du producteur (structurée, souvent en retard) ──
            # try SÉPARÉ : sans ça, l'échec de A écrasait le résultat de C. Vécu sur
            # PSE (hors des 56 pays du catalogue) : C écrivait proprement
            # « unavailable: UnmappedCountry », puis l'API v3 levait « No Country Code
            # found matching your query » et le message brut de A remplaçait C.
            try:
                if not dtm.has_api_access():
                    raise NotImplementedError('pas de clé configurée')
                av = dtm.get_availability(iso3)
                # get_availability n'expose pas de `latest_date` à plat : la date vit
                # dans `latest['date_to']` (par opération, jamais max(round) — le
                # roundNumber est propre à chaque opération).
                latest_op = av.get('latest') or {}
                dossier['freshness']['A_producer_api'] = {
                    'source': 'DTM API v3',
                    'rows': av.get('rows'),
                    'latest_date': latest_op.get('date_to'),
                    'operation': latest_op.get('operation'),
                    'round_max': latest_op.get('round_max'),
                    'idps_latest_snapshot': latest_op.get('idps_latest_snapshot'),
                    'age_months': av.get('age_months'),
                    'stale': av.get('stale'),
                    'cadence_days': latest_op.get('cadence_days'),
                    'operations': av.get('n_operations'),
                }
                if av.get('stale'):
                    dossier['caveats'].append(
                        'API v3 en retard de {} mois (dernière donnée {}, cadence '
                        '~{} j) : son chiffre {} n\'est PAS le courant.'.format(
                            av.get('age_months'), latest_op.get('date_to'),
                            latest_op.get('cadence_days'),
                            latest_op.get('idps_latest_snapshot')))
            except NotImplementedError as e:
                dossier['freshness']['A_producer_api'] = {
                    'source': 'DTM API v3', 'unavailable': str(e)[:90]}
            except Exception as e:
                dossier['freshness']['A_producer_api'] = {
                    'source': 'DTM API v3', 'error': str(e)[:150]}
        except Exception as e:
            dossier['freshness']['C_portal'] = {'error': str(e)[:120]}

    # ── Contrôle d'ordre de grandeur entre couches ──
    # Une phrase peut être nationale, datée, non tronquée et malgré tout absurde.
    # Vécu Soudan : « The number of estimated individuals in Sudan decreased by
    # 4,643,703 IDPs compared to the end of May 2026 » alors que le stock est de
    # 8,7 M et la variation mensuelle réelle de ~120 k. On ne supprime pas la phrase
    # (elle est peut-être mal découpée à la source), on prévient.
    # ⚠ La référence n'est utilisable que si la couche A est À JOUR. Sinon l'écart est
    # ATTENDU et le signaler serait trompeur : sur le Liban, A dit 64 417 (oct. 2025)
    # contre 375 090 publiés en juillet 2026, et les 3 bonnes phrases se faisaient
    # signaler comme incohérentes alors que c'est A qui est périmée.
    layer_a = dossier['freshness'].get('A_producer_api') or {}
    ref = 0 if layer_a.get('stale') else (layer_a.get('idps_latest_snapshot') or 0)
    if layer_a.get('stale') and layer_a.get('idps_latest_snapshot'):
        dossier['caveats'].append(
            'Contrôle d\'ordre de grandeur DÉSACTIVÉ : la couche A est périmée '
            '({} mois), son chiffre ne peut pas servir de référence.'
            .format(layer_a.get('age_months')))
    if ref:
        for c in dossier['citable']:
            top = max((int(f.replace(',', '')) for f in c['figures']), default=0)
            if not top:
                continue
            # (a) Un TOTAL qui dépasse 2,5x la référence ne peut pas être un total.
            #     (Un écart vers le bas n'est PAS suspect : un sous-total légitime est
            #     plus petit. Ne flaguer que le haut.)
            if top > ref * 2.5:
                c['magnitude_flag'] = (
                    'total incohérent avec la couche A ({:,} contre {:,})'
                    .format(top, ref))
            # (b) Une VARIATION période-sur-période qui pèse plus du quart du stock
            #     contredit la série elle-même. Vécu SDN : « The number of estimated
            #     individuals in Sudan decreased by 4,643,703 IDPs compared to the end
            #     of May 2026 » alors que le stock passe de 8 805 506 à 8 685 273, soit
            #     ~120 k. Le chiffre est d'ailleurs proche des 4 649 056 RETOURS du même
            #     rapport : extraction qui mêle deux métriques. Cette phrase est
            #     nationale, datée, non tronquée, non superlative : aucun autre filtre
            #     ne l'attrape.
            elif re.search(r'\b(decreas\w*|increas\w*|declin\w*|dropp?\w*|rose|fell|'
                           r'grew|reduc\w*|chang\w*|more|fewer|less)\b.{0,40}?\bby\b|'
                           r'\bby\b.{0,20}?\b(decreas|increas)', c['sentence'], re.I) \
                    and top > ref * 0.25:
                c['magnitude_flag'] = (
                    'variation invraisemblable : {:,} annoncés comme un changement '
                    'alors que le stock de référence est {:,} (soit {:.0f} % du '
                    'stock en une période)'.format(top, ref, top / ref * 100))
        # ⚠ Une phrase signalée est SORTIE de `citable`, pas seulement étiquetée.
        # Sans ça, « The number of estimated individuals in Sudan decreased by
        # 4,643,703 IDPs compared to the end of May 2026 » restait classée et
        # citable, alors que la variation réelle du mois est de ~120 k : nationale,
        # datée, non tronquée, et fausse de ~38x. Un drapeau qu'on peut ne pas lire
        # ne protège de rien.
        flagged = [c for c in dossier['citable'] if c.get('magnitude_flag')]
        if flagged:
            dossier['citable'] = [c for c in dossier['citable']
                                  if not c.get('magnitude_flag')]
            dossier['other_sentences'].extend(flagged)
            dossier['caveats'].append(
                '{} phrase(s) ÉCARTÉE(S) de citable : chiffre incohérent avec la '
                'couche A ({:,}). Elles sont dans other_sentences avec '
                'magnitude_flag.'.format(len(flagged), ref))

    # ── Verdict de fraîcheur : quelle couche est en tête ──
    dates = {k: v.get('latest_date') for k, v in dossier['freshness'].items()
             if isinstance(v, dict) and v.get('latest_date')}
    if len(dates) > 1:
        dossier['freshest_layer'] = max(dates, key=lambda k: _sortable(dates[k]))
        dossier['caveats'].append(
            'Fraîcheur par couche : {}. Ne pas servir le chiffre d\'une couche en '
            'retard comme s\'il était courant.'.format(
                ', '.join('{}={}'.format(k.split('_')[0], v) for k, v in dates.items())))
    elif dates:
        # Une seule couche a répondu : il n'y a eu AUCUN recoupement. Le dire, au lieu
        # de laisser croire qu'une comparaison a eu lieu.
        only = list(dates)[0]
        dossier['freshest_layer'] = only
        dossier['caveats'].append(
            'UNE SEULE couche a répondu ({}={}) : aucun recoupement de fraîcheur '
            'n\'a été possible, ne pas présenter ce chiffre comme triangulé.'
            .format(only.split('_')[0], dates[only]))
    if rw.get('off_topic'):
        dossier['caveats'].append(
            '{} rapport(s) écarté(s) car ne nommant pas {} : ReliefWeb tague un '
            'produit avec TOUS les pays qu\'il mentionne (une requête PSE ramenait '
            '« DTM Montenegro: Migration Data and Routes »).'
            .format(len(rw['off_topic']), iso3))
    if not dossier['citable'] and not dossier['visual_read']:
        dossier['caveats'].append(
            'Aucune phrase chiffrée extraite : dire « pas de chiffre publié trouvé '
            'par ce chemin », jamais un nombre approché.')
    elif not dossier['citable']:
        dossier['caveats'].append(
            'Aucune phrase citable : le(s) produit(s) le(s) plus récent(s) sont des '
            'infographies. Lire les images de `visual_read` avant de citer un '
            'chiffre, et nommer la page d\'où il vient.')
    return dossier


_MONTHS = {m: i for i, m in enumerate(
    ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
     'jul', 'aug', 'sep', 'oct', 'nov', 'dec'], 1)}


def _sortable(d):
    """Trie « 2026-07-23 » et « Jul 21 2026 » sur la même échelle.

    Les dates du catalogue DTM sont lisibles (« Jul 21 2026 ») et ne se trient PAS
    lexicographiquement, piège déjà payé sur les dates de soumissions SurveyCTO.
    """
    d = str(d or '')
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', d)
    if m:
        return tuple(int(x) for x in m.groups())
    m = re.match(r'([A-Za-z]{3})\w*\s+(\d{1,2}),?\s+(\d{4})', d)
    if m:
        return (int(m.group(3)), _MONTHS.get(m.group(1).lower(), 0), int(m.group(2)))
    return (0, 0, 0)


if __name__ == '__main__':
    iso = sys.argv[1] if len(sys.argv) > 1 else 'LBN'
    tp = sys.argv[2] if len(sys.argv) > 2 else 'dtm'
    cache = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         '..', '..', '_report_cache')
    d = latest_figures(iso, topic=tp, cache_dir=cache,
                       render_dir=os.path.join(cache, 'pages'))
    print('=' * 78)
    print('{} — {}'.format(d['iso3'], d['topic_label']))
    print('=' * 78)
    print('\nFRAÎCHEUR PAR COUCHE')
    for layer, info in d['freshness'].items():
        print('  {:18} {}'.format(layer, json.dumps(info, ensure_ascii=False)[:190]))
    print('\n  couche la plus fraîche :', d.get('freshest_layer', '?'))
    if d['gated']:
        print('\nVERROU')
        print('  {}'.format(d['gated']['dataset'][:70]))
        print('  {}'.format(d['gated']['message']))
    print('\nPHRASES CITABLES ({})'.format(len(d['citable'])))
    for c in d['citable'][:7]:
        print('\n  [{}] {}'.format(c['as_of'], c['origin']))
        print('  « {} »'.format(c['sentence'][:240]))
    if d.get('visual_read'):
        print('\nÀ LIRE VISUELLEMENT ({} produit(s) infographiques)'.format(
            len(d['visual_read'])))
        for v in d['visual_read']:
            print('\n  [{}] {}'.format(v['as_of'], v['report'][:66]))
            for p in v['images']:
                print('     image : {}'.format(p))
            print('     {}'.format(v['why'][:150]))
    print('\nRÉSERVES')
    for c in d['caveats']:
        print('  - {}'.format(c[:170]))
