"""
Explorer un pays en moins d'une minute
======================================
    python -X utf8 scripts/explore.py SDN
    python -X utf8 scripts/explore.py LBN --topic dtm

La porte d'entree du toolkit. Repond a quatre questions dans l'ordre, et s'arrete la :

    1. Qu'est-ce qui EXISTE pour ce pays ?          (catalogue, volumes, acces)
    2. Quelle est la FRAICHEUR de chaque couche ?   (et laquelle est en retard)
    3. Quel est le dernier chiffre, CITE ?          (phrase + date + url)
    4. Qu'est-ce qui MANQUE ?                       (l'absence est un resultat)

Pourquoi ce script existe. Le point d'entree documente jusqu'ici (`01_fetch.py UKR
--only impact,liveuamap`) echouait a la mise en route : IMPACT est bloque sur certains
reseaux, et Liveuamap parcourt 200 pages, soit ~13 minutes. Un nouvel arrivant
abandonnait avant de voir quoi que ce soit. Ici : aucune ecriture de fichier, aucune
cle obligatoire, et une reponse en 30 a 60 secondes.

Ce script NE TELECHARGE RIEN et n'ecrit aucun fichier. Il montre ce qu'il y a et dit
comment aller le chercher. Pour recuperer les donnees : `fetch_country_data.py` ou
`DTMClient.download_dataset`.
"""
import sys
import os
import time

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, 'clients'))


def hr(title):
    print()
    print('=' * 78)
    print(title)
    print('=' * 78)


def explore(iso3, topic='dtm'):
    from config import normalize_iso3
    iso3 = normalize_iso3(iso3)
    t0 = time.time()
    print('EXPLORATION — {} (sujet : {})'.format(iso3, topic))

    # ── 1. Ce qui existe ────────────────────────────────────
    hr('1. QU\'EST-CE QUI EXISTE')
    try:
        from dtm_client import DTMClient, UnmappedCountry
        dtm = DTMClient()
        try:
            s = dtm.catalogue_summary(iso3)
            print('  Catalogue IOM DTM : {} datasets vus, {} telechargeables, {} '
                  'verrouilles'.format(s['datasets_seen'], s['open'], s['gated']))
            if s.get('note'):
                print('    (note : {})'.format(s['note']))
            if s['latest']:
                print('    plus recent : {} | {}'.format(
                    s['latest']['published'], s['latest']['title'][:58]))
                print('    acces       : {}'.format(s['latest']['access']))
            acts = list(s['activities'].items())[:5]
            if acts:
                print('    activites   : {}'.format(
                    ', '.join('{} ({})'.format(k, v) for k, v in acts)))
        except UnmappedCountry:
            print('  Catalogue IOM DTM : ce pays n\'a pas de facette DTM (sur 56). '
                  'Aucune donnee DTM a attendre.')
    except Exception as e:
        print('  Catalogue IOM DTM : indisponible ({})'.format(str(e)[:60]))

    # ── 2/3. Fraicheur par couche + le chiffre cite ─────────
    hr('2. FRAICHEUR PAR COUCHE  (le plus frais n\'est jamais le plus structure)')
    dossier = None
    try:
        from report_figures import latest_figures
        dossier = latest_figures(iso3, topic=topic)
        for layer, info in dossier['freshness'].items():
            if not isinstance(info, dict):
                continue
            name = {'A_producer_api': 'A. API du producteur',
                    'C_portal': 'C. Portail (fichiers)',
                    'D_publication': 'D. Publication (rapports)'}.get(layer, layer)
            if info.get('unavailable') or info.get('error'):
                print('  {:26} indisponible : {}'.format(
                    name, str(info.get('unavailable') or info.get('error'))[:44]))
                continue
            extra = ''
            if info.get('stale'):
                extra = '  ← EN RETARD de {} mois'.format(info.get('age_months'))
            print('  {:26} {}{}'.format(name, info.get('latest_date') or '?', extra))
        if dossier.get('freshest_layer'):
            print('  → la plus fraiche : {}'.format(dossier['freshest_layer']))
    except Exception as e:
        print('  resolveur indisponible : {}'.format(str(e)[:70]))

    hr('3. LE DERNIER CHIFFRE, CITE')
    if dossier and dossier.get('citable'):
        for c in dossier['citable'][:2]:
            print()
            print('  « {} »'.format(c['sentence'][:250]))
            print('     {} | {} | {}'.format(c['as_of'], c['origin'], c['url'][:64]))
        if dossier.get('gated'):
            print()
            print('  ⚠ VERROU : {}'.format(dossier['gated']['message'][:150]))
    elif dossier and dossier.get('visual_read'):
        print('  Aucune phrase citable : les produits recents sont des infographies.')
        for v in dossier['visual_read'][:2]:
            print('    [{}] {}'.format(v['as_of'], v['report'][:60]))
        print('  → rendre les pages en PNG et les lire (render_dir=...), ne PAS')
        print('    apparier automatiquement les nombres.')
    else:
        print('  Aucun chiffre publie trouve par ce chemin. Le dire tel quel :')
        print('  ne pas approcher un ordre de grandeur.')

    # ── 4. Ce qui manque ────────────────────────────────────
    hr('4. CE QUI MANQUE  (l\'absence est un resultat)')
    if dossier:
        for cav in dossier.get('caveats', [])[:4]:
            print('  - {}'.format(cav[:160]))
    print('  - Handicap : 2 sources sur 17 seulement en portent (HAPI pour les')
    print('    effectifs, ACAPS pour la qualification des incidents). Verifier')
    print('    HAPI humanitarian-needs avant de conclure a une absence.')

    hr('POUR ALLER PLUS LOIN')
    print('  telecharger un fichier   : DTMClient().download_dataset(row, dest)')
    print('  lire un fichier          : dtm_files.describe / read_sheet / checksum')
    print('  analyse d\'une crise      : report_figures.analytical_findings(iso3)')
    print('  toutes les sources       : python -X utf8 scripts/fetch_country_data.py {}'
          .format(iso3))
    print('  etat des 17 sources      : python -X utf8 scripts/health_check.py {}'
          .format(iso3))
    print()
    print('  ({:.0f} secondes)'.format(time.time() - t0))


def main(argv):
    args = [a for a in argv if not a.startswith('--')]
    topic = 'dtm'
    for i, a in enumerate(argv):
        if a == '--topic' and i + 1 < len(argv):
            topic = argv[i + 1]
    if not args:
        print(__doc__)
        print('Pays disponibles pour le catalogue DTM : 56 (voir '
              'DTM_CATALOGUE_COUNTRIES).')
        print('Sujets : dtm (defaut), unhcr, ocha, acaps')
        return 1
    explore(args[0], topic=topic)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
