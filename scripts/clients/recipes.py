"""
Recipes — les questions types, en une fonction, avec leurs garde-fous
=====================================================================
Chaque recipe repond a UNE question qu'un IM/MEAL pose vraiment, et rend TOUJOURS
le meme contrat :

    {'value'/'rows', 'vintage', 'source', 'caveats': [...], 'method': '...'}

- `value` / `rows` : le resultat.
- `vintage` : la date de reference. Un chiffre sans millesime n'est pas citable.
- `source` : d'ou il vient.
- `caveats` : ce qu'il ne faut PAS conclure.
- `method` : comment il a ete calcule (pour qu'on puisse le refaire).

Les recipes encodent les regles apprises a la dure (grille HI + gotchas) : ne pas
sommer un total national HAPI (double etiquette), ne pas confondre projection et
courant (IPC), attribuer une analyse au lieu de la servir comme mesure.

Usage :
    from recipes import get_pin, get_disability_pin, get_funding, get_conflict, \\
                        get_3w, get_severity, neglect_index
    r = get_pin('SDN'); print(r['value'], r['vintage'], r['caveats'])
"""
import sys
import os

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)


def _num(v):
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


# ─────────────────────────── BESOINS ───────────────────────────
def get_pin(iso3):
    """People in Need national, dernier millesime, SANS somme.

    ⚠ HAPI publie le total national sous DEUX etiquettes de `category` (`''` et
    `'total'`) : sommer double le chiffre une annee sur deux. `national_total`
    choisit UNE etiquette et verifie que le compte vaut 1.
    """
    from hapi_client import HAPIClient, national_total
    hn = HAPIClient().get_humanitarian_needs(iso3)
    if not hn:
        return {'value': None, 'vintage': None, 'source': 'HDX HAPI humanitarian-needs',
                'caveats': ['aucune donnee HNO pour ' + iso3],
                'method': 'get_humanitarian_needs'}
    nt = national_total(hn)
    if not nt:
        return {'value': None, 'vintage': None, 'source': 'HDX HAPI humanitarian-needs',
                'caveats': ['pas de ligne total national (admin0/Intersectoral/INN)'],
                'method': 'national_total'}
    cav = ['ne PAS sommer les lignes HAPI : le total est publie sous 2 etiquettes '
           'de category (double comptage).']
    if nt.get('warning'):
        cav.append(nt['warning'])
    return {'value': int(nt['value']) if nt['value'] else None,
            'vintage': nt['period'], 'source': 'HDX HAPI humanitarian-needs (HNO)',
            'caveats': cav,
            'method': 'admin0 + Intersectoral + INN + category={} , derniere periode'
                      .format(nt['label_used'])}


def get_disability_pin(iso3, admin_level='2'):
    """PiN handicap, somme au niveau admin demande, dernier millesime.

    Le seul chiffre d'EFFECTIF de personnes handicapees qu'une des 17 sources
    fournisse (HAPI). Matcher tolerant `'disab' in category` (les labels varient
    par millesime HNO). La somme admin2 fait foi (l'admin0 peut differer de
    quelques unites par arrondi).
    """
    from hapi_client import HAPIClient
    hn = HAPIClient().get_humanitarian_needs(iso3)
    dis = [r for r in hn
           if 'disab' in str(r.get('category', '')).lower()
           and str(r.get('population_status', '')) == 'INN'
           and str(r.get('sector_name', '')).lower().startswith('intersect')
           and str(r.get('admin_level', '0')) == str(admin_level)]
    if not dis:
        return {'value': None, 'vintage': None,
                'source': 'HDX HAPI humanitarian-needs',
                'caveats': ['pas de ventilation handicap a admin{} pour {}'.format(
                    admin_level, iso3),
                    'HAPI est 1 des 2 seules sources sur 17 a porter du handicap'],
                'method': "category contains 'disab', INN, Intersectoral"}
    periods = sorted({str(r.get('date_start'))[:10] for r in dis})
    last = periods[-1]
    sel = [r for r in dis if str(r.get('date_start'))[:10] == last]
    total = sum(_num(r.get('population')) for r in sel)
    return {'value': int(total), 'vintage': last,
            'source': 'HDX HAPI humanitarian-needs (HNO)',
            'caveats': ['effectif de personnes en situation de handicap DANS LE BESOIN, '
                        'pas la prevalence du handicap',
                        'people-first : ecrire "personnes handicapees"'],
            'method': 'somme admin{} des lignes category~disab, INN, Intersectoral, '
                      'periode {} ({} lignes)'.format(admin_level, last, len(sel))}


# ─────────────────────────── SECURITE ALIMENTAIRE ───────────────────────────
def get_food_insecurity(iso3):
    """IPC 3+, en distinguant COURANT et PROJECTION (perimetres differents).

    ⚠ L'endpoint WFP IPC global ne sert que des PROJECTIONS, dont le perimetre
    geographique est plus etroit que l'analyse courante. Servir la projection comme
    "l'insecurite alimentaire du pays" sous-estime (Soudan : 5,57M projete sur 8,3M
    analyses contre 19,5M courant sur 47,5M). On rend les deux quand HAPI les a.
    """
    out = {'value': None, 'vintage': None, 'source': 'IPC via HAPI + WFP',
           'caveats': [], 'method': ''}
    # 1. HAPI food-security : le courant national, la reference
    try:
        from hapi_client import HAPIClient
        fs = HAPIClient().get_food_security(iso3)
        nat = [r for r in fs if not r.get('admin1_name') and not r.get('admin2_name')
               and r.get('ipc_type') == 'current' and r.get('ipc_phase') == '3+']
        if nat:
            last = max(str(r.get('date_start'))[:10] for r in nat)
            row = [r for r in nat if str(r.get('date_start'))[:10] == last][0]
            out['value'] = int(_num(row.get('population_in_phase')))
            out['vintage'] = last + ' (current)'
            out['method'] = 'HAPI food-security, ipc_type=current, admin0, phase 3+'
    except Exception as e:
        out['caveats'].append('HAPI food-security: ' + str(e)[:60])
    # 2. WFP IPC global : la projection, avec son perimetre
    try:
        from wfp_client import WFPClient
        w = WFPClient().get_ipc(iso3)
        if w:
            r = w[0]
            out['projection'] = {
                'ipc3plus': r['ipc3plus_population'],
                'period': r['reference_period'],
                'analysed_population': r['analysed_population_implied'],
            }
            out['caveats'].append(
                'projection WFP = {:,} sur {:,} analyses ({}), perimetre plus etroit '
                'que le courant : ne PAS la servir comme le chiffre national'.format(
                    r['ipc3plus_population'] or 0, r['analysed_population_implied'] or 0,
                    r['reference_period']))
    except Exception:
        pass
    if out['value'] is None and 'projection' not in out:
        out['caveats'].append('aucune donnee IPC exploitable pour ' + iso3)
    return out


# ─────────────────────────── FINANCEMENT ───────────────────────────
def get_funding(iso3, year=None):
    """Plan de reponse : requis, finance, couverture. Millesime le plus recent.

    Requis = scalaire de premier niveau du plan (revisedRequirements). Finance =
    endpoint fts/flow?planId= separe (jamais la somme des flux, qui double-compte).
    """
    from hpc_client import HPCClient
    plans = HPCClient().get_plans(iso3, max_funded=3)
    if not plans:
        return {'value': None, 'vintage': None, 'source': 'HPC/FTS',
                'caveats': ['aucun plan HPC pour ' + iso3], 'method': 'get_plans'}
    if year:
        plans = [p for p in plans if str(year) in (p['year'] or '')] or plans
    p = plans[0]
    gap = None
    if p['requirements_usd'] and p['funding_usd'] is not None:
        gap = p['requirements_usd'] - p['funding_usd']
    return {'value': {'requirements_usd': p['requirements_usd'],
                      'funding_usd': p['funding_usd'],
                      'coverage_pct': p['coverage_pct'],
                      'gap_usd': int(gap) if gap is not None else None},
            'vintage': p['year_max'], 'source': 'HPC/FTS ({})'.format(p['plan_name'][:50]),
            'caveats': ['montants en USD',
                        'la couverture d\'un plan multi-pays (RRP) n\'est pas le '
                        'financement du seul pays'],
            'method': 'plan/country + fts/flow?planId (agregat API, pas somme des flux)'}


# ─────────────────────────── CONFLIT ───────────────────────────
def get_conflict(iso3, days=30):
    """Evenements ACLED + morts sur N jours, UN SEUL pays garanti.

    `country_where='='` (sinon LIKE joker : Sudan ramene le Soudan du Sud). Assertion
    mono-pays dans le client. ACLED prend un NOM de pays, pas un ISO3.
    """
    from acled_client import ACLEDClient
    from datetime import datetime, timedelta
    # ACLED prend un NOM de pays, pas un ISO3. Table locale (les noms canoniques
    # ACLED, ex. "Democratic Republic of Congo" sans "the").
    names = {'SDN': 'Sudan', 'SSD': 'South Sudan', 'LBN': 'Lebanon', 'SYR': 'Syria',
             'YEM': 'Yemen', 'COD': 'Democratic Republic of Congo', 'AFG': 'Afghanistan',
             'UKR': 'Ukraine', 'MLI': 'Mali', 'NGA': 'Nigeria', 'ETH': 'Ethiopia',
             'SOM': 'Somalia', 'IRQ': 'Iraq', 'PSE': 'Palestine', 'MOZ': 'Mozambique',
             'HTI': 'Haiti', 'TCD': 'Chad', 'NER': 'Niger', 'BDI': 'Burundi',
             'MMR': 'Myanmar', 'PAK': 'Pakistan', 'KEN': 'Kenya', 'LBY': 'Libya'}
    name = names.get(iso3.upper())
    if not name:
        return {'value': None, 'vintage': None, 'source': 'ACLED',
                'caveats': ['nom de pays ACLED inconnu pour ' + iso3], 'method': ''}
    date_from = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    try:
        ev = ACLEDClient().get_events(name, date_from=date_from, limit=5000)
    except Exception as e:
        return {'value': None, 'vintage': None, 'source': 'ACLED',
                'caveats': ['ACLED indisponible (cle ?): ' + str(e)[:50]], 'method': ''}
    fatal = sum(int(e.get('fatalities') or 0) for e in ev)
    from collections import Counter
    types = Counter(e.get('event_type') for e in ev)
    return {'value': {'events': len(ev), 'fatalities': fatal,
                      'by_type': dict(types.most_common(5))},
            'vintage': '{} derniers jours (depuis {})'.format(days, date_from),
            'source': 'ACLED', 'caveats': ['country_where= strict : 1 seul pays'],
            'method': 'get_events({!r}), {} evenements'.format(name, len(ev))}


# ─────────────────────────── SEVERITE ───────────────────────────
def get_severity(iso3):
    """Score de severite : INFORM (via HAPI/JRC) + ACAPS.

    ⚠ Le champ ACAPS `People in need` est un SCORE 0-10, pas un effectif.
    """
    out = {'value': {}, 'vintage': None, 'source': 'INFORM + ACAPS',
           'caveats': ['le champ ACAPS "People in need" est un score 0-10, pas un '
                       'effectif'], 'method': ''}
    try:
        from acaps_client import ACAPSClient
        rows = ACAPSClient().get_inform_severity(iso3)
        if rows:
            r = rows[0]
            out['value']['acaps'] = {'score': r.get('severity_score'),
                                     'class': r.get('severity_class')}
    except Exception as e:
        out['caveats'].append('ACAPS: ' + str(e)[:50])
    return out


# ─────────────────────────── 3W ───────────────────────────
def get_3w(iso3, sector=None):
    """Presence operationnelle (qui fait quoi ou), via HAPI op-presence."""
    from hapi_client import HAPIClient
    ops = HAPIClient().get_op_presence(iso3)
    if sector:
        ops = [r for r in ops if sector.lower() in str(r.get('sector_name', '')).lower()]
    from collections import Counter
    orgs = Counter(r.get('org_acronym') for r in ops if r.get('org_acronym'))
    sectors = Counter(r.get('sector_name') for r in ops if r.get('sector_name'))
    return {'value': {'rows': len(ops), 'n_orgs': len(orgs),
                      'top_orgs': dict(orgs.most_common(8)),
                      'sectors': dict(sectors.most_common(10))},
            'vintage': max((str(r.get('date_start'))[:10] for r in ops if r.get('date_start')),
                           default=None),
            'source': 'HDX HAPI operational-presence (3W)',
            'caveats': ['la presence declaree n\'est pas la couverture des besoins'],
            'method': 'op-presence' + (' filtre secteur ' + sector if sector else '')}


# ─────────────────────────── CROISEMENTS ───────────────────────────
def neglect_index(iso3):
    """Indice de negligence : besoin eleve + financement faible.

    Croise PiN (HAPI) et couverture de financement (HPC). Debloque par le fix HPC
    du 2026-07-25 (avant, requirements et funding etaient a zero).
    """
    pin = get_pin(iso3)
    fund = get_funding(iso3)
    cov = (fund['value'] or {}).get('coverage_pct') if fund['value'] else None
    return {'value': {'pin': pin['value'], 'pin_vintage': pin['vintage'],
                      'funding_coverage_pct': cov,
                      'funding_vintage': fund['vintage'],
                      'signal': ('besoin eleve + sous-finance' if pin['value'] and
                                 cov is not None and cov < 50 else 'a interpreter')},
            'vintage': '{} / {}'.format(pin['vintage'], fund['vintage']),
            'source': 'HAPI (PiN) x HPC (financement)',
            'caveats': ['indice COMPOSITE : millesimes differents entre PiN et '
                        'financement, ne pas surinterpreter',
                        'un RRP multi-pays fausse la couverture pays'],
            'method': 'get_pin x get_funding.coverage_pct'}


if __name__ == '__main__':
    iso = sys.argv[1] if len(sys.argv) > 1 else 'SDN'
    import json
    for name, fn in [('PiN', get_pin), ('PiN handicap', get_disability_pin),
                     ('securite alimentaire', get_food_insecurity),
                     ('financement', get_funding), ('3W', get_3w),
                     ('neglect index', neglect_index)]:
        print('\n### {} ###'.format(name))
        try:
            r = fn(iso)
            v = r.get('value')
            print('  value   :', json.dumps(v, ensure_ascii=False)[:180]
                  if isinstance(v, dict) else v)
            print('  vintage :', r.get('vintage'))
            print('  source  :', r.get('source'))
            for c in r.get('caveats', [])[:2]:
                print('  caveat  :', c[:120])
        except Exception as e:
            print('  ERREUR:', str(e)[:100])
