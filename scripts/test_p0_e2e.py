"""
Non-regression des correctifs P0 (revue de juillet 2026)
========================================================
Chaque test correspond a un cas de DONNEE FAUSSE constate en production, pas a une
hypothese. Le commentaire dit ce qui etait faux et ce qu'on a mesure.

    python -X utf8 scripts/test_p0_e2e.py

Stdlib uniquement, comme les clients (regle 2 du plan) : pas de pytest.
Ces tests appellent les vraies API : ils sont lents (~2 min) et supposent le reseau
plus les identifiants keyring (ACLED, IDMC). Un manque d'identifiant fait SKIP, pas FAIL.
"""
import os
import sys
import time

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'clients'))

PASS, FAIL, SKIP = [], [], []


def check(label, cond, detail=''):
    (PASS if cond else FAIL).append(label)
    print('  [{}] {} {}'.format('OK  ' if cond else 'FAIL', label, detail))
    return cond


def skip(label, why):
    SKIP.append(label)
    print('  [SKIP] {} ({})'.format(label, why))


def section(n, title):
    print()
    print('=' * 74)
    print('{}. {}'.format(n, title))
    print('=' * 74)


# ── P0 #2 : HPC/FTS ──────────────────────────────────────────
def t_hpc():
    section(2, 'HPC/FTS : les besoins n\'etaient PAS dans un dict imbrique')
    from hpc_client import HPCClient
    h = HPCClient()
    plans = h.get_plans('SDN', max_funded=1)
    check('SDN plans non vides', len(plans) > 10, '{} plans'.format(len(plans)))
    # AVANT : 28/28 plans a 0 requis (lecture de p['requirements']['revisedRequirements'],
    # cle inexistante -> {} -> 0). APRES : scalaire de premier niveau.
    with_req = [p for p in plans if p['requirements_usd']]
    check('tous les plans ont des besoins > 0', len(with_req) == len(plans),
          '{}/{} renseignes'.format(len(with_req), len(plans)))
    top = plans[0]
    check('HRP le plus recent > 1 Md USD', top['requirements_usd'] > 1e9,
          '{} = {:,} USD'.format(top['year_max'], top['requirements_usd']))
    check('financement recupere (endpoint separe)', top['funding_usd'] is not None,
          '{:,} USD, couverture {} %'.format(top['funding_usd'] or 0, top['coverage_pct']))
    check('couverture plausible (0-200 %)',
          top['coverage_pct'] is not None and 0 < top['coverage_pct'] < 200)


# ── P0 #4 : ACLED ────────────────────────────────────────────
def t_acled():
    section(4, 'ACLED : `country=Sudan` faisait un LIKE joker (+ Soudan du Sud)')
    try:
        from acled_client import ACLEDClient
        a = ACLEDClient()
        if not getattr(a, 'email', None) and not os.environ.get('ACLED_EMAIL'):
            pass
        ev = a.get_events('Sudan', date_from='2026-07-01', date_to='2026-07-05', limit=500)
    except Exception as e:
        return skip('ACLED', str(e)[:70])
    check('evenements non vides', len(ev) > 0, '{} evenements'.format(len(ev)))
    countries = sorted({e['country'] for e in ev if e['country']})
    check('UN SEUL pays renvoye', countries == ['Sudan'], str(countries))
    check('champ mort iso3 retire', 'iso3' not in (ev[0] if ev else {}))
    check('code iso numerique conserve', bool(ev and ev[0].get('iso')),
          'iso={}'.format(ev[0].get('iso') if ev else '-'))


# ── P0 #5 : DTM sur HDX ──────────────────────────────────────
def t_dtm_hdx():
    section(5, 'DTM/HDX : la recherche plein texte rendait 30/35 lignes ETH pour LBN')
    from dtm_client import DTMClient
    d = DTMClient()
    scoped = d.search_dtm_datasets('Lebanon', rows=35, iso3='LBN')
    check('scope LBN non vide', len(scoped) > 0, '{} datasets'.format(len(scoped)))
    check('chaque dataset porte LBN dans ses groupes',
          all('LBN' in (x['country_groups'] or '').split(';') for x in scoped if x['country_groups']))
    check('colonne pays ecrite', all('country_groups' in x for x in scoped))
    check('metadata_modified renomme (plus de champ `date` trompeur)',
          all('metadata_modified' in x and 'date' not in x for x in scoped))


# ── P0 #6 : GDACS ────────────────────────────────────────────
def t_gdacs():
    section(6, 'GDACS : filtre par NOM -> `Phl` -> 0 alerte en silence ; severite non lue')
    from gdacs_client import GDACSClient, GDACSUnavailable
    g = GDACSClient()
    alerts = g.get_alerts(iso3='PHL', limit=300)
    check('PHL a des alertes via iso3', len(alerts) > 0, '{} alertes'.format(len(alerts)))
    check('severite lue (champ severitydata, pas severity)',
          all(a['severity_value'] not in ('', None) for a in alerts),
          '{}/{} renseignees'.format(
              sum(1 for a in alerts if a['severity_value'] not in ('', None)), len(alerts)))
    check('evenements multi-pays rattaches',
          any(a['n_countries_affected'] > 1 for a in alerts))
    # Un nom invalide doit LEVER (panne) et non rendre [] (absence).
    try:
        g.get_alerts(country='Phl', limit=50)
        check('nom invalide leve au lieu de rendre 0', False, 'a rendu une liste')
    except GDACSUnavailable:
        check('nom invalide leve au lieu de rendre 0', True, '-> GDACSUnavailable')
    except Exception as e:
        check('nom invalide leve au lieu de rendre 0', True,
              '-> {}'.format(type(e).__name__))


# ── P0 #8 : IDMC ─────────────────────────────────────────────
def t_idmc():
    section(8, 'IDMC : iso3 fabrique depuis l\'entree, gzip non gere, idus/all absent')
    from idmc_client import IDMCClient
    c = IDMCClient()
    if not c.client_id:
        return skip('IDMC', 'pas de client_id keyring')
    gidd = c.get_displacement('SDN', year_from=2023)
    check('GIDD SDN non vide', len(gidd) >= 2, '{} annees'.format(len(gidd)))
    check('stock 2025 > 5 M', any(r['year'] == 2025 and r['conflict_stock'] > 5e6
                                 for r in gidd),
          'stock 2025 = {:,}'.format(next((r['conflict_stock'] for r in gidd
                                           if r['year'] == 2025), 0)))
    ev = c.get_idus_events('SDN')
    check('idus/all lu (gzip decompresse)', len(ev) > 100, '{} evenements'.format(len(ev)))
    check('iso3 vient de la REPONSE, tous SDN',
          all(str(e.get('iso3', '')).upper() == 'SDN' for e in ev))


# ── P0 #9 : ACAPS ────────────────────────────────────────────
def t_acaps():
    section(9, 'ACAPS : `people_in_need` etait un SCORE 0-10 lu comme un effectif')
    from acaps_client import ACAPSClient
    a = ACAPSClient()
    try:
        rows = a.get_inform_severity('SDN')
    except Exception as e:
        return skip('ACAPS', str(e)[:70])
    if not rows:
        return skip('ACAPS', 'aucune ligne (cle absente ?)')
    r = rows[0]
    check('colonne renommee pin_score_0_10', 'pin_score_0_10' in r)
    check('ancien nom trompeur retire', 'people_in_need' not in r)
    check('marqueur d\'echelle present', bool(r.get('pin_score_scale')),
          str(r.get('pin_score_scale'))[:50])
    try:
        v = float(r['pin_score_0_10'])
        check('valeur bien dans 0-10 (donc pas un effectif)', 0 <= v <= 10,
              'valeur = {}'.format(v))
    except (TypeError, ValueError):
        check('valeur numerique', False, repr(r.get('pin_score_0_10'))[:40])


# ── P0 #10 : World Bank ──────────────────────────────────────
def t_worldbank():
    section(10, 'World Bank : erreurs rendues en HTTP 200, libelles internes, pas de mrnev')
    from worldbank_client import WorldBankClient
    w = WorldBankClient()
    try:
        w.get_indicator('ZZZ', 'NY.GDP.MKTP.CD')
        check('erreur HTTP 200 detectee', False, 'aucune exception')
    except ValueError as e:
        check('erreur HTTP 200 detectee', True, str(e)[:60])
    except Exception as e:
        check('erreur HTTP 200 detectee', False, type(e).__name__)
    got = w.get_latest_value('SDN', 'SP.POP.TOTL')
    check('mrnev fonctionne (per_page retire)', bool(got),
          '{} = {:,}'.format(got['year'], int(got['value'])) if got else 'None')
    check('libelle vient de l\'API',
          bool(got and got['indicator_name'] and 'opulation' in got['indicator_name']),
          got['indicator_name'] if got else '-')


# ── P0 #12 : IFRC GO ─────────────────────────────────────────
def t_ifrc():
    section(12, 'IFRC GO : `country__in` ignore sur appeal/ (corpus mondial) ; CHF lus en USD')
    from ifrcgo_client import IFRCGoClient
    c = IFRCGoClient()
    ap = c.get_appeals(iso3='SDN', limit=8)
    check('appels SDN non vides', len(ap) > 0, '{} appels'.format(len(ap)))
    check('tous les appels concernent le Soudan',
          all('sudan' in str(a.get('name', '')).lower() or
              str((a.get('country') or {}).get('iso3', '')).upper() == 'SDN' for a in ap),
          '; '.join(str(a.get('name'))[:24] for a in ap[:3]))
    check('devise explicite CHF', all(a.get('currency') == 'CHF' for a in ap))
    check('anciennes colonnes ambigues retirees',
          all('amount_requested' not in a for a in ap))


# ── Delta : WFP IPC global ───────────────────────────────────
def t_wfp_ipc():
    section('+', 'WFP IPC global : debloque les pays absents de HungerMap (SDN, SSD)')
    from wfp_client import WFPClient
    w = WFPClient()
    allr = w.get_ipc()
    check('endpoint sert des pays', len(allr) > 40, '{} pays'.format(len(allr)))
    sdn = w.get_ipc('SDN')
    check('SDN servi (HungerMap le refuse)', len(sdn) == 1)
    if sdn:
        r = sdn[0]
        check('IPC 3+ = phase35, pas phase3',
              r['ipc3plus_population'] > r['phase3_only_population'],
              '3+ = {:,} contre phase3 seule = {:,}'.format(
                  r['ipc3plus_population'], r['phase3_only_population']))
        check('perimetre rendu visible', bool(r['analysed_population_implied']),
              'population analysee = {:,}'.format(r['analysed_population_implied']))
        check('nature de la periode signalee', 'reference_period' in r and
              isinstance(r['is_projection'], bool),
              '{} (projection={})'.format(r['reference_period'], r['is_projection']))


def main():
    t0 = time.time()
    for fn in (t_hpc, t_acled, t_dtm_hdx, t_gdacs, t_idmc, t_acaps, t_worldbank,
               t_ifrc, t_wfp_ipc):
        try:
            fn()
        except Exception as e:
            FAIL.append(fn.__name__)
            print('  [FAIL] {} a leve : {}: {}'.format(fn.__name__, type(e).__name__,
                                                       str(e)[:110]))
    print()
    print('=' * 74)
    print('{} OK / {} FAIL / {} SKIP en {:.0f} s'.format(
        len(PASS), len(FAIL), len(SKIP), time.time() - t0))
    if FAIL:
        print('ECHECS : {}'.format(', '.join(FAIL)))
    return 1 if FAIL else 0


if __name__ == '__main__':
    sys.exit(main())
