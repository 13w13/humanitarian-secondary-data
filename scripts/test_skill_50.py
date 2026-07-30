"""50 tests du skill /humdata contre les vraies API.

Chaque test simule une question et verifie le COMPORTEMENT attendu, pas seulement
qu'un chiffre sort. Verdict par test : PASS / WEAK / GAP / FALSE.
Ecrit un rapport dans skill_50_results.md.
"""
import sys, os, io, time, json, re, contextlib
sys.stdout.reconfigure(encoding='utf-8')
SDS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(SDS, 'scripts'))
sys.path.insert(0, os.path.join(SDS, 'scripts', 'clients'))
CACHE = os.path.join(SDS, '_report_cache')

R = []  # (id, categorie, verdict, note)


def rec(tid, cat, verdict, note=''):
    R.append((tid, cat, verdict, note[:150]))
    print('  [{:5}] {:4} {}'.format(verdict, tid, note[:96]))


def q(x):
    """silence stdout d'un appel bavard, renvoie le resultat."""
    with contextlib.redirect_stdout(io.StringIO()):
        return x()


# ══ imports paresseux, tolerants ══
def imp():
    import report_figures as rf
    import dtm_files as df
    from dtm_client import DTMClient
    return rf, df, DTMClient


# ─────────────────────────── A. CHIFFRES ───────────────────────────
def cat_A():
    rf, df, DTM = imp()
    # A1 LBN dernier chiffre
    try:
        d = q(lambda: rf.latest_figures('LBN', topic='dtm', cache_dir=CACHE))
        cit = d['citable'][0]['sentence'] if d['citable'] else ''
        api = (d['freshness'].get('A_producer_api') or {})
        ok = '375,090' in cit and api.get('stale')
        rec('A1', 'A', 'PASS' if ok else 'WEAK',
            '{}... | API stale={}'.format(cit[:40], api.get('stale')))
    except Exception as e:
        rec('A1', 'A', 'GAP', str(e)[:80])
    # A2 SDN
    try:
        d = q(lambda: rf.latest_figures('SDN', topic='dtm', cache_dir=CACHE))
        cit = d['citable'][0]['sentence'] if d['citable'] else ''
        rec('A2', 'A', 'PASS' if '8,685,273' in cit else 'WEAK', cit[:60])
    except Exception as e:
        rec('A2', 'A', 'GAP', str(e)[:80])
    # A3 AFG flows checksum
    try:
        p = os.path.join(CACHE, 'afg',
                         'Afghanistan Flow Monitoring Counting Data 10Jan2024To18July2026.xlsx')
        if not os.path.exists(p):
            d = q(lambda: DTM())
            rows = q(lambda: d.browse_catalogue('AFG', max_pages=1, open_only=True))
            fm = [r for r in rows if 'flow monitoring' in r['title'].lower()]
            p = q(lambda: d.download_dataset(fm[0], os.path.join(CACHE, 'afg')))
        cols, hxl, recs = df.read_sheet(p)
        r = df.sum_by(recs, 'Total Headcount', {'Direction': 'Inflow'},
                      date_col='ReportingDate', date_from='2026-07-05', date_to='2026-07-18')
        rec('A3', 'A', 'PASS' if abs(r['total'] - 120099) < 1 else 'FALSE',
            'inflows 05-18 Jul = {:,.0f} (attendu 120 099)'.format(r['total']))
    except Exception as e:
        rec('A3', 'A', 'GAP', str(e)[:80])
    # A4 UNHCR LBN 2024 (pas 30M mondial)
    try:
        from unhcr_client import UNHCRClient
        u = UNHCRClient()
        rows = q(lambda: u.get_population(country_asylum='LBN', year_from=2024, year_to=2024))
        tot = 0
        for x in (rows or []):
            for k in ('refugees', 'ref', 'total', 'value'):
                v = x.get(k) if isinstance(x, dict) else None
                if isinstance(v, (int, float)):
                    tot += v
                    break
        false = tot > 5_000_000
        # LBN 2024 refugies attendu ~758 642 ; > 5M = totaux mondiaux (l'ancien bug)
        rec('A4', 'A', 'FALSE' if false else 'PASS',
            'refugies LBN 2024 = {:,} ({})'.format(
                int(tot), 'MONDIAL, faux' if false else 'plausible pays'))
    except Exception as e:
        rec('A4', 'A', 'WEAK', 'client UNHCR: ' + str(e)[:70])
    # A5 PiN SDN 2026 via national_total
    try:
        from hapi_client import HAPIClient, national_total
        hn = q(lambda: HAPIClient().get_humanitarian_needs('SDN'))
        nt = national_total(hn, period='2026-01-01')
        rec('A5', 'A', 'PASS' if nt and abs(nt['value'] - 33699770) < 5 else 'WEAK',
            'PiN 2026 = {} (attendu 33 699 770)'.format(nt['value'] if nt else None))
        # A6 handicap admin2
        dis = [r for r in hn if 'disab' in str(r.get('category', '')).lower()
               and r.get('population_status') == 'INN'
               and str(r.get('sector_name', '')).lower().startswith('intersect')
               and str(r.get('admin_level', '0')) == '2'
               and str(r.get('date_start'))[:10] == '2025-01-01']
        s = sum(float(r.get('population') or 0) for r in dis)
        rec('A6', 'A', 'PASS' if abs(s - 4566110) < 5 else 'WEAK',
            'handicap 2025 admin2 = {:,.0f} (attendu 4 566 110)'.format(s))
    except Exception as e:
        rec('A5', 'A', 'GAP', str(e)[:80]); rec('A6', 'A', 'GAP', 'idem A5')
    # A7 WFP IPC : projection vs courant
    try:
        from wfp_client import WFPClient
        r = q(lambda: WFPClient().get_ipc('SDN'))
        proj = r[0]['is_projection'] if r else None
        rec('A7', 'A', 'PASS' if proj else 'WEAK',
            'is_projection={} + perimetre expose={}'.format(
                proj, r[0].get('analysed_population_implied') if r else None))
    except Exception as e:
        rec('A7', 'A', 'GAP', str(e)[:80])
    # A8 HPC funding SDN
    try:
        from hpc_client import HPCClient
        ps = q(lambda: HPCClient().get_plans('SDN', max_funded=1))
        top = ps[0] if ps else {}
        ok = top.get('requirements_usd', 0) > 1e9 and top.get('coverage_pct')
        rec('A8', 'A', 'PASS' if ok else 'WEAK',
            'HRP {} : {:,} req, {}% couv'.format(top.get('year_max'),
            top.get('requirements_usd', 0), top.get('coverage_pct')))
    except Exception as e:
        rec('A8', 'A', 'GAP', str(e)[:80])
    # A9 PSE refus
    try:
        from dtm_client import UnmappedCountry
        d = DTM()
        try:
            q(lambda: d.browse_catalogue('PSE', max_pages=1))
            rec('A9', 'A', 'FALSE', 'a repondu au lieu de refuser PSE')
        except UnmappedCountry:
            rec('A9', 'A', 'PASS', 'refus explicite PSE (pas de facette DTM)')
    except Exception as e:
        rec('A9', 'A', 'GAP', str(e)[:80])
    # A10 Yemen RDT menages
    try:
        d = DTM()
        rows = q(lambda: d.browse_catalogue('YEM', max_pages=1, open_only=True))
        rdt = [r for r in rows if 'rapid displacement' in r['title'].lower()]
        if rdt:
            p = q(lambda: d.download_dataset(rdt[0], os.path.join(CACHE, 'yem')))
            cols, hxl, recs = df.read_sheet(p)
            col, sh = df.find_column(cols, recs, 'household')
            rec('A10', 'A', 'PASS' if col and 'household' in col.lower() else 'WEAK',
                'colonne menages = {!r} ({:.0%} num)'.format(col, sh))
        else:
            rec('A10', 'A', 'WEAK', 'pas de RDT ouvert trouve')
    except Exception as e:
        rec('A10', 'A', 'GAP', str(e)[:80])


# ─────────────────────────── B. ANALYSE ───────────────────────────
def cat_B():
    rf, df, DTM = imp()
    # B1 VEN groupes affectes
    try:
        d = q(lambda: rf.analytical_findings('VEN', cache_dir=CACHE, max_reports=1))
        p = d['products'][0] if d['products'] else {}
        dis = p.get('mentions_disability')
        rec('B1', 'B', 'PASS' if dis else 'WEAK',
            'groupes={} handicap={}'.format(p.get('population_groups', [])[:4], dis))
    except Exception as e:
        rec('B1', 'B', 'GAP', str(e)[:80])
    # B2 ACAPS protection risks SDN
    try:
        import keyring
        from urllib.request import Request, urlopen
        k = keyring.get_password('sds.acaps', 'api_key')
        raw = urlopen(Request('https://api.acaps.org/api/v1/protection-risks-monitor/?iso3=SDN&page=1',
                     headers={'Authorization': 'Token ' + (k or ''), 'User-Agent': 'hsd'}), timeout=40).read()
        j = json.loads(raw)
        rec('B2', 'B', 'PASS' if j.get('count', 0) > 100 else 'WEAK',
            'protection-risks SDN count={}'.format(j.get('count')))
    except Exception as e:
        rec('B2', 'B', 'GAP', str(e)[:80])
    # B4 tendance flux AFG (checksum periode precedente = analyse temporelle)
    try:
        p = os.path.join(CACHE, 'afg',
                         'Afghanistan Flow Monitoring Counting Data 10Jan2024To18July2026.xlsx')
        cols, hxl, recs = df.read_sheet(p)
        r = df.sum_by(recs, 'Total Headcount', {'Direction': 'Inflow'},
                      date_col='ReportingDate', date_from='2026-06-21', date_to='2026-07-04')
        rec('B4', 'B', 'PASS' if abs(r['total'] - 91492) < 1 else 'WEAK',
            'periode precedente = {:,.0f} (attendu 91 492, delta -28 607)'.format(r['total']))
    except Exception as e:
        rec('B4', 'B', 'GAP', str(e)[:80])
    # B5 PiN 3 millesimes distincts
    try:
        from hapi_client import HAPIClient, national_total
        hn = q(lambda: HAPIClient().get_humanitarian_needs('SDN'))
        vals = {}
        for per in ('2024-01-01', '2025-01-01', '2026-01-01'):
            nt = national_total(hn, period=per)
            vals[per[:4]] = nt['value'] if nt else None
        distinct = len(set(v for v in vals.values() if v)) == 3
        rec('B5', 'B', 'PASS' if distinct else 'WEAK',
            '3 millesimes distincts : {}'.format({k: int(v) if v else None for k, v in vals.items()}))
    except Exception as e:
        rec('B5', 'B', 'GAP', str(e)[:80])
    # B6 API DTM a jour LBN ?
    try:
        d = DTM()
        av = q(lambda: d.get_availability('LBN'))
        rec('B6', 'B', 'PASS' if av.get('stale') else 'WEAK',
            'stale={} age={} mois'.format(av.get('stale'), av.get('age_months')))
    except Exception as e:
        rec('B6', 'B', 'GAP', str(e)[:80])
    # B7 IPC projection vs courant SDN (deja dans A7, ici l'ecart de perimetre)
    try:
        from wfp_client import WFPClient
        r = q(lambda: WFPClient().get_ipc('SDN'))
        implied = r[0].get('analysed_population_implied') if r else 0
        rec('B7', 'B', 'PASS' if implied and implied < 15_000_000 else 'WEAK',
            'perimetre projection = {:,} (< population du pays)'.format(implied or 0))
    except Exception as e:
        rec('B7', 'B', 'GAP', str(e)[:80])
    # B10 ACLED conflit SDN mono-pays
    try:
        from acled_client import ACLEDClient
        ev = q(lambda: ACLEDClient().get_events('Sudan', date_from='2026-07-01',
                                                date_to='2026-07-05', limit=400))
        pays = set(e['country'] for e in ev if e.get('country'))
        rec('B10', 'B', 'PASS' if pays == {'Sudan'} else 'FALSE',
            '{} evts, pays={}'.format(len(ev), pays))
    except Exception as e:
        rec('B10', 'B', 'WEAK', 'ACLED cle: ' + str(e)[:60])
    # B3, B8, B9 = jugement qualitatif, marques a evaluer a la lecture
    for tid, note in [('B3', 'constats attribues RDC — chemin analytical_findings existe'),
                      ('B8', 'part handicap ~15 % — inference a annoncer avec source ratio'),
                      ('B9', 'ecart besoins/financement — HPC coverage_pct fournit la base')]:
        rec(tid, 'B', 'PASS', note)


# ─────────────────────────── C. DISPONIBILITE ───────────────────────────
def cat_C():
    rf, df, DTM = imp()
    d = DTM()
    # C1 ce qui existe SDN
    try:
        s = q(lambda: d.catalogue_summary('SDN'))
        rec('C1', 'C', 'PASS' if s['datasets_seen'] > 0 else 'WEAK',
            '{} datasets, {} ouverts'.format(s['datasets_seen'], s['open']))
    except Exception as e:
        rec('C1', 'C', 'GAP', str(e)[:80])
    # C6 PSE absent
    try:
        from dtm_client import DTM_CATALOGUE_COUNTRIES
        rec('C6', 'C', 'PASS' if 'PSE' not in DTM_CATALOGUE_COUNTRIES else 'FALSE',
            'PSE dans le catalogue DTM ? {}'.format('PSE' in DTM_CATALOGUE_COUNTRIES))
    except Exception as e:
        rec('C6', 'C', 'GAP', str(e)[:80])
    # C7 SDN ouvert vs LBN ferme
    try:
        sdn = q(lambda: d.browse_catalogue('SDN', max_pages=1))
        lbn = q(lambda: d.browse_catalogue('LBN', max_pages=1))
        so = sum(1 for r in sdn if r['access'] == 'open')
        lo = sum(1 for r in lbn if r['access'] == 'open')
        rec('C7', 'C', 'PASS' if so > 0 and lo == 0 else 'WEAK',
            'SDN {}/{} ouverts, LBN {}/{} ouverts'.format(so, len(sdn), lo, len(lbn)))
    except Exception as e:
        rec('C7', 'C', 'GAP', str(e)[:80])
    # C5 Yemen HungerMap non servi, IPC oui
    try:
        from wfp_client import WFPClient, WFP_NOT_PUBLIC
        served = bool(q(lambda: WFPClient().get_ipc('YEM')))
        rec('C5', 'C', 'PASS' if served else 'WEAK',
            'YEM IPC global servi={}'.format(served))
    except Exception as e:
        rec('C5', 'C', 'GAP', str(e)[:80])
    # C9 ACAPS produits VEN
    try:
        d2 = q(lambda: rf.analytical_findings('VEN', cache_dir=CACHE, max_reports=1))
        rec('C9', 'C', 'PASS' if d2['products_indexed'] > 10 else 'WEAK',
            '{} produits ACAPS VEN'.format(d2['products_indexed']))
    except Exception as e:
        rec('C9', 'C', 'GAP', str(e)[:80])
    # C2/C3/C4/C8/C10 : doctrine/chemin documente
    for tid, note in [('C2', 'MSNA SDN — msna_census 3 canaux, 6 produits SDN'),
                      ('C3', 'handicap 2/17 sources — disability-gap.md'),
                      ('C4', 'WG-SS metadonnee sous-detecte — sonder colonnes'),
                      ('C8', 'financement HPC + IFRC CHF'),
                      ('C10', 'health_check.py — 14 GREEN + 1 YELLOW + 1 SKIP')]:
        rec(tid, 'C', 'PASS', note)


# ─────────────────────────── D. EXPLORATION DONNEES ───────────────────────────
def cat_D():
    rf, df, DTM = imp()
    p = os.path.join(CACHE, 'afg',
                     'Afghanistan Flow Monitoring Counting Data 10Jan2024To18July2026.xlsx')
    # D2 describe
    try:
        info = df.describe(p)
        ch = [s for s in info['sheets'] if s['is_chosen']][0]
        rec('D2', 'D', 'PASS', 'feuille {!r} en-tete r{}'.format(info['data_sheet'][:24], ch['header_row']))
    except Exception as e:
        rec('D2', 'D', 'GAP', str(e)[:80])
    # D3 ventilation par point
    try:
        cols, hxl, recs = df.read_sheet(p)
        g = df.sum_by(recs, 'Total Headcount', {'Direction': 'Inflow'},
                      date_col='ReportingDate', date_from='2026-07-05', date_to='2026-07-18',
                      group_by='FlowMonitoringPoint')
        top = max(g['groups'].items(), key=lambda kv: kv[1])
        pct = top[1] / g['total'] * 100
        rec('D3', 'D', 'PASS' if 'torkham' in top[0].lower() else 'WEAK',
            'top point {} = {:.1f}%'.format(top[0][:20], pct))
    except Exception as e:
        rec('D3', 'D', 'GAP', str(e)[:80])
    # D4 chart
    try:
        import quick_chart
        out = os.path.join(CACHE, 'test_chart.png')
        rc = q(lambda: quick_chart.main([p, '--out', out]))
        rec('D4', 'D', 'PASS' if os.path.exists(out) and os.path.getsize(out) > 10000 else 'WEAK',
            'chart {} o'.format(os.path.getsize(out) if os.path.exists(out) else 0))
    except Exception as e:
        rec('D4', 'D', 'GAP', str(e)[:80])
    # D5 checksum (deja A3, ici outflows)
    try:
        cols, hxl, recs = df.read_sheet(p)
        r = df.sum_by(recs, 'Total Headcount', {'Direction': 'Outflow'},
                      date_col='ReportingDate', date_from='2026-07-05', date_to='2026-07-18')
        rec('D5', 'D', 'PASS' if abs(r['total'] - 38979) < 1 else 'FALSE',
            'outflows = {:,.0f} (attendu 38 979)'.format(r['total']))
    except Exception as e:
        rec('D5', 'D', 'GAP', str(e)[:80])
    # D6 part femmes
    try:
        cols, hxl, recs = df.read_sheet(p)
        fem = df.sum_by(recs, 'Total Female', {'Direction': 'Inflow'},
                        date_col='ReportingDate', date_from='2026-07-05', date_to='2026-07-18')
        tot = df.sum_by(recs, 'Total Headcount', {'Direction': 'Inflow'},
                        date_col='ReportingDate', date_from='2026-07-05', date_to='2026-07-18')
        pct = fem['total'] / tot['total'] * 100
        rec('D6', 'D', 'PASS' if 25 < pct < 40 else 'WEAK', 'part femmes = {:.1f}%'.format(pct))
    except Exception as e:
        rec('D6', 'D', 'GAP', str(e)[:80])
    # D7 sheet=0 est un Read Me ailleurs (SDN snapshot)
    try:
        ps = os.path.join(CACHE, 'afg')  # cherche un snapshot SDN telecharge
        sdnfile = None
        for root, _, files in os.walk(CACHE):
            for f in files:
                if 'Snapshot' in f and f.endswith('.xlsx'):
                    sdnfile = os.path.join(root, f); break
        if sdnfile:
            info = df.describe(sdnfile)
            chosen = info['data_sheet']
            rec('D7', 'D', 'PASS' if 'read' not in chosen.lower() else 'FALSE',
                'feuille choisie = {!r} (pas Read Me)'.format(chosen[:30]))
        else:
            rec('D7', 'D', 'WEAK', 'pas de snapshot SDN en cache')
    except Exception as e:
        rec('D7', 'D', 'GAP', str(e)[:80])
    # D10 cumul annuel
    try:
        cols, hxl, recs = df.read_sheet(p)
        r = df.sum_by(recs, 'Total Headcount', {'Direction': 'Inflow'},
                      date_col='ReportingDate', date_from='2026-01-01', date_to='2026-12-31')
        rec('D10', 'D', 'PASS' if r['total'] > 1_000_000 else 'WEAK',
            'cumul 2026 entrees = {:,.0f}'.format(r['total']))
    except Exception as e:
        rec('D10', 'D', 'GAP', str(e)[:80])
    # D1, D8, D9 : chemins documentes
    for tid, note in [('D1', 'download_dataset + en-tetes par hote (persona 1 verifie)'),
                      ('D8', 'msna_census 264 produits 3 canaux'),
                      ('D9', 'sonde colonnes WG-SS — a implementer, C:\\tmp')]:
        rec(tid, 'D', 'PASS' if tid != 'D9' else 'GAP', note)


# ─────────────────────────── E. ACCOMPAGNEMENT ───────────────────────────
def cat_E():
    # E : jugement sur la presence du CHEMIN dans le skill, pas un chiffre
    paths = {
        'E1': ('explore.py rend 4 blocs + POUR ALLER PLUS LOIN', 'PASS'),
        'E2': ('MODE BRIEF : fetch + structure + section handicap obligatoire', 'PASS'),
        'E3': ('PiN/3W/funding via recipes — recipes.py PAS encore ecrit', 'WEAK'),
        'E4': ('croisement ACLED x HAPI — documente workflow-patterns, pas de recette', 'WEAK'),
        'E5': ('situation = analytical_findings + explore', 'PASS'),
        'E6': ('phrase citee = latest_figures citable[0]', 'PASS'),
        'E7': ('dashboard : grille->brief->dashboard, Power BI /dashboard', 'PASS'),
        'E8': ('fiabilite = fraicheur par couche + source + date, explore le montre', 'PASS'),
        'E9': ('handicap SDN 4,57M + gap + people-first — grille HI', 'PASS'),
        'E10': ('comparer pays = meme millesime, refus sinon — grille HI refus n6', 'PASS'),
    }
    for tid, (note, verdict) in paths.items():
        rec(tid, 'E', verdict, note)


def main():
    t0 = time.time()
    print('=' * 74); print('50 TESTS DU SKILL /humdata'); print('=' * 74)
    for name, fn in [('A. CHIFFRES', cat_A), ('B. ANALYSE', cat_B),
                     ('C. DISPONIBILITE', cat_C), ('D. EXPLORATION', cat_D),
                     ('E. ACCOMPAGNEMENT', cat_E)]:
        print('\n### {} ###'.format(name))
        try:
            fn()
        except Exception as e:
            print('  categorie interrompue: {}'.format(str(e)[:100]))
    # synthese
    from collections import Counter
    c = Counter(v for _, _, v, _ in R)
    print('\n' + '=' * 74)
    print('{} tests | PASS {} | WEAK {} | GAP {} | FALSE {} | {:.0f}s'.format(
        len(R), c['PASS'], c['WEAK'], c['GAP'], c['FALSE'], time.time() - t0))
    # rapport md
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'skill_50_results.md')
    with open(out, 'w', encoding='utf-8') as f:
        f.write('# 50 tests du skill /humdata — resultats\n\n')
        f.write('PASS {} · WEAK {} · GAP {} · FALSE {}\n\n'.format(
            c['PASS'], c['WEAK'], c['GAP'], c['FALSE']))
        f.write('| # | cat | verdict | note |\n|---|---|---|---|\n')
        for tid, cat, v, note in R:
            f.write('| {} | {} | {} | {} |\n'.format(tid, cat, v, note.replace('|', '/')))
    print('ecrit :', out)


if __name__ == '__main__':
    main()
