"""
HPC/FTS Funding Flows Client
==============================
Query OCHA Financial Tracking Service for humanitarian funding data.
Public API — no authentication required (appname recommended).

Usage:
    from hpc_client import HPCClient
    hpc = HPCClient()
    flows = hpc.get_funding_flows('SDN', year=2025)
    plans = hpc.get_plans('SDN')
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
from urllib.request import Request, urlopen
from urllib.parse import urlencode

from config import DEFAULT_TIMEOUT, USER_AGENT, save_csv

HPC_BASE = 'https://api.hpc.tools/v1/public'


class HPCClient:
    """Client for HPC/FTS API (public, no auth required)."""

    def __init__(self):
        self.base = HPC_BASE

    def _get(self, endpoint, params=None):
        """GET request to HPC API. Returns parsed JSON."""
        url = '{}/{}'.format(self.base, endpoint)
        if params:
            url += '?{}'.format(urlencode(params))
        req = Request(url, headers={
            'User-Agent': USER_AGENT,
            'Accept': 'application/json',
        })
        resp = urlopen(req, timeout=DEFAULT_TIMEOUT)
        return json.loads(resp.read())

    def get_funding_flows(self, iso3, year=None, limit=200):
        """Get funding flows for a country.

        Args:
            iso3: Country ISO3 code
            year: Filter by year (e.g. 2025)
            limit: Max results per page

        Returns:
            List of flow dicts: source_org, destination_org, amount_usd,
            flow_date, description, status.
        """
        params = {
            'countryISO3': iso3.upper(),
            'limit': str(limit),
        }
        if year:
            params['year'] = str(year)

        try:
            data = self._get('fts/flow', params)
        except Exception as e:
            print('  HPC flows: {}'.format(e))
            return []

        flows_raw = data.get('data', {}).get('flows', [])
        records = []
        for f in flows_raw:
            # Source organizations
            sources = f.get('sourceObjects', [])
            source_orgs = [s.get('name', '') for s in sources if s.get('type') == 'Organization']
            source_org = '; '.join(source_orgs) if source_orgs else ''

            # Destination organizations
            dests = f.get('destinationObjects', [])
            dest_orgs = [d.get('name', '') for d in dests if d.get('type') == 'Organization']
            dest_org = '; '.join(dest_orgs) if dest_orgs else ''

            # Plans/clusters
            dest_plans = [d.get('name', '') for d in dests if d.get('type') == 'Plan']
            dest_clusters = [d.get('name', '') for d in dests
                             if d.get('type') in ('Cluster', 'GlobalCluster')]

            records.append({
                'flow_id': f.get('id', ''),
                'amount_usd': f.get('amountUSD', 0) or 0,
                'source_org': source_org,
                'destination_org': dest_org,
                'plan': '; '.join(dest_plans),
                'cluster': '; '.join(dest_clusters),
                'flow_date': f.get('date', ''),
                'status': f.get('status', ''),
                'description': (f.get('description', '') or '')[:200],
                'boundary': f.get('boundary', ''),
            })

        return records

    def get_plan_funding(self, plan_id):
        """Financement d'un plan : `fts/flow?planId=` -> `data.incoming`.

        Le financement N'EST PAS sur l'objet plan. Il faut cet appel dedie, qui rend
        l'agregat CALCULE PAR L'API (`fundingTotal`, `pledgeTotal`, `flowCount`).

        ⚠ Ne PAS sommer `data.flows[]` a la main : un flux apparait plusieurs fois
        selon `boundary` / `onBoundary` (incoming / internal / outgoing), et la somme
        naive double-compte. L'agregat de l'API est la reference.
        """
        try:
            data = self._get('fts/flow', {'planId': str(plan_id)})
        except Exception as e:
            print('  HPC funding plan {}: {}'.format(plan_id, str(e)[:70]))
            return {}
        inc = (data.get('data') or {}).get('incoming') or {}
        return {
            'funding_usd': inc.get('fundingTotal') or 0,
            'pledges_usd': inc.get('pledgeTotal') or 0,
            'flow_count': inc.get('flowCount') or 0,
        }

    def get_plans(self, iso3, with_funding=True, max_funded=6):
        """Plans de reponse humanitaire d'un pays, avec besoins ET financement.

        ⚠ BUG CORRIGE 2026-07-25 (cause des 28 plans a zero de la revue). Les besoins
        ne sont PAS dans un dict imbrique : ce sont des scalaires au PREMIER niveau de
        l'objet plan, `revisedRequirements` et `origRequirements`. L'ancien code lisait
        `p['requirements']['revisedRequirements']` -> la cle `requirements` n'existe
        pas -> `{}` -> `.get(...)` -> **0 pour tous les plans, sans erreur**.
        Et le financement n'est pas sur le plan du tout (voir `get_plan_funding`).

        Verifie sur le Soudan : HRP 2026 = 2 866 228 593 requis, 1 134 706 688 finances
        (523 flux), soit 39,6 % de couverture.

        Args:
            with_funding: recuperer le financement (1 appel par plan).
            max_funded: n'appeler le financement que pour les N plans les plus recents
                        (les plans anciens sont rarement utiles et chaque appel coute).

        Returns: liste de dicts, plus recent d'abord. `funding_usd` vaut None quand il
        n'a pas ete recupere, JAMAIS 0 : 0 se lit comme "pas finance", None comme
        "pas demande".
        """
        try:
            data = self._get('plan/country/{}'.format(iso3.upper()))
        except Exception as e:
            print('  HPC plans: {}'.format(e))
            return []

        plans_raw = data.get('data', [])
        records = []
        for p in plans_raw:
            years = [str(y.get('year', '')) for y in (p.get('years') or []) if y.get('year')]
            pv = p.get('planVersion') or {}
            # Scalaires de PREMIER niveau (et non un dict `requirements`).
            req = p.get('revisedRequirements') or p.get('origRequirements') or 0
            records.append({
                'plan_id': p.get('id', ''),
                'plan_name': pv.get('name', '') or p.get('name', ''),
                'plan_type': (p.get('categories') or [{}])[0].get('name', ''),
                'year': ', '.join(years),
                'year_max': max(years) if years else '',
                'start_date': str(pv.get('startDate') or '')[:10],
                'end_date': str(pv.get('endDate') or '')[:10],
                'requirements_usd': req,
                'requirements_orig_usd': p.get('origRequirements') or 0,
                'is_revised': bool(p.get('revisedRequirements')
                                   and p.get('revisedRequirements') != p.get('origRequirements')),
                'funding_usd': None,
                'pledges_usd': None,
                'flow_count': None,
                'coverage_pct': None,
                'is_part_of_gho': bool(pv.get('isPartOfGHO')),
                'released': bool(p.get('isReleased')),
            })

        records.sort(key=lambda r: r['year_max'], reverse=True)
        if with_funding:
            for rec in records[:max_funded]:
                f = self.get_plan_funding(rec['plan_id'])
                if not f:
                    continue
                rec.update(f)
                req = rec['requirements_usd']
                rec['coverage_pct'] = (round(f['funding_usd'] / req * 100, 1)
                                       if req else None)
        # Post-condition : si TOUS les besoins sont a zero, c'est le bug de 2026-07
        # qui revient, pas un pays sans plan. Le dire au lieu de publier des zeros.
        if records and all(not r['requirements_usd'] for r in records):
            print('  HPC: {} plans, TOUS a 0 requis -> lecture des champs a verifier '
                  '(cf. revisedRequirements au premier niveau)'.format(len(records)))
        return records

    def get_emergencies(self, iso3):
        """Get emergencies declared for a country.

        Returns:
            List of emergency dicts: emergency_id, name, status, date, glide_id.
        """
        try:
            data = self._get('emergency/country/{}'.format(iso3.upper()))
        except Exception as e:
            print('  HPC emergencies: {}'.format(e))
            return []

        emergencies_raw = data.get('data', [])
        records = []
        for em in emergencies_raw:
            records.append({
                'emergency_id': em.get('id', ''),
                'name': em.get('name', ''),
                'status': em.get('status', ''),
                'glide_id': em.get('glideId', ''),
                'date': em.get('date', ''),
                'active': em.get('active', False),
            })

        return records

    def get_funding_summary(self, iso3, year=None):
        """Get summarized funding for a country (total in/out).

        Aggregates flows to provide top-level numbers.
        Returns dict: total_funding, total_pledges, n_flows, top_donors, top_clusters.
        """
        flows = self.get_funding_flows(iso3, year=year, limit=500)
        if not flows:
            return {}

        total = sum(f['amount_usd'] for f in flows)
        committed = [f for f in flows if f['status'] == 'commitment']
        pledged = [f for f in flows if f['status'] == 'pledge']

        # Top donors
        donor_totals = {}
        for f in flows:
            if f['source_org']:
                donor_totals[f['source_org']] = donor_totals.get(f['source_org'], 0) + f['amount_usd']
        top_donors = sorted(donor_totals.items(), key=lambda x: x[1], reverse=True)[:10]

        # Top clusters
        cluster_totals = {}
        for f in flows:
            if f['cluster']:
                cluster_totals[f['cluster']] = cluster_totals.get(f['cluster'], 0) + f['amount_usd']
        top_clusters = sorted(cluster_totals.items(), key=lambda x: x[1], reverse=True)[:10]

        return {
            'iso3': iso3.upper(),
            'year': year,
            'total_funding_usd': total,
            'n_flows': len(flows),
            'n_committed': len(committed),
            'n_pledged': len(pledged),
            'top_donors': top_donors,
            'top_clusters': top_clusters,
        }

    @staticmethod
    def save_csv(records, filepath):
        """Save records to CSV."""
        save_csv(records, filepath)


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    iso3 = sys.argv[1] if len(sys.argv) > 1 else 'SDN'
    hpc = HPCClient()

    print('=== HPC/FTS — {} ==='.format(iso3))

    plans = hpc.get_plans(iso3)
    print('\nResponse Plans: {}'.format(len(plans)))
    for p in plans[:5]:
        print('  {} ({}) — Req: ${:,.0f} / Funded: ${:,.0f} ({:.1f}%)'.format(
            p['plan_name'][:50], p['year'],
            p['requirements_usd'], p['funding_usd'], p['coverage_pct']))

    flows = hpc.get_funding_flows(iso3, year=2025, limit=20)
    print('\nFunding Flows (2025): {} flows fetched'.format(len(flows)))
    for f in flows[:10]:
        print('  ${:,.0f} — {} -> {} [{}]'.format(
            f['amount_usd'], f['source_org'][:30], f['destination_org'][:30], f['status']))

    emergencies = hpc.get_emergencies(iso3)
    print('\nEmergencies: {}'.format(len(emergencies)))
    for em in emergencies[:5]:
        print('  [{}] {} — {} ({})'.format(
            em['emergency_id'], em['name'][:50], em['status'], em['glide_id']))
