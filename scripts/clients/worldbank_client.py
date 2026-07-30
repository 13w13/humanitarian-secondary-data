"""
World Bank Indicators API Client
==================================
Query World Bank for development indicators (GDP, poverty, education, health).
Public API — no authentication required.

Usage:
    from worldbank_client import WorldBankClient
    wb = WorldBankClient()
    gdp = wb.get_indicator('LBN', 'NY.GDP.PCAP.CD')
    poverty = wb.get_indicator('LBN', 'SI.POV.DDAY')
    profile = wb.get_country_profile('LBN')
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
from urllib.request import Request, urlopen
from urllib.parse import urlencode

from config import (
    WORLDBANK_BASE as WB_BASE, DEFAULT_TIMEOUT, USER_AGENT, save_csv
)


class WorldBankClient:
    """Client for World Bank Indicators API v2 (public, no auth)."""

    # Key indicators for humanitarian context
    KEY_INDICATORS = {
        'NY.GDP.PCAP.CD': 'GDP per capita (current US$)',
        'NY.GDP.MKTP.KD.ZG': 'GDP growth (annual %)',
        'SI.POV.DDAY': 'Poverty headcount ratio ($2.15/day)',
        'SI.POV.NAHC': 'Poverty headcount ratio (national)',
        'SP.POP.TOTL': 'Population total',
        'SP.DYN.LE00.IN': 'Life expectancy at birth',
        'SE.ADT.LITR.ZS': 'Adult literacy rate (%)',
        'SE.PRM.ENRR': 'Primary school enrollment (gross %)',
        'SH.XPD.CHEX.PC.CD': 'Health expenditure per capita',
        'SH.DYN.MORT': 'Under-5 mortality rate (per 1000)',
        'SP.URB.TOTL.IN.ZS': 'Urban population (%)',
        'SL.UEM.TOTL.ZS': 'Unemployment (%)',
        'FP.CPI.TOTL.ZG': 'Inflation (CPI, annual %)',
        'SM.POP.REFG': 'Refugee population by country of asylum',
        'SM.POP.REFG.OR': 'Refugee population by country of origin',
    }

    def __init__(self):
        self.base = WB_BASE

    def _get(self, endpoint, params=None):
        """GET request to World Bank API.

        ⚠ `mrnev` et `per_page` sont INCOMPATIBLES : `mrnev=1&per_page=500` renvoie
        **HTTP 400**, alors que `mrnev=1` seul marche (mesure 2026-07-25). Comme
        `per_page` est un defaut du client, tout appel `mrnev` echouait en 400 et
        remontait comme "indicateur indisponible".
        """
        default_params = {'format': 'json', 'per_page': 500}
        if params and ('mrnev' in params or 'mrv' in params):
            default_params.pop('per_page')
        if params:
            default_params.update(params)
        url = '{}/{}?{}'.format(self.base, endpoint, urlencode(default_params))
        req = Request(url, headers={'User-Agent': USER_AGENT})
        resp = json.loads(urlopen(req, timeout=DEFAULT_TIMEOUT).read())
        # WB API returns [metadata, data] — guard against error responses
        if isinstance(resp, list) and len(resp) == 2:
            return resp[1] or []
        if isinstance(resp, list):
            return resp
        return []  # error response (dict with message) — return empty

    def get_indicator(self, iso3, indicator_id, year_from=None, year_to=None):
        """Fetch a specific indicator for a country.

        Args:
            iso3: ISO3 country code
            indicator_id: World Bank indicator code (e.g., 'NY.GDP.PCAP.CD')
            year_from: Start year
            year_to: End year

        Returns list of dicts: year, value, indicator_name.
        """
        endpoint = 'country/{}/indicator/{}'.format(iso3.upper(), indicator_id)
        params = {}
        if year_from and year_to:
            params['date'] = '{}:{}'.format(year_from, year_to)
        elif year_from:
            params['date'] = '{}:{}'.format(year_from, 2030)

        data = self._get(endpoint, params)

        # ⚠ LA BANQUE MONDIALE REND SES ERREURS EN HTTP 200. Un indicateur inexistant
        # ou un pays invalide donne `[{"message":[{"id":"120","key":"Invalid value",
        # "value":"..."}]}]`, une LISTE comme une reponse valide. Sans ce test, la
        # boucle ne trouve pas de `value`, rend [] et l'erreur se lit "pas de donnee".
        if isinstance(data, list) and data and isinstance(data[0], dict) \
                and 'message' in data[0]:
            msgs = data[0].get('message') or [{}]
            raise ValueError('World Bank a repondu une ERREUR en HTTP 200 pour '
                             '{}/{} : {}'.format(iso3, indicator_id,
                                                 str(msgs[0].get('value'))[:110]))

        records = []
        for item in data:
            if item.get('value') is not None:
                records.append({
                    # `iso3` : la reponse porte `countryiso3code`, on le prefere a
                    # l'argument pour ne pas etiqueter au pays demande une ligne qui
                    # decrirait un agregat regional.
                    'iso3': str(item.get('countryiso3code') or iso3).upper(),
                    'country_name': (item.get('country') or {}).get('value', ''),
                    'year': item.get('date', ''),
                    'indicator_id': indicator_id,
                    # Libelle donne par l'API, pas notre table interne : nos libelles
                    # derivaient de ceux de la Banque mondiale.
                    'indicator_name': (item.get('indicator') or {}).get('value', ''),
                    'value': item.get('value', ''),
                    'unit': item.get('unit', ''),
                })
        return sorted(records, key=lambda r: r['year'])

    def get_latest_value(self, iso3, indicator_id):
        """Derniere valeur NON VIDE d'un indicateur, via `mrnev=1`.

        `mrnev` = most recent non-empty value : l'API remonte elle-meme au dernier
        millesime renseigne. Sans lui, il faut tirer 10 ans de serie et esperer que la
        derniere annee ne soit pas vide, ce qui est justement le cas courant dans les
        pays en crise (dernier PIB Soudan bien anterieur a l'annee en cours).
        """
        try:
            data = self._get('country/{}/indicator/{}'.format(iso3.upper(), indicator_id),
                             {'mrnev': '1'})
        except Exception as e:
            print('  World Bank {}: {}'.format(indicator_id, str(e)[:70]))
            return None
        if isinstance(data, list) and data and isinstance(data[0], dict) \
                and 'message' in data[0]:
            return None
        for item in data or []:
            if item.get('value') is not None:
                return {
                    'iso3': str(item.get('countryiso3code') or iso3).upper(),
                    'indicator_id': indicator_id,
                    'indicator_name': (item.get('indicator') or {}).get('value', ''),
                    'year': item.get('date', ''),
                    'value': item.get('value'),
                }
        return None

    def get_country_profile(self, iso3, year_from=2015, use_mrnev=True):
        """Indicateurs cles d'un pays (contexte structurel).

        `use_mrnev=True` interroge la derniere valeur non vide par indicateur : dans un
        pays en crise, la derniere annee de la fenetre est souvent vide, et un profil
        base sur `year_from` rendait alors "indicateur indisponible" a tort.
        """
        records = []
        for ind_id, ind_name in self.KEY_INDICATORS.items():
            try:
                if use_mrnev:
                    latest = self.get_latest_value(iso3, ind_id)
                    if not latest:
                        continue
                    records.append({
                        'iso3': latest['iso3'],
                        'indicator_id': ind_id,
                        # libelle de l'API, notre table interne en repli seulement
                        'indicator_name': latest['indicator_name'] or ind_name,
                        'latest_year': latest['year'],
                        'latest_value': latest['value'],
                        'source': 'mrnev',
                    })
                    continue
                data = self.get_indicator(iso3, ind_id, year_from=year_from)
                if data:
                    latest = data[-1]      # trie croissant, le dernier est le plus recent
                    records.append({
                        'iso3': latest['iso3'],
                        'indicator_id': ind_id,
                        'indicator_name': latest['indicator_name'] or ind_name,
                        'latest_year': latest['year'],
                        'latest_value': latest['value'],
                        'source': 'serie depuis {}'.format(year_from),
                    })
            except Exception as e:
                # On DIT quel indicateur a echoue : un `pass` muet a fait croire
                # pendant des mois que la Banque mondiale n'avait aucun indicateur
                # handicap, alors qu'elle en expose 1 333.
                print('  World Bank {} indisponible pour {} : {}'.format(
                    ind_id, iso3.upper(), str(e)[:70]))
        return records

    def get_country_info(self, iso3):
        """Get basic country info (region, income level, capital).

        Returns dict with country metadata.
        """
        data = self._get('country/{}'.format(iso3.upper()))
        if not data:
            return {}
        c = data[0] if isinstance(data, list) else data
        return {
            'iso3': c.get('id', ''),
            'name': c.get('name', ''),
            'capital': c.get('capitalCity', ''),
            'region': c.get('region', {}).get('value', ''),
            'income_level': c.get('incomeLevel', {}).get('value', ''),
            'lending_type': c.get('lendingType', {}).get('value', ''),
            'latitude': c.get('latitude', ''),
            'longitude': c.get('longitude', ''),
        }

    @staticmethod
    def save_csv(records, filepath):
        """Save records to CSV."""
        save_csv(records, filepath)


# ── Standalone test ─────────────────────────────────────────
if __name__ == '__main__':
    iso3 = sys.argv[1] if len(sys.argv) > 1 else 'LBN'
    wb = WorldBankClient()

    print('=== World Bank — {} ==='.format(iso3))

    info = wb.get_country_info(iso3)
    if info:
        print('Country: {} ({})'.format(info['name'], info['iso3']))
        print('Region: {} | Income: {}'.format(info['region'], info['income_level']))

    print('\nKey indicators:')
    profile = wb.get_country_profile(iso3, year_from=2018)
    for p in profile:
        val = p['latest_value']
        if isinstance(val, float):
            val = '{:,.2f}'.format(val)
        print('  {} ({}) = {} [{}]'.format(
            p['indicator_name'][:45], p['indicator_id'], val, p['latest_year']))
