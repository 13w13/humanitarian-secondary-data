"""
Re-record who_don_sample.json from the live WHO API (network, no key)
=====================================================================
    python -X utf8 scripts/fixtures/record_who_don_fixture.py

The offline suite (test_hardening, section 12) serves this file from 127.0.0.1. Each
notice below is a trap the client must handle: a disease named after a country
(Sudan virus, Crimean-Congo), South Sudan, a multi-country title without a dash, a
notice tagged Global or region-only, a slug UrlName, a DON number out of date order.
Summaries are cut to 14 words. Re-record only if WHO changes the feed, then re-run
`run_tests.py hard`: the expected counts in section 12 depend on this selection.
"""
import json
import os
import sys

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), 'clients'))

from urllib.parse import quote, urlencode          # noqa: E402
from urllib.request import Request, urlopen        # noqa: E402

from config import (USER_AGENT, WHO_DON_BASE, WHO_TAXA_BASE,  # noqa: E402
                    WHO_COUNTRY_TAXONOMY_ID)

DEST = os.path.join(HERE, 'who_don_sample.json')
DON_IDS = ['2026-DON627', '2026-DON626', '2026-DON625', '2026-DON624', '2026-DON622',
           '2026-DON618', '2026-DON621', '2026-DON613', '2025-DON581', '2022-DON366',
           '2025-DON566', '2023-DON466', '2022-DON386', '2023-DON362', '2024-DON523',
           '2019-DON205']
EXTRA_TAXA = ('cog', 'gin', 'isr', 'global', 'european-region-(euro)',
              'eastern-mediterranean-region-(emro)')
SELECT = 'Id,Title,PublicationDateAndTime,LastModified,UrlName,DonId,Summary,regionscountries'


def get(url, params):
    full = '{}?{}'.format(url, urlencode(params, quote_via=quote, safe="$,'():/"))
    req = Request(full, headers={'User-Agent': USER_AGENT, 'Accept': 'application/json'})
    with urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())['value']


def main():
    taxa, skip = [], 0
    while True:
        page = get(WHO_TAXA_BASE, {'$filter': 'TaxonomyId eq ' + WHO_COUNTRY_TAXONOMY_ID,
                                   '$top': '100', '$skip': str(skip),
                                   '$select': 'Id,Title,Name,Description,FullUrl,ParentId'})
        taxa += page
        if len(page) < 100:
            break
        skip += 100
    by_id = {t['Id']: t for t in taxa}
    by_name = {t['Name']: t for t in taxa}

    notices = []
    for don in DON_IDS:
        got = get(WHO_DON_BASE, {'sf_culture': 'en', '$select': SELECT,
                                 '$filter': "DonId eq '{}'".format(don)})
        if len(got) != 1:
            raise SystemExit('{}: {} notices found, expected 1'.format(don, len(got)))
        words = (got[0].get('Summary') or '').split()
        got[0]['Summary'] = ' '.join(words[:14]) + (' ...' if len(words) > 14 else '')
        notices.append(got[0])

    keep = {g for n in notices for g in n['regionscountries']}
    keep |= {by_name[k]['Id'] for k in EXTRA_TAXA}
    for g in list(keep):                       # add every ancestor (WHO region)
        parent = by_id[g].get('ParentId')
        while parent and parent in by_id:
            keep.add(parent)
            parent = by_id[parent].get('ParentId')
    fixture = {
        '_note': ('Recorded from the public WHO API (no key): {} and {}. Trimmed with '
                  '$select, summaries cut to 14 words. Used offline by test_hardening '
                  '(section 12). Re-record: scripts/fixtures/record_who_don_fixture.py'
                  .format(WHO_DON_BASE, WHO_TAXA_BASE)),
        'taxa': [{k: t.get(k) for k in ('Id', 'Title', 'Name', 'Description', 'FullUrl',
                                         'ParentId')} for t in taxa if t['Id'] in keep],
        'notices': sorted(notices, key=lambda n: n['PublicationDateAndTime'], reverse=True),
    }
    with open(DEST, 'w', encoding='utf-8', newline='\n') as f:
        json.dump(fixture, f, ensure_ascii=False, indent=1)
        f.write('\n')
    print('{} notices, {} taxonomy entries -> {}'.format(
        len(notices), len(fixture['taxa']), DEST))


if __name__ == '__main__':
    main()
