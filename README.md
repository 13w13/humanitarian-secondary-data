# humanitarian-secondary-data

Python clients for humanitarian data APIs. Work in progress — built in the open.

---

## Why this exists

Humanitarian secondary data is fragmented across dozens of APIs and portals. [HDX](https://data.humdata.org) is doing important work to centralize datasets, and the direction is right — but not every organization contributes, datasets go stale, and HDX is one piece of a larger puzzle. Conflict data lives in ACLED. Displacement in UNHCR and IDMC. Food security in HAPI. Funding in HPC/FTS. MSNA on IMPACT's site with no public API.

The 2025 USAID funding cuts have compounded this problem. Information management systems across the humanitarian sector lost capacity, reducing the quality and timeliness of available data at a moment when crises are intensifying.

Donors call for tools that "gather and analyse the necessary data in a coherent and systemic way, allowing for comparability of identified needs and of their severity between and within crises," with "the necessary granularity of data (people in need; multisectoral; age; gender; disability)."

This repository is a practical step in that direction. Python scripts that query humanitarian APIs and produce flat CSVs. No frameworks, no dependencies beyond the standard library.

> **This is a work in progress.** The architecture, output format, and source coverage will evolve. Published early to signal direction and invite collaboration — not to present a finished product.

## Quick start — explore a country in under a minute

```bash
git clone https://github.com/13w13/humanitarian-secondary-data.git
cd humanitarian-secondary-data

python -X utf8 scripts/explore.py LBN
```

No API key, no `pip install`, nothing written to disk. In about 7 seconds you get:

```
1. WHAT EXISTS          60 DTM datasets, 0 downloadable, 60 gated
                        latest: Jul 23 2026 | Lebanon IDP Tracking Round 109 | gated
2. FRESHNESS BY LAYER   D. publication  2026-07-23
                        C. portal       Jul 23 2026
                        A. producer API 2025-10-31   <- 8.8 MONTHS BEHIND
3. THE LATEST FIGURE    « As of 22 July 2026, IOM's DTM recorded 375,090 internally
                          displaced persons (IDPs) across Lebanon, representing a nine
                          per cent decrease compared to 15 July. »
                          2026-07-23 | PDF (7 pages) | reliefweb.int/node/4222775
4. WHAT'S MISSING       the v3 API would say 64,417 (Oct 2025). It is NOT current.
```

That contrast is the whole point of this repository. **The producer's own API says 64,417. The published report says 375,090.** Both are real; one is eight months stale while still reporting the operation as "Active". A tool that queries one API and prints the number gets this wrong, silently.

Try `SDN` (data is open, 8.7M IDPs cited from a 35-page PDF) or `PSE` (no DTM coverage — the tool says so in one second instead of returning an empty result that reads like "no displacement").

### Then: fetch, download, analyse

```bash
python -X utf8 scripts/health_check.py SDN         # status of all 17 sources
python -X utf8 scripts/fetch_country_data.py SDN   # pull everything to CSV
python -X utf8 scripts/run_tests.py                # 61 assertions, no pytest needed
```

**Requirements**: Python 3.8+. Standard library only — no pip install. Some sources need a free key (ACLED, ACAPS, IDMC, DTM API); everything in the quick start above works without one.

## Parameters

The first argument is an **ISO 3166-1 alpha-3 country code** (e.g. `SDN`, `UKR`, `SYR`). Each source supports a different set of countries:

| Source | Country coverage |
|--------|-----------------|
| **IMPACT/REACH** | Any country with REACH operations (~50 countries) |
| **Liveuamap** | 80 ISO3 codes mapped to 106 subdomains — see [`references/liveuamap_iso3_mapping.csv`](references/liveuamap_iso3_mapping.csv) |
| **ACLED** | All countries with recorded conflict events |
| **HDX HAPI** | All countries in the Humanitarian Data Exchange |
| **HDX CKAN** | Any country with datasets on HDX |
| **UNHCR** | Countries with refugee populations |
| **IDMC** | Countries with displacement data |
| **INFORM** | 191 countries (risk index) |
| **WFP HungerMap** | Countries with food consumption monitoring |
| **World Bank** | All 217 World Bank economies |
| **ACAPS** | Countries with active crises |
| **GDACS** | Global (disaster alerts near the country) |
| **HPC/FTS** | Countries with humanitarian response plans |
| **IFRC Go** | Countries with IFRC operations |
| **DTM/IOM** | Countries with displacement tracking |
| **ReliefWeb** | Any country with reports filed |

If a source has no data for the given country, it returns zero records and moves on.

## Filters

| Flag | What it does | Example |
|------|-------------|---------|
| `ISO3` | Country code (required first argument) | `SDN`, `UKR`, `SYR` |
| `--only` | Sources to query (comma-separated) | `--only impact,liveuamap` |
| `--date-from` | Start date (YYYY-MM-DD) | `--date-from 2025-01-01` |
| `--date-to` | End date (YYYY-MM-DD) | `--date-to 2025-12-31` |
| `--max-pages` | Pagination depth for Liveuamap (default 200) | `--max-pages 50` |
| `--skip-acled` | Skip sources that need API keys | |
| `--output-dir` | Override output directory | |

```bash
# All sources for Sudan
python -X utf8 01_fetch.py SDN

# Liveuamap conflict events for Syria, 2025 only
python -X utf8 01_fetch.py SYR --only liveuamap --date-from 2025-01-01 --date-to 2025-12-31

# Iran, quick test (10 pages)
python -X utf8 01_fetch.py IRN --only liveuamap --max-pages 10

# Download with interactive selection
python -X utf8 01b_download.py --select

# Preview without downloading
python -X utf8 01b_download.py --dry-run
```

## Repository structure

```
01_fetch.py                        # → run this: fetch data for a country
01b_download.py                    # → run this: download datasets from catalogue

scripts/
├── fetch_country_data.py          # engine: 17 sources → CSVs
├── download_catalogue.py          # engine: catalogue → downloads
└── clients/                       # one file per data source
    ├── impact_client.py
    ├── liveuamap_client.py
    └── ...

references/
├── liveuamap_event_types.csv      # cat_id → event_type mapping (76 types)
└── liveuamap_iso3_mapping.csv     # ISO3 → subdomain mapping (80 countries)

{ISO3}_data/                       # output (created automatically, one per country)
├── data_inventory.csv             # index of all files
├── raw/                           # all data: API results + downloaded datasets
│   ├── liveuamap_events.csv
│   ├── hapi_idps.csv
│   └── REACH_UKR_MSNA.xlsx
└── catalogue/                     # dataset listings with download URLs
    └── impact_all_resources.csv

examples/
└── ukraine/data/                  # sample output (800 events + IMPACT catalogue)
```

## Sources

| Source | Data | Auth | Status |
|--------|------|------|--------|
| **IMPACT/REACH** | MSNA datasets, reports, maps (21,000+ resources) | Public | Published |
| **Liveuamap** | Conflict events with coordinates, 106 regions | Public | Published |
| ACLED | Conflict events, CAST 6-month forecasts | OAuth2 (free) | Internal |
| HDX HAPI | IDPs, food security, refugees, humanitarian needs, disability | app_id (free) | Internal |
| HDX CKAN | 27,000+ datasets | Public | Internal |
| UNHCR | Refugee population, demographics | Public | Internal |
| IDMC | Displacement figures | client_id (free) | Internal |
| INFORM | Risk index (national + subnational) | Public | Internal |
| WFP HungerMap | Food consumption scores | Public | Internal |
| ACAPS | Crisis severity, access constraints | API key (free) | Internal |
| HPC/FTS | Funding flows, response plans | Public | Internal |
| IFRC Go | Emergencies, appeals, field reports | Public | Internal |
| ReliefWeb | Report listings, situation reports | appname (free) | Internal |
| World Bank | Development indicators | Public | Internal |
| GDACS | Disaster alerts | Public | Internal |
| DTM/IOM | Displacement tracking | Public | Internal |

Published = code in this repo. Internal = tested, will be released progressively.

## Performance notes

**Liveuamap** scraping speed depends on the region. Country subdomains (SDN, SYR, etc.) respond fast with a 1s delay between pages. The main domain `liveuamap.com` (used for UKR) has more aggressive Cloudflare rate-limiting and uses a higher base delay (2.5s/page) to avoid costly retries.

| Region | Pages for 30 days | Estimated time |
|--------|-------------------|----------------|
| SDN, SYR, YEM | 10–30 | 15–45s |
| UKR | 30–50 | 1.5–2.5 min |
| UKR (no date filter, 200 pages) | 200 | 8–10 min |

**Tip**: Always use `--date-from` to limit pagination depth, especially for UKR. This is the single biggest performance lever — 30 days of UKR data takes ~2 minutes instead of 8–10 for a full 200-page crawl.

## Disability data gap

One motivation behind this project: making the disability data gap measurable.

Humanitarian data systems rarely include disability as a disaggregation variable. Of the 17 sources we query, only one — HDX HAPI's `humanitarian-needs` endpoint — provides a structured `disabled_marker` field. And even that endpoint returns zero records for some countries.

This is not because persons with disabilities are absent from these crises. It is because data systems do not systematically collect this information. That gap matters for advocacy.

## Contributing

Issues and PRs welcome. To add a source: create `scripts/clients/{source}_client.py` with a class that returns lists of dicts. See `impact_client.py` as reference.

## License

MIT
