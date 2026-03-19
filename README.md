# humanitarian-secondary-data

Python clients for humanitarian data APIs. Work in progress — built in the open.

---

## Why this exists

Humanitarian secondary data is fragmented across dozens of APIs and portals. [HDX](https://data.humdata.org) is doing important work to centralize datasets, and the direction is right — but not every organization contributes, datasets go stale, and HDX is one piece of a larger puzzle. Conflict data lives in ACLED. Displacement in UNHCR and IDMC. Food security in HAPI. Funding in HPC/FTS. MSNA on IMPACT's site with no public API.

The 2025 USAID funding cuts have compounded this problem. Information management systems across the humanitarian sector lost capacity, reducing the quality and timeliness of available data at a moment when crises are intensifying.

Donors call for tools that "gather and analyse the necessary data in a coherent and systemic way, allowing for comparability of identified needs and of their severity between and within crises," with "the necessary granularity of data (people in need; multisectoral; age; gender; disability)."

This repository is a practical step in that direction. Python scripts that query humanitarian APIs and produce flat CSVs. No frameworks, no dependencies beyond the standard library.

> **This is a work in progress.** The architecture, output format, and source coverage will evolve. Published early to signal direction and invite collaboration — not to present a finished product.

## Quick start

```bash
git clone https://github.com/13w13/humanitarian-secondary-data.git
cd humanitarian-secondary-data

# Step 1 — Fetch data for Ukraine (IMPACT datasets + Liveuamap conflict events)
python -X utf8 01_fetch.py UKR --only impact,liveuamap --date-from 2026-01-01

# Step 2 — Pick which datasets to download
python -X utf8 01b_download.py --select

# Step 3 — Download all
python -X utf8 01b_download.py
```

Step 1 creates `UKR_data/` with analytical CSVs and a `catalogue/` subfolder (dataset listings with URLs). Step 2 shows a checkbox picker — arrow keys to navigate, space to toggle, enter to confirm. Downloaded files go to `UKR_data/raw/` and are marked with `x` in the catalogue CSV.

Sample output is included in `examples/ukraine/data/`.

**Requirements**: Python 3.8+. Standard library only — no pip install.

## Filters

| Flag | What it does | Example |
|------|-------------|---------|
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

{ISO3}_data/                       # output (created automatically, one per country)
├── data_inventory.csv             # index of all files
├── liveuamap_events.csv           # analytical CSVs
├── fetch_summary.csv
├── catalogue/                     # dataset listings with URLs
│   └── impact_all_resources.csv
└── raw/                           # downloaded files
    └── REACH_UKR_MSNA.xlsx

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

## Disability data gap

One motivation behind this project: making the disability data gap measurable.

Humanitarian data systems rarely include disability as a disaggregation variable. Of the 17 sources we query, only one — HDX HAPI's `humanitarian-needs` endpoint — provides a structured `disabled_marker` field. And even that endpoint returns zero records for some countries.

This is not because persons with disabilities are absent from these crises. It is because data systems do not systematically collect this information. That gap matters for advocacy.

## Contributing

Issues and PRs welcome. To add a source: create `scripts/clients/{source}_client.py` with a class that returns lists of dicts. See `impact_client.py` as reference.

## License

MIT
