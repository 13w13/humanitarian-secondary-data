# humanitarian-secondary-data

Python clients for humanitarian data APIs. Work in progress — built in the open.

---

## Why this exists

Humanitarian secondary data is fragmented across dozens of APIs and portals. [HDX](https://data.humdata.org) has done important work to centralize datasets, and the direction is right — but not every organization contributes, datasets go stale, and HDX is one piece of a larger puzzle. Conflict data lives in ACLED. Displacement in UNHCR and IDMC. Food security in HAPI. Funding in HPC/FTS. MSNA on IMPACT's site with no public API.

The 2025 USAID funding cuts have compounded this problem. Information management systems across the humanitarian sector lost capacity, reducing the quality and timeliness of available data at a moment when crises are intensifying.

Donors calls for tools that "gather and analyse the necessary data in a coherent and systemic way, allowing for comparability of identified needs and of their severity between and within crises," with "the necessary granularity of data (people in need; multisectoral; age; gender; disability)."

This repository is a practical step in that direction. Python scripts that query humanitarian APIs and produce flat CSVs. No frameworks, no dependencies beyond the standard library.

> **This is a work in progress.** The architecture, output format, and source coverage will evolve. Published early to signal direction and invite collaboration — not to present a finished product.

## Quick start

Clone the repo and run the example project. It lists all 2025 MSNA datasets from IMPACT/REACH, then downloads them.

```bash
git clone https://github.com/13w13/humanitarian-secondary-data.git
cd humanitarian-secondary-data/examples/msna_global

# Step 1 — Build the catalogue (list available datasets)
python -X utf8 01_fetch.py

# Step 2 — Preview what would be downloaded
python -X utf8 01b_download.py --dry-run

# Step 3 — Download all datasets
python -X utf8 01b_download.py
```

Result (March 2026): 11 MSNA datasets across 7 countries (Somalia, Kenya, Lebanon, DRC, CAR, Ukraine, Haiti), ~250 MB of clean survey data.

**Requirements**: Python 3.8+. Standard library only — no pip install.

## Project structure

Each project follows the same two-step pattern: **fetch** the catalogue, then **download** selected resources.

```
examples/msna_global/              # A complete example project
├── 01_fetch.py                    # Step 1: query APIs → data/catalogue/
├── 01b_download.py                # Step 2: catalogue → data/raw/
└── data/                          # Created automatically
    ├── data_inventory.csv         # Index of everything on disk
    ├── catalogue/                 # Listings (what exists, with URLs)
    │   └── msna_datasets.csv
    └── raw/                       # Downloaded files (analysis-ready)
        ├── REACH_SOM_MSNA_2025.xlsx
        ├── REACH_RDC_MSNA_2025.xlsx
        └── ...
```

**Catalogue** = inventories (titles, URLs, metadata). The analyst reviews and selects. **Raw** = actual data files, ready for analysis. `data_inventory.csv` tracks everything on disk — updated after each fetch and download.

## How it works

Every project follows two steps:

```
01_fetch.py          "what data exists?"       → data/catalogue/
01b_download.py      "download what I need"    → data/raw/
```

`01_fetch.py` queries one or more API clients and saves catalogue CSVs — lists of available datasets with titles, URLs, and metadata. `01b_download.py` reads those CSVs, downloads each file, and updates `data_inventory.csv`.

Two ways to use this, depending on your scope:

**Country assessment** — fetch from multiple sources for one country:
```bash
# Fetches conflict, displacement, food security, funding, etc.
python -X utf8 scripts/fetch_country_data.py PSE
python -X utf8 scripts/fetch_country_data.py PSE --only impact,acled,hapi

# Then download files listed in the catalogue
python -X utf8 scripts/download_catalogue.py --catalogue-dir data/catalogue/ --output-dir data/raw/
```

**Thematic search** — query one source across all countries:
```bash
# List all MSNA datasets globally
python -X utf8 scripts/clients/impact_client.py --msna --output data/catalogue/msna.csv
```

Both examples in `examples/` show these patterns as ready-to-run projects.

## Repository structure

```
scripts/
├── fetch_country_data.py          # Orchestrator: 16 sources for one country
├── download_catalogue.py          # Download files listed in catalogue CSVs
└── clients/                       # API clients (one file = one source)
    ├── impact_client.py           # IMPACT/REACH Resource Centre
    ├── acled_client.py            # ACLED conflict events
    ├── hapi_client.py             # HDX HAPI (13 endpoints)
    ├── config.example.py          # API endpoints template
    └── ...                        # 13 more clients

examples/
├── palestine/                     # Country assessment — uses fetch_country_data.py
│   ├── 01_fetch.py
│   └── 01b_download.py
└── msna_global/                   # Thematic — uses impact_client.py directly
    ├── 01_fetch.py
    └── 01b_download.py
```

## IMPACT/REACH client

The [IMPACT Resource Centre](https://www.impact-initiatives.org/resource-centre/) hosts 21,000+ humanitarian research outputs. No public API exists. This client reverse-engineers the AJAX calls the website uses.

```bash
python -X utf8 scripts/clients/impact_client.py --msna
python -X utf8 scripts/clients/impact_client.py --msna --country PSE
python -X utf8 scripts/clients/impact_client.py --search "disability" --country SDN
python -X utf8 scripts/clients/impact_client.py --programme rna --country LBN --all-pages
```

```python
from clients.impact_client import IMPACTClient
client = IMPACTClient()

datasets = client.search_msna_datasets('PSE')
for d in datasets:
    print(f"{d['pub_date']} | {d['title']} | {d['url']}")
```

## Disability data gap

One motivation behind this project: making the disability data gap measurable.

Humanitarian data systems rarely include disability as a disaggregation variable. Of the sources we work with, only one — HDX HAPI's `humanitarian-needs` endpoint — provides a structured `disabled_marker` field. And even that endpoint returns zero records for some countries.

This is not because persons with disabilities are absent from these crises. It is because data systems do not systematically ask the question. That gap matters for advocacy.

## Sources

Clients are released step by step as they are tested on real country assessments.

| Source | Covers | Auth | Status |
|--------|--------|------|--------|
| **IMPACT/REACH** | MSNA datasets, reports, maps (21,000+ resources) | Public | **Published** |
| ACLED | Conflict events, CAST 6-month forecasts | OAuth2 (free) | Internal |
| HDX HAPI | IDPs, food security (IPC), refugees, humanitarian needs, disability disaggregation | app_id (free) | Internal |
| HDX CKAN | 27,000+ datasets catalogue | Public | Internal |
| UNHCR | Refugee population, asylum seekers, solutions | Public | Internal |
| IDMC | Displacement figures (annual + events) | client_id (free) | Internal |
| INFORM | Risk index (national + subnational) | Public | Internal |
| WFP HungerMap | Food consumption scores | Public | Internal |
| ACAPS | Crisis severity, access constraints | API key (free) | Internal |
| HPC/FTS | Humanitarian funding flows, response plans | Public | Internal |
| IFRC Go | Emergencies, appeals, 3W projects | Public | Internal |
| ReliefWeb | Report listings, sitreps, thematic search | appname (free) | Internal |
| World Bank | 15 development indicators | Public | Internal |
| GDACS | Real-time disaster alerts | Public | Internal |
| DTM/IOM | Displacement tracking datasets | Public | Internal |
| DTM Portal | IOM portal dataset listings | Public | Internal |

## Contributing

Issues and PRs welcome. To add a new source: create `scripts/{source}_client.py` with a class that exposes `search()` or `get_*()` methods returning lists of dicts. See `impact_client.py` as a reference.

## License

MIT
