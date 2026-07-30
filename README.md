# humanitarian-secondary-data

Python clients for humanitarian data APIs. A work in progress, built in the open.

---

## Why this exists

Humanitarian secondary data is fragmented across dozens of APIs and portals. [HDX](https://data.humdata.org) is doing important work to centralize datasets, and the direction is right. But not every organization contributes, datasets go stale, and HDX is one piece of a larger puzzle. Conflict data lives in ACLED. Displacement in UNHCR and IDMC. Food security in HAPI. Funding in HPC/FTS. MSNA on IMPACT's site with no public API.

The 2025 USAID funding cuts have compounded this problem. Information management systems across the humanitarian sector lost capacity, reducing the quality and timeliness of available data at a moment when crises are intensifying.

Donors call for tools that "gather and analyse the necessary data in a coherent and systemic way, allowing for comparability of identified needs and of their severity between and within crises," with "the necessary granularity of data (people in need; multisectoral; age; gender; disability)."

This repository is a practical step in that direction. Python scripts that query humanitarian APIs and produce flat CSVs. No frameworks, no dependencies beyond the standard library.

> **This is a work in progress.** The architecture, output format, and source coverage will evolve. Published early to signal direction and invite collaboration, not to present a finished product.

## Quick start: explore a country in under a minute

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

Try `SDN` (data is open, 8.7M IDPs cited from a 35-page PDF) or `PSE` (no DTM coverage, and the tool says so in one second instead of returning an empty result that reads like "no displacement").

### Then: fetch, download, analyse

```bash
python -X utf8 scripts/health_check.py SDN         # are the 17 sources reachable?
python -X utf8 scripts/fetch_country_data.py SDN   # pull everything to CSV
python -X utf8 scripts/run_tests.py                # 57 assertions, no pytest needed
python -X utf8 scripts/run_tests.py skill50        # 50 end-to-end scenarios (~4 min)
```

The suites call the real APIs, so they are slow and they depend on other people's
uptime. A provider outage or a missing optional key reports as SKIP; FAIL is reserved
for something that is actually our fault. If you see FAIL, it is a real finding.

**Requirements**: Python 3.8+. Standard library only, so no pip install. Some sources need a free key (ACLED, ACAPS, IDMC, DTM API); everything in the quick start above works without one.

## Parameters

The first argument is an **ISO 3166-1 alpha-3 country code** (e.g. `SDN`, `UKR`, `SYR`). Each source supports a different set of countries:

| Source | Country coverage |
|--------|-----------------|
| **IMPACT/REACH** | Any country with REACH operations (~50 countries) |
| **Liveuamap** | 80 ISO3 codes mapped to 106 subdomains, see [`references/liveuamap_iso3_mapping.csv`](references/liveuamap_iso3_mapping.csv) |
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
01_fetch.py                        # → fetch data for a country
01b_download.py                    # → download datasets from a catalogue

scripts/
├── explore.py                     # → START HERE: orient yourself on a country
├── health_check.py                # are the 17 sources reachable right now?
├── fetch_country_data.py          # engine: all sources → CSVs
├── download_catalogue.py          # engine: catalogue → downloaded files
├── quick_chart.py                 # a downloaded DTM file → a PNG chart
├── msna_census.py                 # locate MSNA products across three channels
├── wgss_probe.py                  # read a workbook's column names, never its values
├── run_tests.py                   # test runner (three suites, stdlib only)
└── clients/
    ├── recipes.py                 # eight one-call indicators, with caveats
    ├── report_figures.py          # resolve a figure across the four layers
    ├── dtm_files.py               # read a downloaded DTM workbook
    ├── dtm_client.py              # DTM v3 API + catalogue browse/download
    ├── hapi_client.py             # ... one file per source, 16 in total
    └── config.py                  # endpoints, credential resolution, CSV writing

references/                        # lookup tables and API notes, versioned
examples/                          # sample output you can read without running anything
USE_CASES.md                       # four reader profiles, walked through a real session

{ISO3}_data/                       # output (created automatically, one per country)
├── data_inventory.csv             # index of everything fetched
├── raw/                           # API results + downloaded datasets
└── catalogue/                     # dataset listings with download URLs
```

## Sources

| Source | Data | Auth |
|--------|------|------|
| IMPACT/REACH | MSNA datasets, reports, maps (21,000+ resources) | Public |
| Liveuamap | Conflict events with coordinates, 106 regions | Public |
| ACLED | Conflict events, CAST 6-month forecasts | OAuth2 (free) |
| HDX HAPI | IDPs, food security, refugees, humanitarian needs, disability | app_id (free) |
| HDX CKAN | 27,000+ datasets | Public |
| UNHCR | Refugee population, demographics | Public |
| IDMC | Displacement figures | client_id (free) |
| INFORM | Risk index (national + subnational) | Public |
| WFP HungerMap | Food consumption scores, IPC phases | Public |
| ACAPS | Crisis severity, access constraints, analytical products | API key (free) |
| HPC/FTS | Funding flows, response plans | Public |
| IFRC Go | Emergencies, appeals, field reports | Public |
| ReliefWeb | Report listings, situation reports, full report text | appname (free) |
| World Bank | Development indicators | Public |
| GDACS | Disaster alerts | Public |
| DTM/IOM | Displacement tracking: v3 API, catalogue (2,375 datasets), data files | Public |

Sixteen clients covering seventeen sources (DTM has two: its v3 API and its
dataset portal), and all of them are in this repo. The ones marked Public need no
registration; the others need a free key you request from the provider. Nothing
here ships a key: credentials are read from the environment or from your OS
keychain, and `.env` is gitignored.

## Performance notes

**Liveuamap** scraping speed depends on the region. Country subdomains (SDN, SYR, etc.) respond fast with a 1s delay between pages. The main domain `liveuamap.com` (used for UKR) has more aggressive Cloudflare rate-limiting and uses a higher base delay (2.5s/page) to avoid costly retries.

| Region | Pages for 30 days | Estimated time |
|--------|-------------------|----------------|
| SDN, SYR, YEM | 10–30 | 15–45s |
| UKR | 30–50 | 1.5–2.5 min |
| UKR (no date filter, 200 pages) | 200 | 8–10 min |

**Tip**: Always use `--date-from` to limit pagination depth, especially for UKR. This is the single biggest performance lever: 30 days of UKR data takes ~2 minutes instead of 8–10 for a full 200-page crawl.

## Disability data gap

One motivation behind this project: making the disability data gap measurable.

Humanitarian data systems rarely include disability as a disaggregation variable. Of the 17 sources we query, only one (HDX HAPI's `humanitarian-needs` endpoint) provides a structured `disabled_marker` field. And even that endpoint returns zero records for some countries.

This is not because persons with disabilities are absent from these crises. It is because data systems do not systematically collect this information. That gap matters for advocacy.

## Five reading rules

These came out of getting the same numbers wrong repeatedly. They are enforced in
code where that is possible, and written down where it is not.

1. **A humanitarian figure lives on four layers**, and they disagree. The producer's
   API, the data commons that republishes it, the portal that hosts the files, and
   the report that cites it. The freshest layer and the most structured layer are
   never the same one. Say which layer a number came from, and when it was cut.
2. **Reproduce a published figure before computing a new one.** If summing a file
   does not reproduce the total printed in the accompanying report, the reading is
   wrong, not the report. `dtm_files.checksum()` exists for this.
3. **A subset is not a total.** The largest number on a page is usually a region,
   a round, or a cumulative series, not the national figure. Every figure this
   toolkit extracts is classified national / subnational / historical / superlative,
   and only national figures are offered as citable.
4. **Measurement and analysis are different claims.** DTM records what enumerators
   counted. ACAPS estimates what analysts infer. Both are legitimate and they are
   not interchangeable, so each carries an explicit epistemic status.
5. **Absence of data is a finding.** A country with no coverage should say so, not
   return an empty list that reads like an absence of need. Queries assert their
   country scope and fail loudly rather than return another country's rows.

## Adding your own reading layer

The toolkit answers what the data says. It does not answer what your organization
should conclude from it, and it should not: that depends on your mandate, your
sectors, and the populations you are accountable to.

The pattern that works is a thin layer of your own on top: which countries and
sectors you care about, which thresholds mean something in your context, which
claims you refuse to make in public. Keep it separate from this repository so the
toolkit stays reusable and your editorial judgement stays yours.

## Next steps

Roughly in the order they are likely to happen.

- **Publish the agent instructions.** This toolkit was built to be driven
  conversationally, and the prompt layer that does so is currently separate. The
  generic part belongs here.
- **A written brief, not just figures.** Assemble the recipes, a chart, and the
  citable sentences into a short situation note with its sources attached.
- **Column-level MSNA census.** `msna_census.py` finds MSNA products from their
  metadata, which under-detects badly: a questionnaire's contents are not in its
  title. The open files need opening, and `wgss_probe.py` is the start of that.
- **Widen the file reader.** `dtm_files.py` handles DTM workbooks. The same
  shape-based sheet detection should work for REACH and cluster files.
- **More sources.** IPC directly rather than through WFP, and FEWS NET.

## Contributing

Issues and PRs welcome. To add a source: create `scripts/clients/{source}_client.py`
with a class that returns lists of dicts. See `impact_client.py` as reference. Two
conventions worth keeping: standard library only, so the clients stay runnable on a
locked-down machine, and no silent failure. If an API ignores a parameter or renames
a field, the client should raise rather than return plausible wrong data.

## License

MIT
