# Use cases: four personas, four verified walkthroughs

> Every command and output below was **actually run on 2026-07-25**. Nothing is
> hypothetical. Where a walkthrough exposed a packaging bug, the bug was fixed the
> same day (listed at the bottom).

---

## 1. Information Management Officer: "I need the data files"

*Goal: get the latest DTM datasets for Sudan onto disk, without reading any code.*

```bash
# What exists, is it open, how fresh?  (~10 s, writes nothing)
python -X utf8 scripts/explore.py SDN

# Build the country catalogue (writes SDN_data/catalogue/dtm_portal_datasets.csv)
python -X utf8 01_fetch.py SDN --only dtm_portal

# What is downloadable?
python -X utf8 01b_download.py --catalogue-dir SDN_data/catalogue --scan
#   dtm_portal_datasets.csv - 60 resources (60 downloadable)

# Grab what you need, by keyword
python -X utf8 01b_download.py --catalogue-dir SDN_data/catalogue \
       --output-dir SDN_data/raw --filter "snapshot (6)"
#   [1/1] 100046... 240,988 bytes
```

Result: `2026_24_05_DTM_SDN_IDPs_Returnees_Snapshot_006_Public_v1.xlsx` on disk.
Lebanon behaves differently on purpose: its 60 catalogue rows are all gated, the
tool says so, and points to the published report for the headline figure.

## 2. Project manager: "one figure for my monthly report"

*Goal: the latest displacement figure for Lebanon, safe to paste in a sitrep.*

```bash
python -X utf8 scripts/explore.py LBN        # ~7 s
```

```
3. THE LATEST FIGURE
  « As of 22 July 2026, IOM's DTM recorded 375,090 internally displaced persons
    (IDPs) across Lebanon, representing a nine per cent decrease compared to 15
    July. »
     2026-07-23 | PDF (7 pages) | reliefweb.int/node/4222775
```

The figure comes as a **sentence with its date and URL**, so paste all three. The
tool also warns that the producer's API would say 64,417 (October 2025, 8.8
months stale): the number a naive integration would have used.

## 3. MEAL / regional advisor: "what is the situation, who is affected?"

*Goal: an analytical read on Venezuela after the June earthquake.*

```python
from report_figures import analytical_findings
d = analytical_findings('VEN', source='ACAPS')
```

Output (real): the ACAPS Thematic Report of 2026-07-03, read from the **14-page
PDF** (the ReliefWeb summary is a 1,658-character teaser), naming the affected
groups (`people with disabilities, older people, pregnant, lactating, children,
girls, women, refugees`) and figures with their inline sources
(`6.6 million people felt the earthquake... (OCHA 29/06/2026; ...)`).

Everything is tagged `epistemic_status: analysis`: these are a third party's
findings, to be quoted as **"ACAPS estimates that..."**, never as your own
measurement.

## 4. IM analyst: "let me verify and chart it myself"

*Goal: from raw file to a presentable chart, with the reading validated first.*

```python
from dtm_files import read_sheet, sum_by, checksum
cols, hxl, recs = read_sheet(path)               # picks the DATA sheet by shape
r = sum_by(recs, 'Total Headcount', {'Direction': 'Inflow'},
           date_col='ReportingDate', date_from='2026-07-05', date_to='2026-07-18')
checksum(r, 120099, 'TOTAL INFLOWS 05-18 Jul')   # [EXACT] -> reading validated
```

```bash
python -X utf8 scripts/quick_chart.py "<the xlsx>" --out chart.png
```

The checksum against the published report is the step that matters: **reproduce a
published figure before computing anything new**. Once it passes (5/5 exact across
2 files and 2 countries so far), the same file yields what IOM never published:
per-crossing-point breakdowns, rolling windows, yearly cumulative, and the chart
`analysis/chart_afg_flows.png` (30 months of weekly border flows, the mid-2025
mass-return spike from Iran plainly visible).

---

## Packaging bugs found by these walkthroughs (and fixed)

| Walkthrough step | What was broken | Fix |
|---|---|---|
| README quick start | first source errors (network-blocked), second runs ~13 min | `scripts/explore.py` is the new entry point (7 s) |
| `01b --scan` | **0 downloadable** out of 6,003 resources | `fetch_dtm_portal` rewritten on `browse_catalogue` (direct `download_url` per row) |
| `01b` column pick | picked the dataset *page* URL over the *file* URL | `download_url` checked first |
| `01b` download | HTTP 403 from DTM | per-host headers (DTM rejects the generic UA *and* a bare Chrome UA) |
| `quick_chart` column pick | charted `Total Male` instead of `Total Headcount` | patterns tried by priority, most specific first |

Two lessons worth keeping: **a test suite proves the code works, only a persona
walkthrough proves someone can use it**, and every one of these bugs failed
*silently* (empty scan, wrong column), which is exactly the failure mode this
repository exists to fight.
