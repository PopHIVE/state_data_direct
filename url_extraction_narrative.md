# POPHIVE Epiportal: URL Extraction & Data Yield Report

**Date:** 2026-03-29
**Source file:** `epiportal_state_indicators.csv` (523 indicators across 33 states)
**Total unique documentation URLs attempted:** 69

---

## Executive Summary

Of the 523 respiratory disease indicators catalogued across 33 U.S. states, **14 states (42%) yielded usable data**, producing **158,911 total rows** of extracted records. The remaining **19 states (58%) returned zero usable data**, almost entirely because their dashboards are JavaScript-rendered (Tableau, custom JS frameworks) with no underlying static data endpoint.

**138 of 523 indicators (26.4%)** are currently covered by the extracted data. Six states achieved GOOD coverage (>80% of their indicators), four achieved PARTIAL coverage, five returned data but with POOR indicator alignment, and eighteen states completely FAILED extraction.

---

## What Succeeded: 13 States That Yielded Data

### Tier 1 — GOOD Coverage (6 states, 83 of 87 indicators covered)

These states expose data via open APIs (Socrata, ArcGIS FeatureServer, Power BI API) or direct CSV/CKAN downloads, making automated extraction reliable and comprehensive.

| State | Rows | Method | Indicators | Covered | Missing |
|-------|------|--------|------------|---------|---------|
| **Arizona** | 14,703 | Power BI API (semantic query) | 21 | 21 (100%) | — |
| **New York** | 50,000 | Socrata API (`w6ed-sctw`) | 15 | 14 (93%) | pathogen-independent aggregate |
| **Pennsylvania** | 67 | Socrata API (`mrpb-ugjv`) | 14 | 13 (93%) | acute respiratory illness (ARI) |
| **Texas** | 1,000 | ArcGIS FeatureServer (1 layer) | 14 | 12 (86%) | pathogen-independent aggregate |
| **Virginia** | 698 | Direct CSV download (1 file) | 13 | 13 (100%) | — |
| **Vermont** | 1,360 | ArcGIS FeatureServer (1 layer) | 10 | 10 (100%) | — |

**Key insight:** States with Socrata, ArcGIS, or Power BI backends are the most reliably scrapable. Arizona's breakthrough came from reverse-engineering the Power BI API — sending direct semantic queries to the `wabi-us-gov-iowa-api.analysis.usgovcloudapi.net` endpoint, bypassing the JavaScript dashboard entirely. This produced 6 structured CSV files covering cases (statewide + by county + by age group), flu type/subtype breakdowns, healthcare visit percentages, and mortality data across seasons 2020–2026. Virginia and Vermont achieved 100% indicator coverage through straightforward CSV and ArcGIS endpoints respectively.

### Tier 2 — PARTIAL Coverage (4 states, 48 of 71 indicators covered)

Data was extracted but does not fully cover all catalogued indicators, typically missing one or more pathogen categories.

| State | Rows | Method | Indicators | Covered | Missing Pathogens |
|-------|------|--------|------------|---------|-------------------|
| **California** | 4,036 | CKAN API (`respiratory-virus-dashboard`) | 15 | 10 (67%) | COVID-19 |
| **Colorado** | 1,780 | ArcGIS FeatureServer (3 layers) | 26 | 19 (73%) | RSV |
| **Connecticut** | 53 | Socrata API (`8d4q-hwjx`) | 13 | 9 (69%) | RSV |
| **Minnesota** | 157 | Direct CSV (`mls_rsv.csv`) | 17 | 10 (59%) | adenovirus, COVID-19, hMPV, rhinovirus, other coronaviruses |

**Key insight:** California's CKAN portal provides flu and RSV but not COVID-19 data. Colorado's ArcGIS layers cover COVID and flu but not RSV. Minnesota has multiple CSV endpoints (14 raw files downloaded) but the current ingest only processes the RSV-specific file.

### Tier 3 — POOR Coverage (5 states, 7 of 66 indicators nominally touched)

Data was technically retrieved but has low indicator alignment — often the wrong dataset, a data dictionary instead of actual surveillance data, or only a small slice of what the state publishes.

| State | Rows | Method | Issue |
|-------|------|--------|-------|
| **Louisiana** | 84,928 | Chromote → XLSX download | Retrieved COVID test-by-parish file; does not map to flu/RSV ED visit or hospitalization indicators |
| **Michigan** | 70 | Chromote → XLSX download | Retrieved a seasonal immunization spreadsheet, not the respiratory surveillance data |
| **Oregon** | 0 (scraper failed) | Tableau Public dashboards | All 5 URLs are Tableau embeds; Chromote could not extract structured data |
| **Tennessee** | 46 | Chromote → XLSX download | Retrieved the "Coverage Rate Dashboard" data dictionary, not actual flu/RSV surveillance data |
| **Washington** | 13 | Direct CSV download | Only the CDC-format respiratory download (13 rows); state has 18 indicators across more granular datasets |

**Key insight:** The Chromote-based approach (headless browser rendering) sometimes finds downloadable files linked from dashboards, but these are often tangential datasets rather than the primary surveillance tables. Louisiana's 84,928-row file is the largest single download but covers only COVID testing by parish, not the ED/hospitalization indicators catalogued.

---

## What Failed: 19 States That Yielded No Data

### Failure Mode 1 — JavaScript-Only Dashboards (15 states)

The dominant failure pattern. These states publish respiratory data exclusively through interactive dashboards (Power BI, Tableau, custom JavaScript visualizations) with no static data endpoint, no downloadable CSV, and no public API.

| State | Indicators | Dashboard Technology | URLs |
|-------|------------|---------------------|------|
| **Alaska** | 8 | Custom JS | 1 URL |
| **Alabama** | 3 | Custom JS | 1 URL |
| **Florida** | 11 | FL Health Charts (JS) | 2 URLs |
| **Georgia** | 5 | Custom JS / S3-hosted | 2 URLs |
| **Illinois** | 29 | Custom JS (IDPH) | 1 URL |
| **Indiana** | 18 | Custom JS (IN Health) | 2 URLs |
| **Iowa** | 24 | Custom JS | 1 URL |
| **Kentucky** | 27 | Tableau Server (4 views) | 4 URLs |
| **Massachusetts** | 16 | Custom JS | 1 URL |
| **Maryland** | 21 | Custom JS (multiple pages) | 3 URLs |
| **Missouri** | 5 | Custom JS | 1 URL |
| **New Jersey** | 23 | Custom JS | 1 URL |
| **Oklahoma** | 24 | Custom JS (Viral View) | 2 URLs |
| **South Carolina** | 13 | Custom JS | 2 URLs |
| **Utah** | 12 | Custom JS (DHHS) | 1 URL |

**Combined impact:** 239 indicators (45.7% of all 523) are locked behind JS-only dashboards in these 15 states.

> **Note:** Arizona was initially categorized as a JS-only dashboard failure, but was subsequently solved by reverse-engineering the Power BI API backend (see Tier 1 above). This approach may be replicable for other Power BI-based dashboards.

### Failure Mode 2 — API/Download Endpoint Unknown (2 states)

| State | Indicators | Issue |
|-------|------------|-------|
| **North Carolina** | 8 | Dashboard page links to "data behind dashboards" but downloads failed; file URLs may have changed |
| **Ohio** | 12 | data.ohio.gov hosts the dataset but the Socrata dataset ID is unknown; API endpoint not discovered |

### Failure Mode 3 — Complex Multi-Source Architecture (2 states)

| State | Indicators | Issue |
|-------|------------|-------|
| **Oregon** | 16 | All 5 URLs point to Tableau Public embeds; no underlying data API exposed |
| **Wisconsin** | 43 | 7 distinct URLs across different DHS pages; data embedded in HTML tables or JS; highest indicator count of any failed state |

**Wisconsin** stands out as the single largest gap: 43 indicators across 7 different URLs, covering flu, COVID, RSV, hMPV, adenovirus, parainfluenza, and more — all inaccessible via automated scraping.

---

## URL-Level Analysis

### URLs by Data Access Method

| Method | States | URLs | Indicators Covered | Success Rate |
|--------|--------|------|--------------------|--------------|
| Socrata API | NY, PA, CT | 3 | 36 | 100% |
| ArcGIS FeatureServer | CO, TX, VT | 3 | 50 | 100% |
| Power BI API | AZ | 1 | 21 | 100% |
| CKAN API | CA | 1 | 10 | 100% |
| Direct CSV/XLSX | VA, MN, WA, MI, LA, TN | ~10 | 21 | Mixed |
| Tableau Public | OR, KY (partial) | ~9 | 43 | 0% |
| Custom JS Dashboard | 15+ states | ~41 | 0 | 0% |

### All 69 Unique Documentation URLs — Status

The 523 indicators reference 69 unique `documentation_link` URLs. Since `dataset_location` is empty for all 523 rows, the documentation links are the only available pointers to each state's data.

---

## Pathogen Coverage Gap

| Pathogen | Total Indicators | Covered | Gap |
|----------|-----------------|---------|-----|
| Influenza (seasonal) | 163 | 59 | 104 (64%) uncovered |
| COVID-19 | 154 | 41 | 113 (73%) uncovered |
| RSV | 99 | 28 | 71 (72%) uncovered |
| ILI | 28 | 6 | 22 (79%) uncovered |
| ARI | 17 | 4 | 13 (76%) uncovered |
| Human metapneumovirus | 10 | 0 | 10 (100%) uncovered |
| Adenovirus | 9 | 0 | 9 (100%) uncovered |
| Parainfluenza | 9 | 0 | 9 (100%) uncovered |
| Enterovirus/Rhinovirus | 8 | 0 | 8 (100%) uncovered |
| Other Coronaviruses | 6 | 0 | 6 (100%) uncovered |

**The "long tail" pathogens (hMPV, adenovirus, parainfluenza, enterovirus/rhinovirus, other coronaviruses) are exclusively reported by states with JS-only dashboards (IL, IN, IA, NJ, OK, WI) and have 0% extraction coverage.**

---

## Recommendations for Closing the Gap

### High-Impact Targets (large indicator counts with potential API access)

1. **Wisconsin (43 indicators):** DHS pages may have downloadable CSV links behind the HTML; deeper Chromote scraping or direct HTTP inspection of network requests could uncover data endpoints.
2. **Illinois (29 indicators):** IDPH respiratory disease report page; investigate whether weekly PDF/Excel reports are posted with stable URLs.
3. **Kentucky (27 indicators):** Tableau Server embeds — Tableau's undocumented API (`/vizql/`) can sometimes be reverse-engineered to extract underlying data.
4. **Colorado RSV gap:** ArcGIS already works for COVID/flu; RSV may be in an additional layer not yet queried.
5. **Other Power BI dashboards:** The Arizona Power BI API breakthrough may be replicable for other states using Power BI (inspect network requests for `wabi-*-api.analysis.usgovcloudapi.net` or `api.powerbi.com` endpoints).

### Medium-Impact Targets

6. **Ohio (12 indicators):** Likely solvable — data.ohio.gov is a Socrata instance; need to discover the correct dataset ID.
7. **North Carolina (8 indicators):** NCDHHS "data behind dashboards" page previously offered CSV downloads; URLs may just need updating.
8. **Minnesota (17 indicators, 10 covered):** 14 raw files already downloaded but only RSV is ingested. Processing the remaining CSVs could cover adenovirus, COVID, hMPV, and other pathogens.

### Low Feasibility Without Browser Automation

- **Oregon** (Tableau Public), **Kentucky** (Tableau Server), **Florida** (FL Health Charts): Would require Tableau data extraction tooling or manual download workflows.
- **Indiana, Iowa, New Jersey, Oklahoma, South Carolina:** Deep JS dashboards with no known static endpoints.

---

## Summary Statistics

| Metric | Value |
|--------|-------|
| Total indicators catalogued | 523 |
| States represented | 33 |
| Unique documentation URLs | 69 |
| States with successful extraction | 14 (42%) |
| States with zero data | 19 (58%) |
| Indicators covered by extracted data | 138 (26.4%) |
| Indicators uncovered | 385 (73.6%) |
| Total rows extracted | 158,911 |
| Primary failure mode | JS-only dashboards (15 states, 239 indicators) |
| Highest-yield state | Louisiana (84,928 rows, but poor indicator alignment) |
| Best-covered states | Virginia (100%, 13/13) and Arizona (100%, 21/21) |
| Largest gap state | Wisconsin (43 indicators, 0% covered) |
| Key breakthrough | Arizona Power BI API reverse-engineering (14,703 rows from 6 queries) |
