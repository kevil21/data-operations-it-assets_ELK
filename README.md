# IT Asset Analytics Pipeline (Mini Project ELK)

A complete end-to-end **IT Asset Analytics pipeline** built using **Python, pandas, NumPy, Elasticsearch, and Kibana**, to clean, index, transform, and visualize IT asset data.

---


## Phase 0 — Environment Setup

### 1. Install required libraries
```bash
pip install -r requirements.txt
```

**requirements.txt**
```txt
pandas==2.2.3
numpy==2.1.3
elasticsearch==8.15.1
matplotlib==3.9.2
```


---

## Phase 1 — Data Cleaning (pandas + NumPy in Jupyter Notebook)

### Run in Jupyter
```bash
jupyter notebook
```
Open **`Phase_1_Cleaning.ipynb`** and execute all cells.

### Operations performed
| Step | Operation | Description |
|------|------------|-------------|
| 1 | Load dataset | Reads `it_asset_inventory_enriched.csv` |
| 2 | Remove duplicates | Removes duplicate rows based on `hostname` |
| 3 | Trim whitespace | Cleans all text columns |
| 4 | Fill blanks | Replaces empty/NaN with `'Unknown'` |
| 5 | Normalize dates | Converts installation dates to `YYYY-MM-DD` |
| 6 | Save cleaned data | Exports as `it_asset_inventory_cleaned.csv` |

### Output
```
/data/it_asset_inventory_cleaned.csv
```

---

## Phase 2 — Indexing Data in Elasticsearch

### Script: `index_data.py`
Creates an index (`it-assets-raw`) and bulk indexes the cleaned dataset.

```bash
python3 index_data.py
```


### Mapping fields:
- `hostname` (keyword)
- `country` (keyword)
- `operating_system` (keyword)
- `operating_system_provider` (keyword)
- `operating_system_lifecycle_status` (keyword)
- `operating_system_installation_date` (date)

---

## Phase 3 — Transforming and Enriching Data

### Script: `transform_data.py`
Performs transformations and enrichment using Elasticsearch’s Painless scripts.

```bash
python3 transform_data.py
```

### Operations performed:
| Step | Operation | Description |
|------|------------|-------------|
| 1 | Reindex | Copies data from `it-assets-raw` → `it-assets-final` |
| 2 | Add `risk_level` | “High” for `EOL/EOS` OS lifecycle, else “Low” |
| 3 | Add `system_age_years` | Calculates system age based on installation date |
| 4 | Delete invalid records | Removes docs missing `hostname` or `Unknown` provider |


---

## Phase 4 — Visualization in Kibana

### Create a Data View
1. Open Kibana → **Stack Management → Data Views → Create data view**
2. Name: `it-assets-final`
3. Index pattern: `it-assets-final`

### Visualizations
|         Visualization         |      Type      |                    Field                    |
|-------------------------------|----------------|---------------------------------------------|
|       Assets by Country       |       Bar      |              `country.keyword`              |
| Lifecycle Status Distribution |       Pie      | `operating_system_lifecycle_status.keyword` |
|    Risk Level Distribution    |      Donut     |             `risk_level.keyword`            |
|        Top OS Providers       | Horizontal Bar |     `operating_system_provider.keyword`     |

### Save Screenshots
Store charts in:
```
visualization_screenshots/
```
Example:
```
visualization_screenshots/
├── assets_by_country.png
├── lifecycle_distribution.png
├── risk_level_distribution.png
└── top_os_providers.png
```

---

## Phase 5 — Reporting and Documentation

### Deliverables:
|               File               |                 Description                |
|----------------------------------|--------------------------------------------|
| `it_asset_inventory_cleaned.csv` |         Cleaned dataset (Phase 1)          |
|         `index_data.py`          |      Elasticsearch ingestion (Phase 2)     |
|       `transform_data.py`        |  Data transformation and cleanup (Phase 3) |
|   `visualization_screenshots/`   | Screenshots of Kibana dashboards (Phase 4) |
|            `README.md`           |        Full documentation (Phase 5)        |

---

## How to Run Everything
```bash
# Activate venv
source .venv/bin/activate.fish

# Phase 1
jupyter notebook Phase_1_Cleaning.ipynb

# Phase 2
python3 index_data.py

# Phase 3
python3 transform_data.py

# Phase 4
# Open Kibana → Create visualizations
```

---

## Insights & Recommendations
- Identify countries or OS providers with **highest EOL/EOS risk**.
- Prioritize system upgrades for high-risk assets.
- Use Kibana dashboards to continuously monitor asset lifecycle health.

---

## Summary
This project automates IT asset data processing from raw CSV to analytics dashboards using the ELK stack and Python.  
All operations — cleaning, indexing, transforming, and visualizing — are reproducible and script-driven for scalability and reusability.

---

## Author
**Name:** Kevil  
**Organization:** Kyndryl  
**Date:** November 2025  