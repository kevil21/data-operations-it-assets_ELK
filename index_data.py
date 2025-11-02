#!/usr/bin/env python3
"""
Index cleaned IT Asset CSV into Elastic Cloud.

- Input CSV: data/it_asset_inventory_cleaned.csv
- Index name: data-operations-it-assets_elk
- Auth: Elastic Cloud API key (provided below)

Run:
  python index_data.py
"""

import os
import csv
import sys
import logging
from pathlib import Path
from typing import Iterator, Dict, Any

try:
    from elasticsearch import Elasticsearch, helpers
except Exception:
    print("Missing dependency 'elasticsearch'. Install with: pip install elasticsearch==8.12.1")
    raise

# ---------- Config ----------
CSV_PATH = Path("data/it_asset_inventory_cleaned.csv")
INDEX_NAME = "data-operations-it-assets_elk"

# Your Elastic Cloud endpoint + API key (as provided)
ES_ENDPOINT = "https://my-elasticsearch-project-c9f0a6.es.us-east-1.aws.elastic.cloud:443"
ES_API_KEY = "Yl80dVJKb0JWVXY2SFFZTnNEWTk6UzlYWlAtQkJsVF9ic25mNGkybG9BUQ=="  # api_key can be a base64 string

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("index_data")


def get_client() -> Elasticsearch:
    """
    Create an Elasticsearch 8.x client for Elastic Cloud using API key auth.
    """
    es = Elasticsearch(
        ES_ENDPOINT,
        api_key=ES_API_KEY,
        request_timeout=120,
        retry_on_timeout=True,
        max_retries=3,
        verify_certs=True,  # Elastic Cloud has valid certs
    )
    try:
        if not es.ping():
            logger.warning("Elasticsearch ping failed; check endpoint/API key.")
    except Exception as e:
        logger.warning("Ping raised an exception: %s", e)
    return es


def ensure_index(es: Elasticsearch, index_name: str) -> None:
    """
    Create index with a practical mapping if it doesn't exist.
    - Categorical fields as keyword
    - Date normalized (YYYY-MM-DD)
    - Room for future fields via dynamic mapping
    """
    mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 1},
        "mappings": {
            "properties": {
                "hostname": {"type": "keyword"},
                "country": {"type": "keyword"},
                "operating_system": {"type": "keyword"},
                "operating_system_provider": {"type": "keyword"},
                "operating_system_lifecycle_status": {"type": "keyword"},
                "operating_system_installation_date": {
                    "type": "date",
                    "format": "yyyy-MM-dd||strict_date_optional_time||epoch_millis"
                },
                # Fields added later by your transform step (safe to predeclare)
                "risk_level": {"type": "keyword"},
                "system_age_years": {"type": "integer"},
            },
            "dynamic": True  # accept any extra columns present in the CSV
        }
    }

    try:
        exists = es.indices.exists(index=index_name)
    except Exception:
        exists = False

    if not exists:
        logger.info("Creating index '%s'...", index_name)
        es.indices.create(index=index_name, body=mapping)
    else:
        logger.info("Index '%s' already exists; skipping creation.", index_name)


def read_csv_rows(path: Path) -> Iterator[Dict[str, Any]]:
    """
    Stream rows from CSV as dicts.
    """
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield row


def actions_from_rows(rows: Iterator[Dict[str, Any]], index_name: str):
    """
    Turn CSV rows into bulk index actions.
    - Use 'hostname' as _id when present for idempotent upserts
    """
    for row in rows:
        doc_id = (row.get("hostname") or "").strip() or None
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_id": doc_id,
            "_source": row
        }


def main() -> None:
    if not CSV_PATH.exists():
        logger.error("CSV not found: %s", CSV_PATH.resolve())
        sys.exit(1)

    es = get_client()
    ensure_index(es, INDEX_NAME)

    logger.info("Reading rows from %s", CSV_PATH)
    rows_iter = read_csv_rows(CSV_PATH)
    actions = actions_from_rows(rows_iter, INDEX_NAME)

    logger.info("Starting bulk indexing into '%s' ...", INDEX_NAME)
    try:
        indexed, _ = helpers.bulk(
            es,
            actions,
            refresh=True,
            chunk_size=2000,
            request_timeout=120
        )
        logger.info("Bulk indexing completed. Indexed actions: %s", indexed)
    except Exception as e:
        logger.exception("Bulk indexing failed: %s", e)
        sys.exit(2)


if __name__ == "__main__":
    main()
