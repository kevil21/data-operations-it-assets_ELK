"""
Transform data in Elastic Cloud:

1) Reindex from source -> destination
2) Add derived fields via _update_by_query:
   - risk_level = "High" if operating_system_lifecycle_status in {"EOL","EOS"} else "Low"
   - system_age_years computed from operating_system_installation_date (YYYY-MM-DD)
3) Delete records with missing hostname OR operating_system_provider == "Unknown"

"""

import logging
import dotenv
import os
from datetime import date
from elasticsearch import Elasticsearch

# ----------- CONFIG -----------
dotenv.load_dotenv()  # load from .env file if present
ES_ENDPOINT = os.getenv("ES_ENDPOINT")
ES_API_KEY = os.getenv("ES_API_KEY")

SRC_INDEX = "data-operations-it-assets_elk"
DST_INDEX = "data-operations-it-assets_elk_final"

# ----------- LOGGING ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("transform_data")

def get_client() -> Elasticsearch:
    es = Elasticsearch(
        ES_ENDPOINT,
        api_key=ES_API_KEY,
        verify_certs=True,
        request_timeout=300,
        retry_on_timeout=True,
        max_retries=5,
    )
    try:
        if not es.ping():
            logger.warning("Elasticsearch ping failed. Check endpoint/API key.")
    except Exception as e:
        logger.warning("Ping raised an exception: %s", e)
    return es

def ensure_dest_index(es: Elasticsearch, index_name: str) -> None:
    """
    Serverless-safe index creation: NO settings, only mappings.
    """
    mapping = {
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
                # derived fields we’ll add via update_by_query
                "risk_level": {"type": "keyword"},
                "system_age_years": {"type": "integer"},
            },
            "dynamic": True
        }
    }
    if not es.indices.exists(index=index_name):
        logger.info("Creating destination index '%s'...", index_name)
        # IMPORTANT: pass only mappings — NO settings
        es.indices.create(index=index_name, body=mapping)
    else:
        logger.info("Destination index '%s' already exists; keeping as-is.", index_name)

def reindex(es: Elasticsearch, src: str, dst: str) -> None:
    body = {"source": {"index": src}, "dest": {"index": dst}}
    logger.info("Reindexing '%s' -> '%s' ...", src, dst)
    es.reindex(
        body=body,
        wait_for_completion=True,
        refresh=True,
        requests_per_second=-1,
        slices="auto",
    )
    logger.info("Reindex completed.")

def enrich_fields(es: Elasticsearch, index_name: str) -> None:
    body = {
        "conflicts": "proceed",
        "script": {
            "lang": "painless",
            "params": {
                "now_year": date.today().year  # avoid java.time in Painless
            },
            "source": """
                // ---- risk_level: High for EOL/EOS, else Low
                String lvl = 'Low';
                if (ctx._source.containsKey('operating_system_lifecycle_status')) {
                    def s = ctx._source.operating_system_lifecycle_status;
                    if (s != null) {
                        s = s.toString().toLowerCase();
                        if (s == 'eol' || s == 'eos') { lvl = 'High'; }
                    }
                }
                ctx._source.risk_level = lvl;

                // ---- system_age_years using YYYY-MM-DD string (no java.time)
                def d = ctx._source.get('operating_system_installation_date');
                if (d != null && d != 'Unknown') {
                    try {
                        String ds = d.toString();
                        // Expecting 'YYYY-MM-DD' → take first 4 chars
                        int installYear = Integer.parseInt(ds.substring(0, 4));
                        int years = params.now_year - installYear;
                        // Clamp to non-negative just in case
                        if (years < 0) { years = 0; }
                        ctx._source.system_age_years = years;
                    } catch (Exception e) {
                        ctx._source.system_age_years = null;
                    }
                } else {
                    ctx._source.system_age_years = null;
                }
            """
        },
        "query": { "match_all": {} }
    }

    logger.info("Applying enrichment on '%s' ...", index_name)
    es.update_by_query(
        index=index_name,
        body=body,
        refresh=True,
        wait_for_completion=True,
        slices="auto",
        requests_per_second=-1,
    )
    logger.info("Enrichment completed.")

def delete_bad_records(es: Elasticsearch, index_name: str) -> None:
    delete_query = {
        "query": {
            "bool": {
                "should": [
                    {"bool": {"must_not": {"exists": {"field": "hostname"}}}},
                    {"term": {"operating_system_provider.keyword": "Unknown"}}
                ],
                "minimum_should_match": 1
            }
        }
    }
    logger.info("Deleting docs with missing hostname or Unknown provider from '%s' ...", index_name)
    es.delete_by_query(
        index=index_name,
        body=delete_query,
        refresh=True,
        wait_for_completion=True,
        conflicts="proceed",
        slices="auto",
        requests_per_second=-1,
    )
    logger.info("Delete completed.")

def main() -> None:
    es = get_client()
    ensure_dest_index(es, DST_INDEX)   # create serverless-safe index
    reindex(es, SRC_INDEX, DST_INDEX)  # 1) reindex
    enrich_fields(es, DST_INDEX)       # 2) add derived fields
    delete_bad_records(es, DST_INDEX)  # 3) delete problem docs
    logger.info("All transformations complete on index '%s'.", DST_INDEX)

if __name__ == "__main__":
    main()
