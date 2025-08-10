import logging
import psycopg2
from datetime import datetime, timedelta
import json
from fuzzywuzzy import fuzz
import argparse
import sys

# Imports from config and extractors
from config import DB_PARAMS, InstitutionType, DEFAULT_REFRESH_DAYS
from extractors import extractor_registry, BaseExtractor  # BaseExtractor defined here

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BaseExtractor:
    def __init__(self, country):
        self.country = country.upper()  # Ensure ISO3 uppercase
        self.conn = psycopg2.connect(**DB_PARAMS)
        self.cur = self.conn.cursor()

    # ... (All previous BaseExtractor methods unchanged: fetch_data, normalize, deduplicate, insert_to_db, needs_refresh, run, close, fetch_avma_vet)

    # Note: Shared methods like fetch_avma_vet stay here

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract medical institutions by country (ISO3 code)")
    parser.add_argument('--country', type=str, help="Comma-separated ISO3 codes (e.g., USA,CAN)", required=True)
    parser.add_argument('--force', action='store_true', help="Force refresh regardless of last_updated")
    parser.add_argument('--refresh-days', type=int, default=DEFAULT_REFRESH_DAYS, help="Days since last update to trigger refresh")
    args = parser.parse_args()

    countries = [c.strip().upper() for c in args.country.split(',')]
    for country in countries:
        if country in extractor_registry:
            extractor_class = extractor_registry[country]
            extractor = extractor_class()
            extractor.run(force=args.force)  # Pass force; needs_refresh uses args.refresh_days if overridden
        else:
            logger.error(f"Unknown country: {country}")
            sys.exit(1)