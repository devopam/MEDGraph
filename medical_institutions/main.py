import logging
import psycopg2
from datetime import datetime, timedelta
import json
from fuzzywuzzy import fuzz
import argparse
import sys

# Imports from config and extractors
from config import DB_PARAMS, InstitutionType, DEFAULT_REFRESH_DAYS
from extractors import extractor_registry

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract medical institutions by country (ISO3 code)")
    parser.add_argument('--country', type=str, help="Comma-separated ISO3 codes (e.g., USA,CAN,CHN,IND)", required=True)
    parser.add_argument('--force', action='store_true', help="Force refresh regardless of last_updated")
    parser.add_argument('--refresh-days', type=int, default=DEFAULT_REFRESH_DAYS, help="Days since last update to trigger refresh")
    args = parser.parse_args()

    countries = [c.strip().upper() for c in args.country.split(',')]
    for country in countries:
        if country in extractor_registry:
            extractor_class = extractor_registry[country]
            try:
                extractor = extractor_class()
                extractor.run(force=args.force, refresh_days=args.refresh_days)
                logger.info(f"Successfully completed extraction for {country}")
            except Exception as e:
                logger.error(f"Failed to extract data for {country}: {e}")
        else:
            logger.error(f"Unknown country: {country}. Available countries: {list(extractor_registry.keys())}")
            sys.exit(1)