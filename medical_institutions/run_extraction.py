#!/usr/bin/env python3
"""
Optimized Medical Institutions Data Extraction Script
Supports USA, India, China with comprehensive monitoring and error recovery
"""

import logging
import sys
import time
from datetime import datetime
import argparse
from pathlib import Path
import json

from config import DB_PARAMS, COUNTRY_SETTINGS, LOG_DIR, LOG_FORMAT
from extractors import extractor_registry
from extraction_monitor import ExtractionMonitor

def setup_logging(country, log_level=logging.INFO):
    """Setup country-specific logging"""
    log_file = LOG_DIR / f"extraction_{country}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=log_level,
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

def run_extraction_with_monitoring(country, force=False, refresh_days=30):
    """Run extraction for a country with comprehensive monitoring"""
    logger = logging.getLogger(__name__)
    
    start_time = time.time()
    logger.info(f"🚀 Starting extraction for {country}")
    
    # Pre-extraction report
    monitor = ExtractionMonitor()
    
    try:
        # Get initial count
        monitor.cur.execute("SELECT COUNT(*) FROM institutions WHERE country = %s", (country,))
        initial_count = monitor.cur.fetchone()[0]
        logger.info(f"📊 Initial count for {country}: {initial_count:,} institutions")
        
        # Run extraction
        if country in extractor_registry:
            extractor_class = extractor_registry[country]
            extractor = extractor_class()
            
            # Apply country-specific settings
            settings = COUNTRY_SETTINGS.get(country, {})
            logger.info(f"⚙️  Using settings for {country}: {settings}")
            
            extractor.run(force=force, refresh_days=refresh_days)
            
            # Post-extraction report
            monitor.cur.execute("SELECT COUNT(*) FROM institutions WHERE country = %s", (country,))
            final_count = monitor.cur.fetchone()[0]
            
            new_records = final_count - initial_count
            duration = time.time() - start_time
            
            logger.info(f"✅ Extraction completed for {country}")
            logger.info(f"📈 Records added: {new_records:,}")
            logger.info(f"📊 Total records: {final_count:,}")
            logger.info(f"⏱️  Duration: {duration:.2f} seconds")
            
            # Generate mini-report
            monitor.generate_extraction_report([country])
            
            return {
                'country': country,
                'initial_count': initial_count,
                'final_count': final_count,
                'new_records': new_records,
                'duration': duration,
                'success': True
            }
            
        else:
            logger.error(f"❌ Unknown country: {country}")
            logger.info(f"Available countries: {list(extractor_registry.keys())}")
            return {
                'country': country,
                'success': False,
                'error': 'Unknown country'
            }
            
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"💥 Extraction failed for {country}: {e}")
        return {
            'country': country,
            'success': False,
            'error': str(e),
            'duration': duration
        }
    finally:
        monitor.close()

def run_batch_extraction(countries, force=False, refresh_days=30, max_parallel=1):
    """Run extraction for multiple countries with summary reporting"""
    logger = logging.getLogger(__name__)
    
    results = []
    total_start_time = time.time()
    
    logger.info(f"🌍 Starting batch extraction for countries: {', '.join(countries)}")
    
    for i, country in enumerate(countries, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {i}/{len(countries)}: {country}")
        logger.info(f"{'='*60}")
        
        result = run_extraction_with_monitoring(country, force, refresh_days)
        results.append(result)
        
        # Brief pause between countries to be respectful to servers
        if i < len(countries):
            logger.info("⏸️  Pausing between countries...")
            time.sleep(2)
    
    # Final summary
    total_duration = time.time() - total_start_time
    successful = [r for r in results if r.get('success')]
    failed = [r for r in results if not r.get('success')]
    
    logger.info(f"\n{'='*80}")
    logger.info("📊 BATCH EXTRACTION SUMMARY")
    logger.info(f"{'='*80}")
    logger.info(f"Countries processed: {len(countries)}")
    logger.info(f"Successful: {len(successful)}")
    logger.info(f"Failed: {len(failed)}")
    logger.info(f"Total duration: {total_duration/60:.1f} minutes")
    
    if successful:
        total_new_records = sum(r.get('new_records', 0) for r in successful)
        logger.info(f"Total new records: {total_new_records:,}")
        
        logger.info("\n✅ Successful extractions:")
        for result in successful:
            logger.info(f"  {result['country']}: +{result.get('new_records', 0):,} records "
                       f"({result.get('duration', 0):.1f}s)")
    
    if failed:
        logger.info("\n❌ Failed extractions:")
        for result in failed:
            logger.info(f"  {result['country']}: {result.get('error', 'Unknown error')}")
    
    # Generate comprehensive report for all countries
    monitor = ExtractionMonitor()
    try:
        monitor.generate_extraction_report(countries)
        
        # Export summary
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        export_file = f"extraction_summary_{timestamp}.csv"
        monitor.export_summary_csv(export_file, countries)
        
    finally:
        monitor.close()
    
    return results

def main():
    parser = argparse.ArgumentParser(
        description="Extract medical institutions data for USA, India, China",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_extraction.py --countries USA,IND,CHN
  python run_extraction.py --countries USA --force
  python run_extraction.py --countries IND --refresh-days 7 --verbose
  python run_extraction.py --report-only --countries USA,IND,CHN
        """
    )
    
    parser.add_argument('--countries', type=str, 
                       default='USA,IND,CHN',
                       help="Comma-separated country codes (default: USA,IND,CHN)")
    parser.add_argument('--force', action='store_true', 
                       help="Force refresh regardless of last_updated")
    parser.add_argument('--refresh-days', type=int, default=30,
                       help="Days since last update to trigger refresh (default: 30)")
    parser.add_argument('--verbose', action='store_true',
                       help="Enable verbose logging")
    parser.add_argument('--report-only', action='store_true',
                       help="Generate report without running extraction")
    parser.add_argument('--validate-coords', action='store_true',
                       help="Validate geographic coordinates")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger = setup_logging('batch', log_level)
    
    countries = [c.strip().upper() for c in args.countries.split(',')]
    
    # Validate countries
    valid_countries = []
    for country in countries:
        if country in extractor_registry:
            valid_countries.append(country)
        else:
            logger.error(f"❌ Unknown country: {country}")
            logger.info(f"Available countries: {list(extractor_registry.keys())}")
    
    if not valid_countries:
        logger.error("No valid countries specified. Exiting.")
        sys.exit(1)
    
    countries = valid_countries
    
    if args.report_only:
        # Generate report only
        logger.info("📊 Generating report without extraction...")
        monitor = ExtractionMonitor()
        try:
            monitor.generate_extraction_report(countries)
            if args.validate_coords:
                monitor.validate_coordinates(countries)
        finally:
            monitor.close()
    else:
        # Run extraction
        logger.info(f"🎯 Target countries: {', '.join(countries)}")
        logger.info(f"💪 Force refresh: {args.force}")
        logger.info(f"📅 Refresh threshold: {args.refresh_days} days")
        
        results = run_batch_extraction(
            countries, 
            force=args.force, 
            refresh_days=args.refresh_days
        )
        
        # Exit with error code if any extraction failed
        if any(not r.get('success') for r in results):
            sys.exit(1)
        
        logger.info("🎉 All extractions completed successfully!")

if __name__ == "__main__":
    main()