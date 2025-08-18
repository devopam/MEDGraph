import psycopg2
import pandas as pd
import json
from datetime import datetime, timedelta
import logging
from config import DB_PARAMS
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExtractionMonitor:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_PARAMS)
        self.cur = self.conn.cursor()
    
    def generate_extraction_report(self, countries=None):
        """Generate comprehensive extraction report"""
        where_clause = ""
        params = []
        
        if countries:
            where_clause = "WHERE country = ANY(%s)"
            params.append(countries)
        
        # Basic statistics
        self.cur.execute(f"""
            SELECT 
                country,
                type,
                COUNT(*) as count,
                COUNT(CASE WHEN website IS NOT NULL THEN 1 END) as with_website,
                COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as with_coordinates,
                MAX(last_updated) as last_updated
            FROM institutions 
            {where_clause}
            GROUP BY country, type
            ORDER BY country, type
        """, params)
        
        results = self.cur.fetchall()
        
        print("\n" + "="*80)
        print("MEDICAL INSTITUTIONS EXTRACTION REPORT")
        print("="*80)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Group by country
        country_data = defaultdict(list)
        for row in results:
            country_data[row[0]].append(row)
        
        total_institutions = 0
        
        for country, data in country_data.items():
            print(f"\n📍 {country}")
            print("-" * 50)
            
            country_total = sum(row[2] for row in data)
            total_institutions += country_total
            
            for row in data:
                _, inst_type, count, with_website, with_coords, last_updated = row
                website_pct = (with_website / count * 100) if count > 0 else 0
                coords_pct = (with_coords / count * 100) if count > 0 else 0
                
                print(f"  {inst_type.replace('_', ' ').title():25} | "
                      f"{count:5d} | "
                      f"Websites: {website_pct:5.1f}% | "
                      f"Coords: {coords_pct:5.1f}% | "
                      f"Updated: {last_updated.strftime('%Y-%m-%d') if last_updated else 'Never'}")
            
            print(f"  {'TOTAL':25} | {country_total:5d}")
        
        print(f"\n🌍 GRAND TOTAL: {total_institutions:,} institutions")
        
        # Data quality metrics
        self.generate_quality_report(countries)
        
        # Source analysis
        self.generate_source_analysis(countries)
    
    def generate_quality_report(self, countries=None):
        """Generate data quality metrics"""
        where_clause = ""
        params = []
        
        if countries:
            where_clause = "WHERE country = ANY(%s)"
            params.append(countries)
        
        print("\n" + "="*80)
        print("DATA QUALITY ANALYSIS")
        print("="*80)
        
        # Completeness analysis
        self.cur.execute(f"""
            SELECT 
                country,
                COUNT(*) as total,
                COUNT(name) as has_name,
                COUNT(state) as has_state,
                COUNT(city) as has_city,
                COUNT(address) as has_address,
                COUNT(website) as has_website,
                COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as has_coordinates
            FROM institutions 
            {where_clause}
            GROUP BY country
            ORDER BY country
        """, params)
        
        quality_data = self.cur.fetchall()
        
        print("\nData Completeness by Country:")
        print("-" * 80)
        print(f"{'Country':<8} | {'Total':<6} | {'Name':<6} | {'State':<6} | {'City':<6} | {'Address':<8} | {'Website':<8} | {'Coords':<6}")
        print("-" * 80)
        
        for row in quality_data:
            country, total, name, state, city, address, website, coords = row
            print(f"{country:<8} | {total:<6} | "
                  f"{(name/total*100):5.1f}% | "
                  f"{(state/total*100):5.1f}% | "
                  f"{(city/total*100):5.1f}% | "
                  f"{(address/total*100):6.1f}% | "
                  f"{(website/total*100):6.1f}% | "
                  f"{(coords/total*100):5.1f}%")
        
        # Potential duplicates
        self.cur.execute(f"""
            SELECT country, COUNT(*) as potential_duplicates
            FROM (
                SELECT country, name, COUNT(*) as name_count
                FROM institutions 
                {where_clause}
                GROUP BY country, name
                HAVING COUNT(*) > 1
            ) duplicates
            GROUP BY country
        """, params)
        
        dup_data = self.cur.fetchall()
        
        if dup_data:
            print("\nPotential Duplicates (same name):")
            print("-" * 30)
            for country, dup_count in dup_data:
                print(f"{country}: {dup_count} potential duplicates")
    
    def generate_source_analysis(self, countries=None):
        """Analyze data sources and their contributions"""
        where_clauses = ["additional_attributes->>'source' IS NOT NULL"]
        params = []
        
        if countries:
            where_clauses.append("country = ANY(%s)")
            params.append(countries)
        
        where_clause = "WHERE " + " AND ".join(where_clauses)
        
        print("\n" + "="*80)
        print("DATA SOURCE ANALYSIS")
        print("="*80)
        
        self.cur.execute(f"""
            SELECT 
                country,
                additional_attributes->>'source' as source,
                type,
                COUNT(*) as count
            FROM institutions 
            {where_clause}
            GROUP BY country, additional_attributes->>'source', type
            ORDER BY country, count DESC
        """, params)
        
        source_data = self.cur.fetchall()
        
        # Group by country
        country_sources = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        
        for country, source, inst_type, count in source_data:
            country_sources[country][source][inst_type] += count
        
        for country, sources in country_sources.items():
            print(f"\n📊 {country} - Sources:")
            print("-" * 40)
            
            for source, types in sources.items():
                total_from_source = sum(types.values())
                print(f"  {source:<20} | {total_from_source:5d} total")
                
                for inst_type, count in types.items():
                    print(f"    ↳ {inst_type.replace('_', ' '):<15} | {count:4d}")
    
    def validate_coordinates(self, countries=None):
        """Validate geographic coordinates"""
        where_clauses = ["latitude IS NOT NULL", "longitude IS NOT NULL"]
        params = []
        
        if countries:
            where_clauses.append("country = ANY(%s)")
            params.append(countries)
        
        where_clause = "WHERE " + " AND ".join(where_clauses)
        
        print("\n" + "="*80)
        print("COORDINATE VALIDATION")
        print("="*80)
        
        # Check for invalid coordinates
        self.cur.execute(f"""
            SELECT 
                country,
                COUNT(*) as total_with_coords,
                COUNT(CASE WHEN latitude < -90 OR latitude > 90 THEN 1 END) as invalid_lat,
                COUNT(CASE WHEN longitude < -180 OR longitude > 180 THEN 1 END) as invalid_lng,
                COUNT(CASE WHEN latitude = 0 AND longitude = 0 THEN 1 END) as null_island
            FROM institutions 
            {where_clause}
            GROUP BY country
        """, params)
        
        coord_data = self.cur.fetchall()
        
        if coord_data:
            print("Coordinate Quality:")
            print("-" * 50)
            print(f"{'Country':<8} | {'Total':<6} | {'Invalid Lat':<12} | {'Invalid Lng':<12} | {'(0,0)':<6}")
            print("-" * 50)
            
            for country, total, invalid_lat, invalid_lng, null_island in coord_data:
                print(f"{country:<8} | {total:<6} | {invalid_lat:<12} | {invalid_lng:<12} | {null_island:<6}")
    
    def export_summary_csv(self, filename="extraction_summary.csv", countries=None):
        """Export summary data to CSV"""
        where_clause = ""
        params = []
        
        if countries:
            where_clause = "WHERE country = ANY(%s)"
            params.append(countries)
        
        self.cur.execute(f"""
            SELECT 
                country,
                type,
                state,
                city,
                COUNT(*) as count,
                MAX(last_updated) as last_updated,
                STRING_AGG(DISTINCT additional_attributes->>'source', ', ') as sources
            FROM institutions 
            {where_clause}
            GROUP BY country, type, state, city
            ORDER BY country, type, state, city
        """, params)
        
        df = pd.DataFrame(self.cur.fetchall(), 
                         columns=['country', 'type', 'state', 'city', 'count', 'last_updated', 'sources'])
        
        df.to_csv(filename, index=False)
        print(f"\n📄 Summary exported to: {filename}")
    
    def close(self):
        self.cur.close()
        self.conn.close()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor and analyze medical institutions extraction")
    parser.add_argument('--countries', type=str, help="Comma-separated country codes (e.g., USA,IND,CHN)")
    parser.add_argument('--export', type=str, help="Export summary to CSV file")
    parser.add_argument('--validate-coords', action='store_true', help="Validate geographic coordinates")
    
    args = parser.parse_args()
    
    countries = [c.strip().upper() for c in args.countries.split(',')] if args.countries else None
    
    monitor = ExtractionMonitor()
    
    try:
        monitor.generate_extraction_report(countries)
        
        if args.validate_coords:
            monitor.validate_coordinates(countries)
        
        if args.export:
            monitor.export_summary_csv(args.export, countries)
            
    finally:
        monitor.close()

if __name__ == "__main__":
    main()