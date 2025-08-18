#!/usr/bin/env python3
"""
Real-time progress monitor for medical institutions extraction
Run this in a separate terminal while extraction is running
"""

import psycopg2
import time
import sys
from datetime import datetime
from collections import defaultdict
from config import DB_PARAMS

class ProgressMonitor:
    def __init__(self, countries=None, refresh_interval=5):
        self.countries = countries or ['USA', 'IND', 'CHN', 'CAN']
        self.refresh_interval = refresh_interval
        self.previous_counts = defaultdict(int)
        self.start_time = datetime.now()
        
    def get_current_counts(self):
        """Get current record counts by country and type"""
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            cur = conn.cursor()
            
            where_clause = "WHERE country = ANY(%s)" if self.countries else ""
            params = [self.countries] if self.countries else []
            
            cur.execute(f"""
                SELECT 
                    country,
                    type,
                    COUNT(*) as count,
                    MAX(last_updated) as last_updated
                FROM institutions 
                {where_clause}
                GROUP BY country, type
                ORDER BY country, type
            """, params)
            
            results = cur.fetchall()
            cur.close()
            conn.close()
            
            return results
            
        except Exception as e:
            print(f"Error getting counts: {e}")
            return []
    
    def display_progress(self):
        """Display current progress with changes"""
        results = self.get_current_counts()
        
        # Clear screen and move cursor to top
        print('\033[2J\033[H')
        
        print("🔄 MEDICAL INSTITUTIONS EXTRACTION - LIVE MONITOR")
        print("=" * 70)
        print(f"Monitoring: {', '.join(self.countries)}")
        print(f"Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Current: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Refresh: Every {self.refresh_interval} seconds")
        print()
        
        if not results:
            print("⏳ No data found yet... Extraction may still be starting.")
            return
        
        # Group by country
        country_data = defaultdict(list)
        total_current = 0
        
        for country, inst_type, count, last_updated in results:
            country_data[country].append((inst_type, count, last_updated))
            total_current += count
        
        total_previous = sum(self.previous_counts.values())
        total_change = total_current - total_previous
        
        print(f"📊 SUMMARY: {total_current:,} total records (+{total_change:,} since last refresh)")
        print()
        
        # Display by country
        for country in sorted(country_data.keys()):
            data = country_data[country]
            country_total = sum(count for _, count, _ in data)
            country_previous = sum(self.previous_counts.get(f"{country}_{inst_type}", 0) 
                                 for inst_type, _, _ in data)
            country_change = country_total - country_previous
            
            print(f"🌍 {country}: {country_total:,} total (+{country_change:,})")
            print("-" * 50)
            
            for inst_type, count, last_updated in data:
                key = f"{country}_{inst_type}"
                previous = self.previous_counts.get(key, 0)
                change = count - previous
                
                # Update tracking
                self.previous_counts[key] = count
                
                # Format change indicator
                change_str = f"(+{change})" if change > 0 else f"({change})" if change < 0 else ""
                change_color = "🟢" if change > 0 else "🔴" if change < 0 else "⚪"
                
                # Format last updated
                time_str = last_updated.strftime('%H:%M:%S') if last_updated else 'Never'
                
                print(f"  {change_color} {inst_type.replace('_', ' ').title():25} | "
                      f"{count:5,} {change_str:>8} | "
                      f"Updated: {time_str}")
            
            print()
        
        # Show extraction rate
        elapsed = (datetime.now() - self.start_time).total_seconds()
        if elapsed > 0:
            rate = total_current / elapsed * 60  # records per minute
            print(f"⚡ Extraction rate: {rate:.1f} records/minute")
        
        print()
        print("Press Ctrl+C to stop monitoring")
    
    def run(self):
        """Run the monitoring loop"""
        print("🚀 Starting extraction monitor...")
        print("This will update every few seconds with live progress.")
        print("Make sure extraction is running in another terminal!")
        print()
        
        try:
            while True:
                self.display_progress()
                time.sleep(self.refresh_interval)
                
        except KeyboardInterrupt:
            print("\n\n👋 Monitoring stopped by user")
        except Exception as e:
            print(f"\n❌ Monitor error: {e}")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor medical institutions extraction progress")
    parser.add_argument('--countries', type=str, default='USA,IND,CHN',
                       help="Comma-separated country codes to monitor")
    parser.add_argument('--interval', type=int, default=5,
                       help="Refresh interval in seconds (default: 5)")
    
    args = parser.parse_args()
    
    countries = [c.strip().upper() for c in args.countries.split(',')]
    
    monitor = ProgressMonitor(countries, args.interval)
    monitor.run()

if __name__ == "__main__":
    main()