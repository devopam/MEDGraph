import logging
import psycopg2
import requests
from requests.exceptions import RequestException
import time
from fuzzywuzzy import fuzz
from datetime import datetime, timedelta
import json
import bs4 as BeautifulSoup
import tempfile
import pdfplumber
import re
from config import DB_PARAMS, InstitutionType

logger = logging.getLogger(__name__)

class BaseExtractor:
    def __init__(self, country):
        self.country = country.upper()  # Ensure ISO3 uppercase
        self.conn = psycopg2.connect(**DB_PARAMS)
        self.cur = self.conn.cursor()

    def fetch_data(self):
        raise NotImplementedError("Subclasses must implement fetch_data")

    def normalize(self, data):
        return data  # Subclasses can override for custom cleaning

    def deduplicate(self):
        self.cur.execute("SELECT id, name, address FROM institutions WHERE country = %s", (self.country,))
        existing = self.cur.fetchall()
        existing_dict = {row[0]: {'name': row[1], 'address': row[2] or ''} for row in existing}
        duplicates = []
        for id1, info1 in existing_dict.items():
            for id2, info2 in existing_dict.items():
                if id1 < id2 and fuzz.token_sort_ratio(info1['name'] + info1['address'], info2['name'] + info2['address']) > 90:
                    duplicates.append(id2)  # Keep lower ID
        if duplicates:
            logger.info(f"Found {len(set(duplicates))} duplicates in {self.country}. Removing...")
            self.cur.execute("DELETE FROM institutions WHERE id = ANY(%s)", (list(set(duplicates)),))
            self.conn.commit()

    def insert_to_db(self, data):
        inserted_count = 0
        for item in data:
            try:
                additional = json.dumps(item.get('additional_attributes', {}))
                self.cur.execute("""
                    INSERT INTO institutions (name, type, country, state, city, address, website, latitude, longitude, additional_attributes, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    item.get('name'), 
                    item.get('type').value if hasattr(item.get('type'), 'value') else item.get('type'), 
                    self.country, 
                    item.get('state'), 
                    item.get('city'),
                    item.get('address'), 
                    item.get('website'), 
                    item.get('latitude'), 
                    item.get('longitude'),
                    additional, 
                    datetime.now()
                ))
                inserted_count += 1
            except Exception as e:
                logger.error(f"Error inserting record {item.get('name', 'Unknown')}: {e}")
                continue
        
        self.conn.commit()
        logger.info(f"Inserted/updated {inserted_count} records for {self.country}")

    def needs_refresh(self, days=30):
        self.cur.execute("SELECT MAX(last_updated) FROM institutions WHERE country = %s", (self.country,))
        last = self.cur.fetchone()[0]
        return last is None or (datetime.now() - last) > timedelta(days=days)

    def run(self, force=False, refresh_days=30):
        try:
            if force or self.needs_refresh(days=refresh_days):
                logger.info(f"Starting data extraction for {self.country}")
                data = self.fetch_data()
                logger.info(f"Fetched {len(data)} raw records for {self.country}")
                
                normalized = self.normalize(data)
                logger.info(f"Normalized to {len(normalized)} records for {self.country}")
                
                self.insert_to_db(normalized)
                self.deduplicate()
                logger.info(f"Completed data extraction for {self.country}")
            else:
                logger.info(f"No refresh needed for {self.country}")
        except Exception as e:
            logger.error(f"Error during extraction for {self.country}: {e}")
            raise
        finally:
            self.close()

    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def get_with_retry(self, url, retries=3, backoff_factor=1, timeout=30):
        """Enhanced retry mechanism with better error handling"""
        backoff = backoff_factor
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        for attempt in range(retries):
            try:
                response = requests.get(url, timeout=timeout, headers=headers)
                response.raise_for_status()
                return response
            except RequestException as e:
                logger.warning(f"Error fetching {url}: {e}. Retry {attempt+1}/{retries} in {backoff} seconds.")
                if attempt < retries - 1:  # Don't sleep on the last attempt
                    time.sleep(backoff)
                    backoff *= 2  # Exponential backoff
        
        logger.error(f"Failed to fetch {url} after {retries} retries.")
        return None

    def fetch_paginated_scrape(self, base_url, page_param='page', start_page=1, max_pages=10, parser=None):
        data = []
        for page in range(start_page, start_page + max_pages):
            url = f"{base_url}&{page_param}={page}" if '?' in base_url else f"{base_url}?{page_param}={page}"
            response = self.get_with_retry(url)
            if not response:
                break
            
            try:
                soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
                page_data = parser(soup) if parser else []
                if not page_data:
                    logger.info(f"No data found on page {page}, stopping pagination")
                    break
                data += page_data
                logger.info(f"Fetched {len(page_data)} records from page {page}")
                time.sleep(1)  # Rate limit sleep
            except Exception as e:
                logger.error(f"Error parsing page {page}: {e}")
                break
        
        return data

    def fetch_avma_vet(self, country_filter):
        """Enhanced AVMA veterinary school fetcher"""
        try:
            page_url = "https://www.avma.org/education/center-for-veterinary-accreditation/accredited-veterinary-colleges"
            response = self.get_with_retry(page_url)
            if not response:
                return []
            
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            
            # Look for PDF download link
            pdf_link = None
            for link in soup.find_all('a', href=True):
                if 'pdf' in link.get('href', '').lower() and 'list' in link.text.lower():
                    pdf_link = link['href']
                    break
            
            if not pdf_link:
                logger.warning("Could not find PDF link on AVMA page")
                return []
            
            pdf_url = f"https://www.avma.org{pdf_link}" if pdf_link.startswith('/') else pdf_link
            pdf_response = self.get_with_retry(pdf_url)
            if not pdf_response:
                return []
            
            with tempfile.NamedTemporaryFile(suffix='.pdf') as tmp:
                tmp.write(pdf_response.content)
                with pdfplumber.open(tmp.name) as pdf:
                    data = []
                    for page in pdf.pages:
                        tables = page.extract_tables()
                        for table in tables:
                            if not table:
                                continue
                            for row in table[1:]:  # Skip header
                                if not row or len(row) < 2:
                                    continue
                                
                                if country_filter in str(row[0]):
                                    name = row[1] if len(row) > 1 else row[0]
                                    address = row[2] if len(row) > 2 else ''
                                    
                                    # Parse city and state from address
                                    city_state = address.split(',')[-2:] if address else ['', '']
                                    city = city_state[0].strip() if len(city_state) > 1 else None
                                    state = city_state[1].strip() if len(city_state) > 1 else None
                                    
                                    data.append({
                                        'name': name,
                                        'city': city,
                                        'state': state,
                                        'address': address,
                                        'type': InstitutionType.VETERINARY_SCHOOL,
                                        'additional_attributes': {
                                            'accreditation': row[3] if len(row) > 3 else 'AVMA',
                                            'source': 'AVMA'
                                        }
                                    })
            
            logger.info(f"Fetched {len(data)} vet schools from AVMA for {country_filter}")
            return data
            
        except Exception as e:
            logger.error(f"Error fetching AVMA: {e}")
            return []