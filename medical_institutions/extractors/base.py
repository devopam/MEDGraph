from loguru import logger
import psycopg2
import requests
from requests.exceptions import RequestException
import time
from fuzzywuzzy import fuzz
from datetime import datetime, timedelta
import json
import bs4 as BeautifulSoup
import smart_open
import logging

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
        for item in data:
            additional = json.dumps(item.get('additional_attributes', {}))
            self.cur.execute("""
                INSERT INTO institutions (name, type, country, state, city, address, website, latitude, longitude, additional_attributes, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                item['name'], item['type'].value, self.country, item.get('state'), item.get('city'),
                item.get('address'), item.get('website'), item.get('latitude'), item.get('longitude'),
                additional, datetime.now()
            ))
        self.conn.commit()
        logger.info(f"Inserted/updated {len(data)} records for {self.country}")

    def needs_refresh(self, days=30):
        self.cur.execute("SELECT MAX(last_updated) FROM institutions WHERE country = %s", (self.country,))
        last = self.cur.fetchone()[0]
        return last is None or (datetime.now() - last) > timedelta(days=days)

    def run(self, force=False, refresh_days=30):
        if force or self.needs_refresh(days=refresh_days):
            data = self.fetch_data()
            normalized = self.normalize(data)
            self.insert_to_db(normalized)
            self.deduplicate()
        else:
            logger.info(f"No refresh needed for {self.country}")
        self.close()

    def close(self):
        self.cur.close()
        self.conn.close()

    def get_with_retry(self, url, retries=3, backoff_factor=1, timeout=10):
        backoff = backoff_factor
        for attempt in range(retries):
            try:
                response = requests.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except RequestException as e:
                logger.error(f"Error fetching {url}: {e}. Retry {attempt+1}/{retries} in {backoff} seconds.")
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
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            page_data = parser(soup) if parser else []
            if not page_data:
                break
            data += page_data
            time.sleep(1)  # Rate limit sleep
        return data

    def fetch_avma_vet(self, country_filter):
        try:
            page_url = "https://www.avma.org/education/center-for-veterinary-accreditation/accredited-veterinary-colleges"
            response = self.get_with_retry(page_url)
            if not response:
                return []
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            pdf_link = soup.find('a', text=re.compile('Download printable list'))['href']
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
                            for row in table[1:]:
                                if country_filter in row[0]:
                                    name = row[1]
                                    address = row[2]
                                    city_state = address.split(',')[-2:] if address else ['', '']
                                    data.append({
                                        'name': name,
                                        'city': city_state[0].strip() if len(city_state) > 1 else None,
                                        'state': city_state[1].strip() if len(city_state) > 1 else None,
                                        'address': address,
                                        'type': InstitutionType.VETERINARY_SCHOOL,
                                        'additional_attributes': {'accreditation': row[3], 'source': 'AVMA'}
                                    })
            logger.info(f"Fetched {len(data)} vet schools from AVMA for {country_filter}")
            return data
        except Exception as e:
            logger.error(f"Error fetching AVMA: {e}")
            return []