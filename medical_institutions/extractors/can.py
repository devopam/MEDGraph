import logging
import requests
import pandas as pd
import bs4 as BeautifulSoup
import pdfplumber
import tempfile
import re
from ..config import InstitutionType
from .base import BaseExtractor

logger = logging.getLogger(__name__)

class CANExtractor(BaseExtractor):
    def __init__(self):
        super().__init__('CAN')

    def fetch_data(self):
        all_data = []
        all_data += self.fetch_vet_cvma() or []
        all_data += self.fetch_avma_vet('Canada') or []
        all_data += self.fetch_med_cacms() or []
        all_data += self.fetch_med_wiki() or []
        all_data += self.fetch_health_odhf() or []
        all_data += self.fetch_research_hospitals() or []
        logger.info(f"Fetched {len(all_data)} raw records from all sources for Canada")
        return all_data

    def fetch_vet_cvma(self):
        try:
            url = "https://www.canadianveterinarians.net/public-resources/careers-in-veterinary-medicine/veterinary-colleges/"
            response = self.get_with_retry(url)
            if not response:
                return []
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            data = []
            colleges_section = soup.find('div', class_='entry-content')
            for li in colleges_section.find_all('li'):
                name = li.text.strip()
                if 'College' in name:
                    data.append({
                        'name': name,
                        'type': InstitutionType.VETERINARY_SCHOOL,
                        'additional_attributes': {'source': 'CVMA'}
                    })
            logger.info(f"Fetched {len(data)} vet schools from CVMA")
            return data
        except Exception as e:
            logger.error(f"Error fetching CVMA: {e}")
            return []

    def fetch_med_cacms(self):
        try:
            url = "https://cacms-cafmc.ca/about-cacms/accredited-programs/"
            response = self.get_with_retry(url)
            if not response:
                return []
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            data = []
            for item in soup.find_all('div', class_='program-item'):
                name = item.find('h3').text.strip()
                location = item.find('p').text.strip()
                province = location.split(',')[-1].strip() if ',' in location else None
                data.append({
                    'name': name,
                    'state': province,
                    'type': InstitutionType.MEDICAL_SCHOOL,
                    'additional_attributes': {'source': 'CACMS'}
                })
            logger.info(f"Fetched {len(data)} med schools from CACMS")
            return data
        except Exception as e:
            logger.error(f"Error fetching CACMS: {e}")
            return []

    def fetch_med_wiki(self):
        try:
            url = "https://en.wikipedia.org/wiki/List_of_medical_schools_in_Canada"
            response = self.get_with_retry(url)
            if not response:
                return []
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            table = soup.find('table', class_='wikitable')
            data = []
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) >= 4:
                    province = cols[0].text.strip()
                    school = cols[1].text.strip()
                    city = cols[2].text.strip()
                    data.append({
                        'name': school,
                        'state': province,
                        'city': city,
                        'type': InstitutionType.MEDICAL_SCHOOL,
                        'additional_attributes': {'est_year': cols[3].text.strip(), 'source': 'Wikipedia'}
                    })
            logger.info(f"Fetched {len(data)} med schools from Wikipedia")
            return data
        except Exception as e:
            logger.error(f"Error fetching Wikipedia med schools: {e}")
            return []

    def fetch_health_odhf(self):
        try:
            csv_url = "https://ftp.maps.canada.ca/pub/statcan_statcan/Health-care-facilities_Etablissement-de-sante/ODHF_BDOES/odhf_bdoes_v1.csv"
            response = self.get_with_retry(csv_url)
            if not response:
                return []
            df = pd.read_csv(StringIO(response.text))
            data = []
            for _, row in df.iterrows():
                inst_type = InstitutionType.HOSPITAL if 'hospital' in row.get('odhf_facility_type', '').lower() else \
                            InstitutionType.CLINIC if 'clinic' in row.get('odhf_facility_type', '').lower() else \
                            InstitutionType.OTHER
                data.append({
                    'name': row.get('facility_name'),
                    'type': inst_type,
                    'state': row.get('province'),
                    'city': row.get('city'),
                    'address': row.get('street_no') + ' ' + row.get('street_name') if 'street_no' in row else row.get('address'),
                    'latitude': row.get('latitude'),
                    'longitude': row.get('longitude'),
                    'additional_attributes': {'odhf_type': row.get('odhf_facility_type'), 'source': 'ODHF'}
                })
            logger.info(f"Fetched {len(data)} facilities from ODHF")
            return data
        except Exception as e:
            logger.error(f"Error fetching ODHF: {e}")
            return []

    def fetch_research_hospitals(self):
        try:
            url = "https://researchinfosource.com/top-40-research-hospitals/2020/list"
            response = self.get_with_retry(url)
            if not response:
                return []
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            table = soup.find('table')
            data = []
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) >= 4:
                    rank = cols[0].text.strip()
                    name = cols[1].text.strip()
                    city = cols[2].text.strip()
                    province = cols[3].text.strip()
                    data.append({
                        'name': name,
                        'city': city,
                        'state': province,
                        'type': InstitutionType.ACADEMIC_MEDICAL_CENTER,
                        'additional_attributes': {'rank': rank, 'source': 'Research Infosource'}
                    })
            logger.info(f"Fetched {len(data)} research hospitals")
            return data
        except Exception as e:
            logger.error(f"Error fetching research hospitals: {e}")
            return []

    def normalize(self, data):
        return data