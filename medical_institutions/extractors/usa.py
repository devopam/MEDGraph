import logging
import requests
import pandas as pd
from io import StringIO
import bs4 as BeautifulSoup
import pdfplumber
import tempfile
import re
from ..config import InstitutionType
from .base import BaseExtractor

logger = logging.getLogger(__name__)

class USAExtractor(BaseExtractor):
    def __init__(self):
        super().__init__('USA')

    def fetch_data(self):
        all_data = []
        all_data += self.fetch_vet_nifa() or []
        all_data += self.fetch_avma_vet('United States') or []
        all_data += self.fetch_md_lcme() or []
        all_data += self.fetch_do_aacom() or []
        all_data += self.fetch_hospitals_cms() or []
        all_data += self.fetch_teaching_cms() or []
        all_data += self.fetch_clinics_hrsa() or []
        return all_data

    def fetch_vet_nifa(self):
        try:
            url = "https://www.nifa.usda.gov/grants/programs/veterinary-medicine-loan-repayment-program/us-avma-accredited-veterinary-schools"
            response = self.get_with_retry(url)
            if not response:
                return []
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            table = soup.find('table')
            data = []
            current_state = None
            for row in table.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) == 1 and cols[0].find('strong'):
                    current_state = cols[0].text.strip()
                elif len(cols) > 0 and current_state:
                    name = cols[0].text.strip()
                    data.append({
                        'name': name,
                        'state': current_state,
                        'type': InstitutionType.VETERINARY_SCHOOL,
                        'additional_attributes': {'source': 'NIFA'}
                    })
            logger.info(f"Fetched {len(data)} vet schools from NIFA")
            return data
        except Exception as e:
            logger.error(f"Error fetching NIFA: {e}")
            return []

    def fetch_md_lcme(self):
        try:
            url = "https://lcme.org/directory/accredited-u-s-programs/"
            response = self.get_with_retry(url)
            if not response:
                return []
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            table = soup.find('table')
            data = []
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) >= 4:
                    name = cols[0].text.strip()
                    location = cols[1].text.strip()
                    city, state = location.rsplit(',', 1) if ',' in location else (location, None)
                    sponsorship = cols[2].text.strip()
                    status = cols[3].text.strip()
                    data.append({
                        'name': name,
                        'city': city.strip(),
                        'state': state.strip() if state else None,
                        'type': InstitutionType.MEDICAL_SCHOOL,
                        'additional_attributes': {'sponsorship': sponsorship, 'status': status, 'degree': 'MD', 'source': 'LCME'}
                    })
            logger.info(f"Fetched {len(data)} MD schools from LCME")
            return data
        except Exception as e:
            logger.error(f"Error fetching LCME: {e}")
            return []

    def fetch_do_aacom(self):
        try:
            url = "https://www.aacom.org/docs/default-source/become-doctor/us-com-directory.pdf"
            response = self.get_with_retry(url)
            if not response:
                return []
            with tempfile.NamedTemporaryFile(suffix='.pdf') as tmp:
                tmp.write(response.content)
                with pdfplumber.open(tmp.name) as pdf:
                    data = []
                    text = ''
                    for page in pdf.pages:
                        text += page.extract_text() + '\n'
                    lines = text.split('\n')
                    for line in lines:
                        if re.match(r'^[A-Z].+ [A-Z]{2} https?://', line):
                            parts = re.split(r' (\w{2}) (https?://\S+)', line)
                            if len(parts) >= 4:
                                name = parts[0]
                                state = parts[1]
                                website = parts[2]
                                city = name.split(',')[-1].strip() if ',' in name else None
                                data.append({
                                    'name': name,
                                    'city': city,
                                    'state': state,
                                    'website': website,
                                    'type': InstitutionType.MEDICAL_SCHOOL,
                                    'additional_attributes': {'degree': 'DO', 'source': 'AACOM'}
                                })
            logger.info(f"Fetched {len(data)} DO schools from AACOM PDF")
            return data
        except Exception as e:
            logger.error(f"Error fetching AACOM: {e}")
            return []

    def fetch_hospitals_cms(self):
        try:
            api_url = "https://data.cms.gov/provider-data/api/1/datastore/sql?query=[SELECT * FROM xubh-q36u LIMIT 10000]&show_db_columns"
            response = self.get_with_retry(api_url)
            if not response:
                return []
            df = pd.read_json(StringIO(response.text))
            data = []
            for _, row in df.iterrows():
                data.append({
                    'name': row.get('facility_name'),
                    'type': InstitutionType.HOSPITAL,
                    'state': row.get('state'),
                    'city': row.get('city'),
                    'address': row.get('address'),
                    'latitude': row.get('location').get('latitude') if 'location' in row else None,
                    'longitude': row.get('location').get('longitude') if 'location' in row else None,
                    'additional_attributes': {
                        'hospital_type': row.get('hospital_type'),
                        'rating': row.get('hospital_overall_rating'),
                        'source': 'CMS'
                    }
                })
            logger.info(f"Fetched {len(data)} hospitals from CMS")
            return data
        except Exception as e:
            logger.error(f"Error fetching CMS hospitals: {e}")
            return []

    def fetch_teaching_cms(self):
        try:
            url = "https://www.cms.gov/files/document/2025-reporting-cycle-teaching-hospital-list.xlsx"
            response = self.get_with_retry(url)
            if not response:
                return []
            with tempfile.NamedTemporaryFile(suffix='.xlsx') as tmp:
                tmp.write(response.content)
                df = pd.read_excel(tmp.name)
            data = []
            for _, row in df.iterrows():
                data.append({
                    'name': row.get('Teaching_Hospital_Name') or row.get(0),
                    'type': InstitutionType.ACADEMIC_MEDICAL_CENTER,
                    'address': row.get('Address') or f"{row.get('Address_Line_1')}, {row.get('City')}, {row.get('State')}, {row.get('Zip')}",
                    'city': row.get('City'),
                    'state': row.get('State'),
                    'additional_attributes': {'ccn': row.get('CCN'), 'source': 'CMS Teaching'}
                })
            logger.info(f"Fetched {len(data)} teaching hospitals from CMS")
            return data
        except Exception as e:
            logger.error(f"Error fetching CMS teaching: {e}")
            return []

    def fetch_clinics_hrsa(self):
        try:
            csv_url = "https://data.hrsa.gov/api/views/29i4-dfs4/rows.csv?accessType=DOWNLOAD"
            response = self.get_with_retry(csv_url)
            if not response:
                return []
            df = pd.read_csv(StringIO(response.text))
            data = []
            for _, row in df.iterrows():
                if row.get('Country') == 'US':
                    data.append({
                        'name': row.get('Site_Name'),
                        'type': InstitutionType.CLINIC,
                        'state': row.get('State'),
                        'city': row.get('City'),
                        'address': row.get('Address'),
                        'latitude': row.get('Latitude'),
                        'longitude': row.get('Longitude'),
                        'additional_attributes': {'type': row.get('Site_Type'), 'source': 'HRSA'}
                    })
            logger.info(f"Fetched {len(data)} clinics from HRSA")
            return data
        except Exception as e:
            logger.error(f"Error fetching HRSA clinics: {e}")
            return []

    def normalize(self, data):
        return data