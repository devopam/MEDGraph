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
        logger.info(f"Fetched {len(all_data)} raw records from all sources for USA")
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
            
            if table:
                for row in table.find_all('tr'):
                    cols = row.find_all('td')
                    if len(cols) == 1 and cols[0].find('strong'):
                        current_state = cols[0].text.strip()
                    elif len(cols) > 0 and current_state:
                        name = cols[0].text.strip()
                        if name and 'university' in name.lower() or 'college' in name.lower():
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
            
            if table:
                for row in table.find_all('tr')[1:]:
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        name = cols[0].text.strip()
                        location = cols[1].text.strip()
                        sponsorship = cols[2].text.strip()
                        status = cols[3].text.strip()
                        
                        # Parse location
                        city, state = location.rsplit(',', 1) if ',' in location else (location, None)
                        
                        data.append({
                            'name': name,
                            'city': city.strip(),
                            'state': state.strip() if state else None,
                            'type': InstitutionType.MEDICAL_SCHOOL,
                            'additional_attributes': {
                                'sponsorship': sponsorship, 
                                'status': status, 
                                'degree': 'MD', 
                                'source': 'LCME'
                            }
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
                    
                    # Parse the text for school information
                    lines = text.split('\n')
                    for line in lines:
                        # Look for lines that match school pattern
                        if re.match(r'^[A-Z].+ [A-Z]{2} https?://', line):
                            parts = re.split(r' (\w{2}) (https?://\S+)', line)
                            if len(parts) >= 4:
                                name = parts[0].strip()
                                state = parts[1]
                                website = parts[2]
                                
                                # Extract city if present
                                city = None
                                if ',' in name:
                                    name_parts = name.split(',')
                                    if len(name_parts) > 1:
                                        name = name_parts[0].strip()
                                        city = name_parts[-1].strip()
                                
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
            # Using a simpler approach since the API query might be complex
            api_url = "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0"
            response = self.get_with_retry(api_url)
            if not response:
                return []
            
            try:
                json_data = response.json()
                data = []
                
                for record in json_data.get('results', [])[:1000]:  # Limit to first 1000
                    name = record.get('facility_name')
                    if name:
                        data.append({
                            'name': name,
                            'type': InstitutionType.HOSPITAL,
                            'state': record.get('state'),
                            'city': record.get('city'),
                            'address': record.get('address'),
                            'additional_attributes': {
                                'hospital_type': record.get('hospital_type'),
                                'rating': record.get('hospital_overall_rating'),
                                'source': 'CMS'
                            }
                        })
                
                logger.info(f"Fetched {len(data)} hospitals from CMS")
                return data
            except Exception as e:
                logger.error(f"Error parsing CMS JSON: {e}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching CMS hospitals: {e}")
            return []

    def fetch_teaching_cms(self):
        try:
            # This would need to be updated with the actual current URL
            url = "https://www.cms.gov/files/document/2025-reporting-cycle-teaching-hospital-list.xlsx"
            response = self.get_with_retry(url)
            if not response:
                return []
            
            with tempfile.NamedTemporaryFile(suffix='.xlsx') as tmp:
                tmp.write(response.content)
                try:
                    df = pd.read_excel(tmp.name)
                    data = []
                    
                    for _, row in df.iterrows():
                        name = row.get('Teaching_Hospital_Name') or row.get(df.columns[0])
                        if name:
                            address_parts = []
                            if 'Address' in row:
                                address = row.get('Address')
                            else:
                                # Construct address from parts
                                addr_line = row.get('Address_Line_1', '')
                                city = row.get('City', '')
                                state = row.get('State', '')
                                zip_code = row.get('Zip', '')
                                address = f"{addr_line}, {city}, {state}, {zip_code}".strip(', ')
                            
                            data.append({
                                'name': str(name).strip(),
                                'type': InstitutionType.ACADEMIC_MEDICAL_CENTER,
                                'address': address,
                                'city': row.get('City'),
                                'state': row.get('State'),
                                'additional_attributes': {
                                    'ccn': row.get('CCN'),
                                    'source': 'CMS Teaching'
                                }
                            })
                    
                    logger.info(f"Fetched {len(data)} teaching hospitals from CMS")
                    return data
                    
                except Exception as e:
                    logger.error(f"Error reading Excel file: {e}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching CMS teaching: {e}")
            return []

    def fetch_clinics_hrsa(self):
        try:
            csv_url = "https://data.hrsa.gov/api/views/29i4-dfs4/rows.csv?accessType=DOWNLOAD"
            response = self.get_with_retry(csv_url)
            if not response:
                return []
            
            try:
                df = pd.read_csv(StringIO(response.text))
                data = []
                
                for _, row in df.iterrows():
                    # Filter for US locations
                    if row.get('Country') == 'US':
                        site_name = row.get('Site_Name')
                        if site_name:
                            data.append({
                                'name': str(site_name).strip(),
                                'type': InstitutionType.CLINIC,
                                'state': row.get('State'),
                                'city': row.get('City'),
                                'address': row.get('Address'),
                                'latitude': row.get('Latitude') if pd.notna(row.get('Latitude')) else None,
                                'longitude': row.get('Longitude') if pd.notna(row.get('Longitude')) else None,
                                'additional_attributes': {
                                    'type': row.get('Site_Type'),
                                    'source': 'HRSA'
                                }
                            })
                
                logger.info(f"Fetched {len(data)} clinics from HRSA")
                return data
                
            except Exception as e:
                logger.error(f"Error parsing HRSA CSV: {e}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching HRSA clinics: {e}")
            return []

    def normalize(self, data):
        """Normalize US institution data"""
        for item in data:
            # Standardize state names/abbreviations
            if item.get('state'):
                state = item['state'].strip()
                # You could add a state abbreviation to full name mapping here
                item['state'] = state
            
            # Clean institution names
            if item.get('name'):
                name = item['name']
                # Remove common artifacts
                name = re.sub(r'\s+', ' ', name)  # Multiple spaces to single
                name = name.strip()
                item['name'] = name
                
            # Ensure coordinates are numeric
            for coord in ['latitude', 'longitude']:
                if item.get(coord):
                    try:
                        item[coord] = float(item[coord])
                    except (ValueError, TypeError):
                        item[coord] = None
        
        return data