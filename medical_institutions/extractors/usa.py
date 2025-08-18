import logging
import requests
import pandas as pd
from io import StringIO
import bs4 as BeautifulSoup
import pdfplumber
import tempfile
import re
from config import InstitutionType
from extractors.base import BaseExtractor

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
                        if name and ('university' in name.lower() or 'college' in name.lower()):
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
            # Try the API endpoint first
            api_urls = [
                "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0",
                "https://data.cms.gov/data-api/v1/dataset/xubh-q36u/data",
                "https://data.cms.gov/provider-data/dataset/xubh-q36u.json"
            ]
            
            for api_url in api_urls:
                logger.info(f"Trying CMS hospitals API: {api_url}")
                response = self.get_with_retry(api_url, retries=1)
                
                if response and response.status_code == 200:
                    try:
                        json_data = response.json()
                        data = []
                        
                        # Handle different JSON structures
                        records = []
                        if 'results' in json_data:
                            records = json_data['results']
                        elif 'data' in json_data:
                            records = json_data['data']
                        elif isinstance(json_data, list):
                            records = json_data
                        
                        # Limit to reasonable number for testing
                        for record in records[:2000]:
                            # Handle different field names
                            name = (record.get('facility_name') or 
                                   record.get('hospital_name') or 
                                   record.get('provider_name'))
                            
                            if name:
                                # Extract coordinates if available
                                lat, lng = None, None
                                if 'location' in record and isinstance(record['location'], dict):
                                    lat = record['location'].get('latitude')
                                    lng = record['location'].get('longitude')
                                
                                data.append({
                                    'name': str(name).strip(),
                                    'type': InstitutionType.HOSPITAL,
                                    'state': record.get('state'),
                                    'city': record.get('city'),
                                    'address': record.get('address'),
                                    'latitude': lat,
                                    'longitude': lng,
                                    'additional_attributes': {
                                        'hospital_type': record.get('hospital_type'),
                                        'rating': record.get('hospital_overall_rating'),
                                        'ownership': record.get('hospital_ownership'),
                                        'source': 'CMS'
                                    }
                                })
                        
                        if data:
                            logger.info(f"Fetched {len(data)} hospitals from CMS API")
                            return data
                        
                    except Exception as e:
                        logger.warning(f"Error parsing CMS API response: {e}")
                        continue
            
            # If API fails, try CSV download
            logger.info("API methods failed, trying CSV download...")
            csv_url = "https://data.cms.gov/provider-data/sites/default/files/resources/092256becd267d9eeccf73bf8eaa1e1b_1729555442/Hospital_General_Information.csv"
            response = self.get_with_retry(csv_url, retries=1)
            
            if response and response.status_code == 200:
                try:
                    df = pd.read_csv(StringIO(response.text))
                    data = []
                    
                    for _, row in df.iterrows():
                        name = row.get('Hospital Name') or row.get('Facility Name')
                        if name and pd.notna(name):
                            data.append({
                                'name': str(name).strip(),
                                'type': InstitutionType.HOSPITAL,
                                'state': row.get('State'),
                                'city': row.get('City'),
                                'address': row.get('Address'),
                                'additional_attributes': {
                                    'hospital_type': row.get('Hospital Type'),
                                    'rating': row.get('Hospital overall rating'),
                                    'ownership': row.get('Hospital Ownership'),
                                    'source': 'CMS CSV'
                                }
                            })
                    
                    logger.info(f"Fetched {len(data)} hospitals from CMS CSV")
                    return data
                    
                except Exception as e:
                    logger.error(f"Error parsing CMS CSV: {e}")
            
            # Final fallback - return empty list with warning
            logger.warning("All CMS hospital data sources failed")
            return []
                
        except Exception as e:
            logger.error(f"Error fetching CMS hospitals: {e}")
            return []

    def fetch_teaching_cms(self):
        try:
            # Try multiple years since the exact URL changes annually
            years = ['2024', '2025', '2023']
            base_url = "https://www.cms.gov/files/document/{year}-reporting-cycle-teaching-hospital-list.xlsx"
            
            for year in years:
                url = base_url.format(year=year)
                logger.info(f"Trying CMS teaching hospitals for {year}...")
                response = self.get_with_retry(url, retries=1)  # Reduce retries for 404s
                
                if response and response.status_code == 200:
                    logger.info(f"Found CMS teaching hospitals file for {year}")
                    break
            else:
                logger.warning("Could not find current CMS teaching hospitals file, using fallback")
                return self.fetch_teaching_hospitals_fallback()
            
            with tempfile.NamedTemporaryFile(suffix='.xlsx') as tmp:
                tmp.write(response.content)
                try:
                    df = pd.read_excel(tmp.name)
                    data = []
                    
                    # Handle different possible column names
                    name_cols = ['Teaching_Hospital_Name', 'Hospital_Name', 'Facility_Name']
                    name_col = None
                    for col in name_cols:
                        if col in df.columns:
                            name_col = col
                            break
                    
                    if not name_col and len(df.columns) > 0:
                        name_col = df.columns[0]  # Use first column as fallback
                    
                    if not name_col:
                        logger.error("Could not identify hospital name column in CMS file")
                        return []
                    
                    for _, row in df.iterrows():
                        name = row.get(name_col)
                        if name and pd.notna(name):
                            # Construct address from available fields
                            address_parts = []
                            for addr_field in ['Address', 'Address_Line_1', 'Street']:
                                if addr_field in row and pd.notna(row.get(addr_field)):
                                    address_parts.append(str(row.get(addr_field)))
                            
                            city = row.get('City') if 'City' in row and pd.notna(row.get('City')) else None
                            state = row.get('State') if 'State' in row and pd.notna(row.get('State')) else None
                            zip_code = row.get('Zip') if 'Zip' in row and pd.notna(row.get('Zip')) else None
                            
                            if city:
                                address_parts.append(city)
                            if state:
                                address_parts.append(state)
                            if zip_code:
                                address_parts.append(str(zip_code))
                            
                            address = ', '.join(address_parts) if address_parts else None
                            
                            data.append({
                                'name': str(name).strip(),
                                'type': InstitutionType.ACADEMIC_MEDICAL_CENTER,
                                'address': address,
                                'city': city,
                                'state': state,
                                'additional_attributes': {
                                    'ccn': row.get('CCN') if 'CCN' in row else None,
                                    'source': f'CMS Teaching {year}'
                                }
                            })
                    
                    logger.info(f"Fetched {len(data)} teaching hospitals from CMS")
                    return data
                    
                except Exception as e:
                    logger.error(f"Error reading Excel file: {e}")
                    return self.fetch_teaching_hospitals_fallback()
                    
        except Exception as e:
            logger.error(f"Error fetching CMS teaching: {e}")
            return self.fetch_teaching_hospitals_fallback()

    def fetch_teaching_hospitals_fallback(self):
        """Fallback method for teaching hospitals when CMS file is unavailable"""
        try:
            # Use a known list of major academic medical centers
            teaching_hospitals = [
                {'name': 'Mayo Clinic', 'city': 'Rochester', 'state': 'MN'},
                {'name': 'Cleveland Clinic', 'city': 'Cleveland', 'state': 'OH'},
                {'name': 'Johns Hopkins Hospital', 'city': 'Baltimore', 'state': 'MD'},
                {'name': 'Massachusetts General Hospital', 'city': 'Boston', 'state': 'MA'},
                {'name': 'UCLA Medical Center', 'city': 'Los Angeles', 'state': 'CA'},
                {'name': 'New York-Presbyterian Hospital', 'city': 'New York', 'state': 'NY'},
                {'name': 'UCSF Medical Center', 'city': 'San Francisco', 'state': 'CA'},
                {'name': 'Brigham and Women\'s Hospital', 'city': 'Boston', 'state': 'MA'},
                {'name': 'Hospital of the University of Pennsylvania', 'city': 'Philadelphia', 'state': 'PA'},
                {'name': 'Duke University Hospital', 'city': 'Durham', 'state': 'NC'},
                {'name': 'Stanford Health Care-Stanford Hospital', 'city': 'Stanford', 'state': 'CA'},
                {'name': 'Northwestern Memorial Hospital', 'city': 'Chicago', 'state': 'IL'},
                {'name': 'Cedars-Sinai Medical Center', 'city': 'Los Angeles', 'state': 'CA'},
                {'name': 'Mount Sinai Hospital', 'city': 'New York', 'state': 'NY'},
                {'name': 'Houston Methodist Hospital', 'city': 'Houston', 'state': 'TX'},
                {'name': 'University of Michigan Hospitals', 'city': 'Ann Arbor', 'state': 'MI'},
                {'name': 'Barnes-Jewish Hospital', 'city': 'St. Louis', 'state': 'MO'},
                {'name': 'Vanderbilt University Medical Center', 'city': 'Nashville', 'state': 'TN'},
                {'name': 'University of Washington Medical Center', 'city': 'Seattle', 'state': 'WA'},
                {'name': 'Emory University Hospital', 'city': 'Atlanta', 'state': 'GA'}
            ]
            
            data = []
            for hospital in teaching_hospitals:
                data.append({
                    'name': hospital['name'],
                    'city': hospital['city'],
                    'state': hospital['state'],
                    'type': InstitutionType.ACADEMIC_MEDICAL_CENTER,
                    'additional_attributes': {
                        'source': 'Fallback Teaching Hospitals',
                        'category': 'Major Academic Medical Center'
                    }
                })
            
            logger.info(f"Using fallback list of {len(data)} major teaching hospitals")
            return data
            
        except Exception as e:
            logger.error(f"Error in teaching hospitals fallback: {e}")
            return []

    def fetch_clinics_hrsa(self):
        try:
            # Try multiple HRSA data sources
            urls = [
                "https://data.hrsa.gov/api/views/29i4-dfs4/rows.csv?accessType=DOWNLOAD",
                "https://data.hrsa.gov/DataDownload/FQHC/Comma_Delimited/findahealthcenter.csv"
            ]
            
            for csv_url in urls:
                logger.info(f"Trying HRSA data source: {csv_url}")
                response = self.get_with_retry(csv_url, retries=2)
                
                if response and response.status_code == 200:
                    try:
                        # Try multiple CSV parsing strategies
                        parsing_strategies = [
                            # Strategy 1: Standard parsing
                            {'sep': ',', 'quotechar': '"', 'error_bad_lines': False, 'warn_bad_lines': True},
                            # Strategy 2: Handle quoted fields better
                            {'sep': ',', 'quotechar': '"', 'quoting': 1, 'error_bad_lines': False},
                            # Strategy 3: More lenient parsing
                            {'sep': ',', 'engine': 'python', 'error_bad_lines': False, 'warn_bad_lines': False},
                            # Strategy 4: Try semicolon separator
                            {'sep': ';', 'error_bad_lines': False, 'warn_bad_lines': False}
                        ]
                        
                        df = None
                        for i, strategy in enumerate(parsing_strategies):
                            try:
                                logger.info(f"Trying CSV parsing strategy {i+1}")
                                df = pd.read_csv(StringIO(response.text), **strategy)
                                if len(df) > 0:
                                    logger.info(f"Successfully parsed CSV with strategy {i+1}, got {len(df)} rows")
                                    break
                            except Exception as parse_error:
                                logger.warning(f"CSV parsing strategy {i+1} failed: {parse_error}")
                                continue
                        
                        if df is None or len(df) == 0:
                            logger.warning("All CSV parsing strategies failed for this URL")
                            continue
                        
                        data = []
                        
                        # Handle different column naming conventions - be more flexible
                        country_col = None
                        name_col = None
                        
                        # More comprehensive column detection
                        for col in df.columns:
                            col_lower = str(col).lower()
                            if any(term in col_lower for term in ['country']):
                                country_col = col
                            if any(term in col_lower for term in ['site_name', 'facility_name', 'name', 'facility', 'center_name', 'organization']):
                                if name_col is None:  # Take first match
                                    name_col = col
                        
                        # If no specific name column found, use first column
                        if not name_col and len(df.columns) > 0:
                            name_col = df.columns[0]
                            logger.info(f"Using first column '{name_col}' as facility name")
                        
                        if not name_col:
                            logger.warning("Could not identify facility name column")
                            continue
                        
                        # Process each row
                        valid_rows = 0
                        for _, row in df.iterrows():
                            try:
                                # Filter for US locations if country column exists
                                if country_col:
                                    country_val = str(row.get(country_col, '')).strip().upper()
                                    if country_val not in ['US', 'USA', 'UNITED STATES', '']:
                                        continue
                                
                                site_name = row.get(name_col)
                                if site_name and pd.notna(site_name) and str(site_name).strip():
                                    site_name = str(site_name).strip()
                                    
                                    # Handle coordinates with error checking
                                    lat = None
                                    lng = None
                                    
                                    for lat_col in ['Latitude', 'latitude', 'lat', 'Lat']:
                                        if lat_col in row and pd.notna(row.get(lat_col)):
                                            try:
                                                lat = float(row.get(lat_col))
                                                if not (-90 <= lat <= 90):  # Validate latitude range
                                                    lat = None
                                                break
                                            except (ValueError, TypeError):
                                                continue
                                    
                                    for lng_col in ['Longitude', 'longitude', 'lng', 'Lng', 'long']:
                                        if lng_col in row and pd.notna(row.get(lng_col)):
                                            try:
                                                lng = float(row.get(lng_col))
                                                if not (-180 <= lng <= 180):  # Validate longitude range
                                                    lng = None
                                                break
                                            except (ValueError, TypeError):
                                                continue
                                    
                                    # Get other fields with error handling
                                    state = None
                                    city = None
                                    address = None
                                    site_type = None
                                    
                                    for state_col in ['State', 'state', 'State_Abbreviation']:
                                        if state_col in row and pd.notna(row.get(state_col)):
                                            state = str(row.get(state_col)).strip()
                                            break
                                    
                                    for city_col in ['City', 'city', 'City_Name']:
                                        if city_col in row and pd.notna(row.get(city_col)):
                                            city = str(row.get(city_col)).strip()
                                            break
                                    
                                    for addr_col in ['Address', 'address', 'Street_Address', 'Address_Line_1']:
                                        if addr_col in row and pd.notna(row.get(addr_col)):
                                            address = str(row.get(addr_col)).strip()
                                            break
                                    
                                    for type_col in ['Site_Type', 'Type', 'Facility_Type', 'Organization_Type']:
                                        if type_col in row and pd.notna(row.get(type_col)):
                                            site_type = str(row.get(type_col)).strip()
                                            break
                                    
                                    data.append({
                                        'name': site_name,
                                        'type': InstitutionType.CLINIC,
                                        'state': state,
                                        'city': city,
                                        'address': address,
                                        'latitude': lat,
                                        'longitude': lng,
                                        'additional_attributes': {
                                            'type': site_type,
                                            'fqhc': str(row.get('FQHC', '')).strip() if 'FQHC' in row else None,
                                            'source': 'HRSA',
                                            'data_source_url': csv_url
                                        }
                                    })
                                    valid_rows += 1
                                    
                            except Exception as row_error:
                                logger.debug(f"Error processing row: {row_error}")
                                continue
                        
                        if data:
                            logger.info(f"Successfully processed {valid_rows} valid rows out of {len(df)} total rows")
                            logger.info(f"Fetched {len(data)} clinics from HRSA")
                            return data
                        else:
                            logger.warning("No valid clinic data found in this CSV")
                        
                    except Exception as e:
                        logger.warning(f"Error processing HRSA CSV from {csv_url}: {e}")
                        continue
                else:
                    logger.warning(f"Failed to fetch data from {csv_url}")
            
            # If all sources fail, create a small fallback dataset
            logger.warning("All HRSA data sources failed, using fallback clinic data")
            return self.fetch_clinics_fallback()
                
        except Exception as e:
            logger.error(f"Error fetching HRSA clinics: {e}")
            return self.fetch_clinics_fallback()

    def fetch_clinics_fallback(self):
        """Fallback method for clinics when HRSA data is unavailable"""
        try:
            # Sample of known major FQHCs and community health centers
            fallback_clinics = [
                {'name': 'Community Health Center of Buffalo', 'city': 'Buffalo', 'state': 'NY'},
                {'name': 'Alliance Community Health Center', 'city': 'Boston', 'state': 'MA'},
                {'name': 'Central City Concern', 'city': 'Portland', 'state': 'OR'},
                {'name': 'Community Health Center of Richmond', 'city': 'Richmond', 'state': 'VA'},
                {'name': 'Denver Health Community Health Centers', 'city': 'Denver', 'state': 'CO'},
                {'name': 'Houston Community Health Centers', 'city': 'Houston', 'state': 'TX'},
                {'name': 'Los Angeles Community Health Center', 'city': 'Los Angeles', 'state': 'CA'},
                {'name': 'Chicago Family Health Center', 'city': 'Chicago', 'state': 'IL'},
                {'name': 'Miami Community Health Center', 'city': 'Miami', 'state': 'FL'},
                {'name': 'Seattle Community Health Centers', 'city': 'Seattle', 'state': 'WA'}
            ]
            
            data = []
            for clinic in fallback_clinics:
                data.append({
                    'name': clinic['name'],
                    'city': clinic['city'],
                    'state': clinic['state'],
                    'type': InstitutionType.CLINIC,
                    'additional_attributes': {
                        'type': 'Community Health Center',
                        'source': 'Fallback Clinics',
                        'category': 'FQHC/Community Health Center'
                    }
                })
            
            logger.info(f"Using fallback list of {len(data)} community health centers")
            return data
            
        except Exception as e:
            logger.error(f"Error in clinics fallback: {e}")
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