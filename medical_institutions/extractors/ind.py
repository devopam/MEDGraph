import logging
import requests
import pandas as pd
from io import StringIO
import bs4 as BeautifulSoup
import json
import re
from config import InstitutionType
from extractors.base import BaseExtractor

logger = logging.getLogger(__name__)

class INDExtractor(BaseExtractor):
    def __init__(self):
        super().__init__('IND')

    def fetch_data(self):
        all_data = []
        all_data += self.fetch_med_nmc() or []
        all_data += self.fetch_vet_vci() or []
        all_data += self.fetch_hospitals_cghs() or []
        all_data += self.fetch_hospitals_nhp() or []
        all_data += self.fetch_aiims() or []
        all_data += self.fetch_hospitals_wiki() or []
        all_data += self.fetch_med_wiki() or []
        all_data += self.fetch_vet_wiki() or []
        logger.info(f"Fetched {len(all_data)} raw records from all sources for India")
        return all_data

    def fetch_med_nmc(self):
        """Fetch medical colleges from National Medical Commission"""
        try:
            # NMC has a searchable database
            url = "https://www.nmc.org.in/information-desk/college-and-course-search/"
            response = self.get_with_retry(url)
            if not response:
                return []
            
            # Try to get the data through their API or form submission
            api_url = "https://www.nmc.org.in/wp-admin/admin-ajax.php"
            data = {
                'action': 'get_colleges',
                'course_type': 'MBBS',
                'state': '',
                'college_type': ''
            }
            
            response = requests.post(api_url, data=data, timeout=10)
            if response.status_code == 200:
                colleges = response.json().get('data', [])
                data = []
                for college in colleges:
                    data.append({
                        'name': college.get('college_name'),
                        'state': college.get('state'),
                        'city': college.get('city'),
                        'type': InstitutionType.MEDICAL_SCHOOL,
                        'additional_attributes': {
                            'recognition_status': college.get('recognition_status'),
                            'course': college.get('course'),
                            'intake': college.get('annual_intake'),
                            'source': 'NMC'
                        }
                    })
                logger.info(f"Fetched {len(data)} medical colleges from NMC")
                return data
        except Exception as e:
            logger.error(f"Error fetching NMC: {e}")
        
        # Fallback to scraping the page
        try:
            url = "https://www.nmc.org.in/information-desk/college-and-course-search/"
            response = self.get_with_retry(url)
            if not response:
                return []
            
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            data = []
            
            # Look for college listings in tables or lists
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')[1:]  # Skip header
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        name = cols[0].text.strip()
                        state = cols[1].text.strip()
                        course = cols[2].text.strip() if len(cols) > 2 else 'MBBS'
                        
                        if name and 'college' in name.lower():
                            data.append({
                                'name': name,
                                'state': state,
                                'type': InstitutionType.MEDICAL_SCHOOL,
                                'additional_attributes': {
                                    'course': course,
                                    'source': 'NMC'
                                }
                            })
            
            logger.info(f"Fetched {len(data)} medical colleges from NMC (scraping)")
            return data
        except Exception as e:
            logger.error(f"Error fetching NMC (scraping): {e}")
            return []

    def fetch_vet_vci(self):
        """Fetch veterinary colleges from Veterinary Council of India"""
        try:
            url = "https://vci.nic.in/vets_college.htm"
            response = self.get_with_retry(url)
            if not response:
                return []
            
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            data = []
            
            # Look for state-wise listings
            current_state = None
            for element in soup.find_all(['h3', 'h4', 'li', 'p']):
                text = element.text.strip()
                
                # Check if this is a state header
                if any(state in text for state in ['Pradesh', 'State', 'Karnataka', 'Maharashtra', 'Gujarat', 'Punjab', 'Haryana']):
                    current_state = text
                
                # Check if this is a college name
                elif 'college' in text.lower() and 'veterinary' in text.lower():
                    data.append({
                        'name': text,
                        'state': current_state,
                        'type': InstitutionType.VETERINARY_SCHOOL,
                        'additional_attributes': {'source': 'VCI'}
                    })
            
            logger.info(f"Fetched {len(data)} veterinary colleges from VCI")
            return data
        except Exception as e:
            logger.error(f"Error fetching VCI: {e}")
            return []

    def fetch_hospitals_cghs(self):
        """Fetch empaneled hospitals from CGHS"""
        try:
            url = "https://cghs.gov.in/CGHSProviders.html"
            response = self.get_with_retry(url)
            if not response:
                return []
            
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            data = []
            
            # Look for hospital listings
            for link in soup.find_all('a', href=re.compile(r'pdf|doc|xls')):
                if 'hospital' in link.text.lower() or 'provider' in link.text.lower():
                    # This would need to download and parse PDF/Excel files
                    # For now, we'll extract what we can from the page
                    state = link.text.split('-')[0].strip() if '-' in link.text else None
                    data.append({
                        'name': f"CGHS Empaneled Hospitals - {state}",
                        'state': state,
                        'type': InstitutionType.HOSPITAL,
                        'additional_attributes': {
                            'empanelment': 'CGHS',
                            'source': 'CGHS'
                        }
                    })
            
            logger.info(f"Fetched {len(data)} CGHS hospital references")
            return data
        except Exception as e:
            logger.error(f"Error fetching CGHS: {e}")
            return []

    def fetch_hospitals_nhp(self):
        """Fetch hospitals from National Health Portal"""
        try:
            url = "https://www.nhp.gov.in/healthlyliving/hospitals"
            response = self.get_with_retry(url)
            if not response:
                return []
            
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            data = []
            
            # Look for hospital directories or state-wise links
            for link in soup.find_all('a', href=True):
                if 'hospital' in link.get('href', '').lower():
                    state_response = self.get_with_retry(f"https://www.nhp.gov.in{link['href']}")
                    if state_response:
                        state_soup = BeautifulSoup.BeautifulSoup(state_response.text, 'lxml')
                        
                        # Extract hospital information from the state page
                        for hospital_link in state_soup.find_all('a'):
                            hospital_text = hospital_link.text.strip()
                            if 'hospital' in hospital_text.lower():
                                data.append({
                                    'name': hospital_text,
                                    'type': InstitutionType.HOSPITAL,
                                    'additional_attributes': {'source': 'NHP'}
                                })
            
            logger.info(f"Fetched {len(data)} hospitals from NHP")
            return data
        except Exception as e:
            logger.error(f"Error fetching NHP: {e}")
            return []

    def fetch_aiims(self):
        """Fetch AIIMS institutions"""
        try:
            aiims_list = [
                {'name': 'All India Institute of Medical Sciences, New Delhi', 'city': 'New Delhi', 'state': 'Delhi'},
                {'name': 'AIIMS Bhopal', 'city': 'Bhopal', 'state': 'Madhya Pradesh'},
                {'name': 'AIIMS Bhubaneswar', 'city': 'Bhubaneswar', 'state': 'Odisha'},
                {'name': 'AIIMS Jodhpur', 'city': 'Jodhpur', 'state': 'Rajasthan'},
                {'name': 'AIIMS Patna', 'city': 'Patna', 'state': 'Bihar'},
                {'name': 'AIIMS Raipur', 'city': 'Raipur', 'state': 'Chhattisgarh'},
                {'name': 'AIIMS Rishikesh', 'city': 'Rishikesh', 'state': 'Uttarakhand'},
                {'name': 'AIIMS Nagpur', 'city': 'Nagpur', 'state': 'Maharashtra'},
                {'name': 'AIIMS Mangalagiri', 'city': 'Mangalagiri', 'state': 'Andhra Pradesh'},
                {'name': 'AIIMS Bathinda', 'city': 'Bathinda', 'state': 'Punjab'},
                {'name': 'AIIMS Deoghar', 'city': 'Deoghar', 'state': 'Jharkhand'},
                {'name': 'AIIMS Gorakhpur', 'city': 'Gorakhpur', 'state': 'Uttar Pradesh'},
                {'name': 'AIIMS Jammu', 'city': 'Jammu', 'state': 'Jammu and Kashmir'},
                {'name': 'AIIMS Kalyani', 'city': 'Kalyani', 'state': 'West Bengal'},
                {'name': 'AIIMS Raebareli', 'city': 'Raebareli', 'state': 'Uttar Pradesh'},
                {'name': 'AIIMS Bilaspur', 'city': 'Bilaspur', 'state': 'Himachal Pradesh'},
                {'name': 'AIIMS Madurai', 'city': 'Madurai', 'state': 'Tamil Nadu'},
                {'name': 'AIIMS Bibinagar', 'city': 'Bibinagar', 'state': 'Telangana'},
                {'name': 'AIIMS Vijaypur', 'city': 'Vijaypur', 'state': 'Jammu and Kashmir'},
                {'name': 'AIIMS Darbhanga', 'city': 'Darbhanga', 'state': 'Bihar'},
                {'name': 'AIIMS Rajkot', 'city': 'Rajkot', 'state': 'Gujarat'},
                {'name': 'AIIMS Guwahati', 'city': 'Guwahati', 'state': 'Assam'},
            ]
            
            data = []
            for aiims in aiims_list:
                data.append({
                    'name': aiims['name'],
                    'city': aiims['city'],
                    'state': aiims['state'],
                    'type': InstitutionType.ACADEMIC_MEDICAL_CENTER,
                    'additional_attributes': {
                        'institution_type': 'AIIMS',
                        'source': 'Government Records'
                    }
                })
            
            logger.info(f"Added {len(data)} AIIMS institutions")
            return data
        except Exception as e:
            logger.error(f"Error adding AIIMS: {e}")
            return []

    def fetch_hospitals_wiki(self):
        """Fetch hospitals from Wikipedia"""
        try:
            url = "https://en.wikipedia.org/wiki/List_of_hospitals_in_India"
            response = self.get_with_retry(url)
            if not response:
                return []
            
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            data = []
            current_state = None
            
            for header in soup.find_all(['h2', 'h3', 'li']):
                if header.name in ['h2', 'h3']:
                    # Check if this is a state/region header
                    header_text = header.text.strip()
                    if any(word in header_text for word in ['Pradesh', 'State', 'Territory', 'Delhi', 'Mumbai', 'Bangalore', 'Chennai']):
                        current_state = header_text.replace('[edit]', '').strip()
                
                elif header.name == 'li' and current_state:
                    text = header.text.strip()
                    if 'hospital' in text.lower() or 'medical' in text.lower():
                        # Extract hospital name (remove references like [1], [2])
                        name = re.sub(r'\[\d+\]', '', text).strip()
                        if name:
                            data.append({
                                'name': name,
                                'state': current_state,
                                'type': InstitutionType.HOSPITAL,
                                'additional_attributes': {'source': 'Wikipedia'}
                            })
            
            logger.info(f"Fetched {len(data)} hospitals from Wikipedia")
            return data
        except Exception as e:
            logger.error(f"Error fetching Wikipedia hospitals: {e}")
            return []

    def fetch_med_wiki(self):
        """Fetch medical colleges from Wikipedia"""
        try:
            url = "https://en.wikipedia.org/wiki/List_of_medical_colleges_in_India"
            response = self.get_with_retry(url)
            if not response:
                return []
            
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            data = []
            
            # Look for tables with medical college information
            tables = soup.find_all('table', class_='wikitable')
            for table in tables:
                rows = table.find_all('tr')[1:]  # Skip header
                for row in rows:
                    cols = row.find_all(['td', 'th'])
                    if len(cols) >= 2:
                        name = cols[0].text.strip()
                        location = cols[1].text.strip() if len(cols) > 1 else ''
                        
                        # Parse location for state/city
                        if ',' in location:
                            parts = location.split(',')
                            city = parts[0].strip()
                            state = parts[-1].strip()
                        else:
                            city = location
                            state = None
                        
                        if name and 'college' in name.lower():
                            data.append({
                                'name': name,
                                'city': city,
                                'state': state,
                                'type': InstitutionType.MEDICAL_SCHOOL,
                                'additional_attributes': {
                                    'est_year': cols[2].text.strip() if len(cols) > 2 else None,
                                    'source': 'Wikipedia'
                                }
                            })
            
            logger.info(f"Fetched {len(data)} medical colleges from Wikipedia")
            return data
        except Exception as e:
            logger.error(f"Error fetching Wikipedia medical colleges: {e}")
            return []

    def fetch_vet_wiki(self):
        """Fetch veterinary colleges from Wikipedia"""
        try:
            url = "https://en.wikipedia.org/wiki/List_of_schools_of_veterinary_medicine#India"
            response = self.get_with_retry(url)
            if not response:
                return []
            
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            data = []
            
            # Find the India section
            india_section = soup.find('span', id='India')
            if india_section:
                # Get the next sibling which should be a list
                next_element = india_section.parent.find_next_sibling(['ul', 'ol'])
                if next_element:
                    for li in next_element.find_all('li'):
                        text = li.text.strip()
                        if text:
                            data.append({
                                'name': text,
                                'type': InstitutionType.VETERINARY_SCHOOL,
                                'additional_attributes': {'source': 'Wikipedia'}
                            })
            
            logger.info(f"Fetched {len(data)} veterinary colleges from Wikipedia")
            return data
        except Exception as e:
            logger.error(f"Error fetching Wikipedia veterinary colleges: {e}")
            return []

    def normalize(self, data):
        """Normalize Indian institution data"""
        for item in data:
            # Standardize state names
            if item.get('state'):
                state = item['state']
                # Common state name normalizations
                state_mapping = {
                    'Tamil Nadu': 'Tamil Nadu',
                    'TN': 'Tamil Nadu',
                    'Karnataka': 'Karnataka',
                    'KA': 'Karnataka',
                    'Maharashtra': 'Maharashtra',
                    'MH': 'Maharashtra',
                    'Gujarat': 'Gujarat',
                    'GJ': 'Gujarat',
                    'Rajasthan': 'Rajasthan',
                    'RJ': 'Rajasthan',
                    'Uttar Pradesh': 'Uttar Pradesh',
                    'UP': 'Uttar Pradesh',
                    'West Bengal': 'West Bengal',
                    'WB': 'West Bengal',
                    'Delhi': 'Delhi',
                    'NCT of Delhi': 'Delhi',
                    'New Delhi': 'Delhi'
                }
                item['state'] = state_mapping.get(state, state)
            
            # Clean institution names
            if item.get('name'):
                name = item['name']
                # Remove common prefixes/suffixes that might cause duplicates
                name = re.sub(r'\[edit\]', '', name)
                name = re.sub(r'\[\d+\]', '', name)  # Remove reference numbers
                name = name.strip()
                item['name'] = name
        
        return data