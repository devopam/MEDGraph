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

class CHNExtractor(BaseExtractor):
    def __init__(self):
        super().__init__('CHN')

    def fetch_data(self):
        all_data = []
        all_data += self.fetch_vet_avma('China') or []
        all_data += self.fetch_vet_wiki() or []
        all_data += self.fetch_med_wiki() or []
        all_data += self.fetch_med_wdoms() or []
        all_data += self.fetch_med_wcame() or []
        all_data += self.fetch_hospitals_wiki() or []
        all_data += self.fetch_hospitals_nhc() or []
        all_data += self.fetch_hospitals_csds() or []
        logger.info(f"Fetched {len(all_data)} raw records from all sources for China")
        return all_data

    def fetch_vet_wiki(self):
        try:
            url = "https://en.wikipedia.org/wiki/List_of_schools_of_veterinary_medicine#China"
            response = self.get_with_retry(url)
            if not response:
                return []
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            section = soup.find('span', id='China').parent.find_next_sibling('ul')
            data = []
            for li in section.find_all('li'):
                text = li.text.strip()
                match = re.match(r'^(.*?)( \((.*?)\))?$', text)
                name = match.group(1)
                local_name = match.group(3) if match.group(3) else None
                data.append({
                    'name': name,
                    'type': InstitutionType.VETERINARY_SCHOOL,
                    'additional_attributes': {'source': 'Wikipedia', 'local_name': local_name}
                })
            logger.info(f"Fetched {len(data)} vet schools from Wikipedia")
            return data
        except Exception as e:
            logger.error(f"Error fetching Wikipedia vet: {e}")
            return []

    def fetch_med_wiki(self):
        try:
            url = "https://en.wikipedia.org/wiki/List_of_medical_schools_in_Asia#People's_Republic_of_China"
            response = self.get_with_retry(url)
            if not response:
                return []
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            tables = soup.find_all('table', class_='wikitable')
            data = []
            for table in tables:
                for row in table.find_all('tr')[1:]:
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        province = cols[0].text.strip()
                        school = cols[1].text.strip()
                        city = cols[2].text.strip()
                        match = re.match(r'^(.*?)( \((.*?)\))?$', school)
                        name = match.group(1)
                        local_name = match.group(3) if match.group(3) else None
                        data.append({
                            'name': name,
                            'state': province,
                            'city': city,
                            'type': InstitutionType.MEDICAL_SCHOOL,
                            'additional_attributes': {'est_year': cols[3].text.strip() if len(cols) > 3 else None, 'source': 'Wikipedia', 'local_name': local_name}
                        })
            logger.info(f"Fetched {len(data)} med schools from Wikipedia")
            return data
        except Exception as e:
            logger.error(f"Error fetching Wikipedia med: {e}")
            return []

    def fetch_med_wdoms(self):
        try:
            base_url = "https://search.wdoms.org/home/SchoolSearch?Country=China&SchoolType=Medical&ProgramType=Undergraduate"
            parser = lambda soup: [
                {
                    'name': item.find('h3').text.strip(),
                    'city': location.split(',')[0].strip() if ',' in location else None,
                    'state': location.split(',')[1].strip() if ',' in location else None,
                    'type': InstitutionType.MEDICAL_SCHOOL,
                    'additional_attributes': {'source': 'WDOMS'}
                } for item in soup.find_all('div', class_='school-item') if (location := item.find('p', class_='location').text.strip())
            ]
            data = self.fetch_paginated_scrape(base_url, page_param='Page', max_pages=20, parser=parser)
            logger.info(f"Fetched {len(data)} med schools from WDOMS")
            return data
        except Exception as e:
            logger.error(f"Error fetching WDOMS: {e}")
            return []

    def fetch_med_wcame(self):
        try:
            url = "https://wcame.meduc.cn/en_school.php"
            response = self.get_with_retry(url)
            if not response:
                return []
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            table = soup.find('table')
            data = []
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if len(cols) >= 4:
                    school = cols[0].text.strip()
                    province = cols[1].text.strip()
                    program = cols[2].text.strip()
                    status = cols[3].text.strip()
                    data.append({
                        'name': school,
                        'state': province,
                        'type': InstitutionType.MEDICAL_SCHOOL,
                        'additional_attributes': {'program': program, 'status': status, 'source': 'WCAME'}
                    })
            logger.info(f"Fetched {len(data)} accredited med programs from WCAME")
            return data
        except Exception as e:
            logger.error(f"Error fetching WCAME: {e}")
            return []

    def fetch_hospitals_wiki(self):
        try:
            url = "https://en.wikipedia.org/wiki/List_of_hospitals_in_China"
            response = self.get_with_retry(url)
            if not response:
                return []
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            data = []
            current_province = None
            for header in soup.find_all(['h2', 'h3', 'li']):
                if header.name in ['h2', 'h3'] and 'Province' in header.text:
                    current_province = header.text.strip()
                elif header.name == 'li' and current_province:
                    text = header.text.strip()
                    match = re.match(r'^(.*?)( \((.*?)\))?$', text)
                    name = match.group(1)
                    local_name = match.group(3) if match.group(3) else None
                    data.append({
                        'name': name,
                        'state': current_province,
                        'type': InstitutionType.HOSPITAL,
                        'additional_attributes': {'source': 'Wikipedia', 'local_name': local_name}
                    })
            logger.info(f"Fetched {len(data)} hospitals from Wikipedia")
            return data
        except Exception as e:
            logger.error(f"Error fetching Wikipedia hospitals: {e}")
            return []

    def fetch_hospitals_nhc(self):
        try:
            url = "https://en.nhc.gov.cn/hospitals.html"
            response = self.get_with_retry(url)
            if not response:
                return []
            soup = BeautifulSoup.BeautifulSoup(response.text, 'lxml')
            data = []
            for link in soup.find_all('a', href=re.compile('hospitals-')):
                sub_url = f"https://en.nhc.gov.cn/{link['href']}"
                sub_response = self.get_with_retry(sub_url)
                if not sub_response:
                    continue
                sub_soup = BeautifulSoup.BeautifulSoup(sub_response.text, 'lxml')
                province = link.text.strip()
                for item in sub_soup.find_all('li', class_='hospital-item'):
                    name = item.find('h4').text.strip()
                    address = item.find('p', class_='address').text.strip()
                    data.append({
                        'name': name,
                        'state': province,
                        'address': address,
                        'type': InstitutionType.HOSPITAL,
                        'additional_attributes': {'source': 'NHC'}
                    })
            logger.info(f"Fetched {len(data)} hospitals from NHC")
            return data
        except Exception as e:
            logger.error(f"Error fetching NHC hospitals: {e}")
            return []

    def fetch_hospitals_csds(self):
        try:
            url = "https://citas.csde.washington.edu/data/hospital/hosmc.dat"
            response = self.get_with_retry(url)
            if not response:
                return []
            df = pd.read_csv(StringIO(response.text), sep='\s+', header=None, 
                             names=['GB86MC'] + [f'HFND{year}' for year in range(1950, 1986)] + 
                                   [f'HCNT{year}' for year in range(1950, 1986)] + 
                                   [f'HYRS{year}' for year in range(1950, 1986)])
            province_map = {
                '11': 'Beijing', '12': 'Tianjin', '13': 'Hebei', '14': 'Shanxi', '15': 'Inner Mongolia',
                '21': 'Liaoning', '22': 'Jilin', '23': 'Heilongjiang', '31': 'Shanghai', '32': 'Jiangsu',
                '33': 'Zhejiang', '34': 'Anhui', '35': 'Fujian', '36': 'Jiangxi', '37': 'Shandong',
                '41': 'Henan', '42': 'Hubei', '43': 'Hunan', '44': 'Guangdong', '45': 'Guangxi',
                '46': 'Hainan', '50': 'Chongqing', '51': 'Sichuan', '52': 'Guizhou', '53': 'Yunnan',
                '54': 'Tibet', '61': 'Shaanxi', '62': 'Gansu', '63': 'Qinghai', '64': 'Ningxia',
                '65': 'Xinjiang'
            }
            data = []
            for _, row in df.iterrows():
                code = str(row['GB86MC'])
                if code in ('-99999', '-66666', '-55555'):
                    continue
                province_code = code[:2]
                province = province_map.get(province_code, 'Unknown')
                count = row.get('HCNT1985', 0)
                if count > 0:
                    data.append({
                        'name': f'County Hospitals ({code})',
                        'type': InstitutionType.HOSPITAL,
                        'state': province,
                        'city': f'County {code[2:]}',
                        'additional_attributes': {'count_1985': count, 'foundings_1950_1985': sum(row[f'HFND{year}'] for year in range(1950, 1986)), 'source': 'CSDS'}
                    })
            logger.info(f"Fetched {len(data)} aggregated hospital entries from CSDS")
            return data
        except Exception as e:
            logger.error(f"Error fetching CSDS: {e}")
            return []

    def normalize(self, data):
        for item in data:
            if 'local_name' in item.get('additional_attributes', {}):
                item['additional_attributes']['local_name'] = item['additional_attributes']['local_name']
        return data