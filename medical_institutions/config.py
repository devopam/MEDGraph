from enum import Enum
import os
from pathlib import Path

# Database parameters (use environment variables for production)
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'medical_institutions'),
    'user': os.getenv('DB_USER', 'medinst_user'),
    'password': os.getenv('DB_PASSWORD', 'Adm1nistr@t0r'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432))
}

class InstitutionType(Enum):
    HOSPITAL = 'hospital'
    CLINIC = 'clinic'
    MEDICAL_SCHOOL = 'medical_school'
    VETERINARY_SCHOOL = 'veterinary_school'
    ACADEMIC_MEDICAL_CENTER = 'academic_medical_center'
    OTHER = 'other'

# Enhanced extraction settings
DEFAULT_REFRESH_DAYS = 30
MAX_RETRIES = 5
RETRY_BACKOFF = 2
REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 1  # seconds between requests

# Data quality settings
MIN_SIMILARITY_THRESHOLD = 90  # for deduplication
MAX_RECORDS_PER_SOURCE = 10000  # prevent runaway extractions

# Logging configuration
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Country-specific settings
COUNTRY_SETTINGS = {
    'USA': {
        'priority_sources': ['LCME', 'AACOM', 'CMS', 'AVMA'],
        'max_records': 15000,
        'rate_limit': 0.5
    },
    'IND': {
        'priority_sources': ['NMC', 'VCI', 'AIIMS'],
        'max_records': 8000,
        'rate_limit': 1.0
    },
    'CHN': {
        'priority_sources': ['WCAME', 'WDOMS', 'NHC'],
        'max_records': 12000,
        'rate_limit': 1.5
    },
    'CAN': {
        'priority_sources': ['CACMS', 'CVMA', 'ODHF'],
        'max_records': 5000,
        'rate_limit': 0.5
    }
}

# Source reliability scoring
SOURCE_RELIABILITY = {
    'government': 10,  # Official government sources
    'professional_body': 9,  # Medical councils, accreditation bodies
    'academic': 8,  # University databases
    'commercial': 6,  # Commercial directories
    'wiki': 5,  # Wikipedia and similar
    'other': 3
}