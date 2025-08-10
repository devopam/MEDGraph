from enum import Enum

# Database parameters (use environment variables or .env for production)
DB_PARAMS = {
    'dbname': 'medical_institutions_db',
    'user': 'your_user',
    'password': 'your_password',
    'host': 'localhost',
    'port': 5432
}

class InstitutionType(Enum):
    HOSPITAL = 'hospital'
    CLINIC = 'clinic'
    MEDICAL_SCHOOL = 'medical_school'
    VETERINARY_SCHOOL = 'veterinary_school'
    ACADEMIC_MEDICAL_CENTER = 'academic_medical_center'
    OTHER = 'other'

# Other globals, e.g., default refresh days
DEFAULT_REFRESH_DAYS = 30