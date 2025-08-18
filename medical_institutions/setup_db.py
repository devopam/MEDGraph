#!/usr/bin/env python3
"""
Database setup helper for medical institutions extraction
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

# Database configuration
ADMIN_DB_PARAMS = {
    'dbname': 'postgres',  # Connect to default postgres DB first
    'user': 'postgres',
    'password': 'password',  # Update this to your postgres password
    'host': 'localhost',
    'port': 5432
}

TARGET_DB_PARAMS = {
    'dbname': 'medical_institutions',
    'user': 'medinst_user',
    'password': 'Adm1nistr@t0r',
    'host': 'localhost',
    'port': 5432
}

def create_database_and_user():
    """Create database and user if they don't exist"""
    try:
        # Connect as admin
        print("Connecting to PostgreSQL as admin...")
        conn = psycopg2.connect(**ADMIN_DB_PARAMS)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Check if user exists
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (TARGET_DB_PARAMS['user'],))
        if not cur.fetchone():
            print(f"Creating user {TARGET_DB_PARAMS['user']}...")
            cur.execute(f"CREATE USER {TARGET_DB_PARAMS['user']} WITH ENCRYPTED PASSWORD %s", (TARGET_DB_PARAMS['password'],))
        else:
            print(f"User {TARGET_DB_PARAMS['user']} already exists")
        
        # Check if database exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (TARGET_DB_PARAMS['dbname'],))
        if not cur.fetchone():
            print(f"Creating database {TARGET_DB_PARAMS['dbname']}...")
            cur.execute(f"CREATE DATABASE {TARGET_DB_PARAMS['dbname']}")
            cur.execute(f"GRANT ALL PRIVILEGES ON DATABASE {TARGET_DB_PARAMS['dbname']} TO {TARGET_DB_PARAMS['user']}")
        else:
            print(f"Database {TARGET_DB_PARAMS['dbname']} already exists")
        
        cur.close()
        conn.close()
        print("✅ Database and user setup complete")
        return True
        
    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        return False

def create_tables():
    """Create the institutions table and related objects"""
    try:
        print("Creating tables...")
        conn = psycopg2.connect(**TARGET_DB_PARAMS)
        cur = conn.cursor()
        
        # Create enum type
        cur.execute("""
            DO $$ BEGIN
                CREATE TYPE institution_type AS ENUM ('hospital', 'clinic', 'medical_school', 'veterinary_school', 'academic_medical_center', 'other');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        """)
        
        # Create table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS institutions (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                type institution_type NOT NULL,
                country TEXT NOT NULL,
                state TEXT,
                city TEXT,
                address TEXT,
                website TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                additional_attributes JSONB,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                search_vector TSVECTOR
            )
        """)
        
        # Create indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_country ON institutions(country)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_name ON institutions(name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_search_vector ON institutions USING GIN(search_vector)")
        
        # Create trigger function
        cur.execute("""
            CREATE OR REPLACE FUNCTION update_search_vector() RETURNS TRIGGER AS $$
            BEGIN
                NEW.search_vector := to_tsvector('english', COALESCE(NEW.name, '') || ' ' || COALESCE(NEW.country, ''));
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        # Create trigger
        cur.execute("""
            DROP TRIGGER IF EXISTS trg_update_search_vector ON institutions;
            CREATE TRIGGER trg_update_search_vector
            BEFORE INSERT OR UPDATE ON institutions
            FOR EACH ROW EXECUTE FUNCTION update_search_vector();
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Tables created successfully")
        return True
        
    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        return False

def test_connection():
    """Test connection to the target database"""
    try:
        print("Testing connection to target database...")
        conn = psycopg2.connect(**TARGET_DB_PARAMS)
        cur = conn.cursor()
        
        # Test basic query
        cur.execute("SELECT COUNT(*) FROM institutions")
        count = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        print(f"✅ Connection successful. Current records: {count}")
        return True
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

def main():
    print("🚀 MEDICAL INSTITUTIONS DATABASE SETUP")
    print("=" * 50)
    
    # Update admin password if needed
    if ADMIN_DB_PARAMS['password'] == 'password':
        print("⚠️  Please update ADMIN_DB_PARAMS['password'] in this script with your postgres password")
        admin_password = input("Enter your postgres admin password: ").strip()
        if admin_password:
            ADMIN_DB_PARAMS['password'] = admin_password
        else:
            print("❌ No password provided. Exiting.")
            return False
    
    steps = [
        ("Creating database and user", create_database_and_user),
        ("Creating tables", create_tables),
        ("Testing connection", test_connection)
    ]
    
    for step_name, step_func in steps:
        print(f"\n{step_name}...")
        if not step_func():
            print(f"❌ Failed at step: {step_name}")
            return False
    
    print("\n🎉 Database setup complete!")
    print("\nYou can now run:")
    print("  python simple_test.py")
    print("  python run_extraction.py --countries USA,IND --verbose")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)