#!/usr/bin/env python3
"""
Test script to verify the medical institutions extraction setup
Run this before attempting the full extraction
"""

import sys
import importlib
from pathlib import Path

def test_imports():
    """Test all required imports"""
    print("🔍 Testing imports...")
    
    required_modules = [
        'requests',
        'pandas', 
        'bs4',
        'pdfplumber',
        'fuzzywuzzy',
        'psycopg2',
        'logging',
        'json',
        'tempfile',
        're'
    ]
    
    missing = []
    for module in required_modules:
        try:
            importlib.import_module(module)
            print(f"  ✅ {module}")
        except ImportError as e:
            print(f"  ❌ {module}: {e}")
            missing.append(module)
    
    if missing:
        print(f"\n❌ Missing modules: {', '.join(missing)}")
        print("Install with: pip install -r requirements.txt")
        return False
    else:
        print("\n✅ All imports successful!")
        return True

def test_project_structure():
    """Test project file structure"""
    print("\n🔍 Testing project structure...")
    
    required_files = [
        'config.py',
        'extractors/__init__.py',
        'extractors/base.py',
        'extractors/usa.py',
        'extractors/ind.py',
        'extractors/can.py',
        'extractors/chn.py',
        'run_extraction.py',
        'extraction_monitor.py'
    ]
    
    missing = []
    for file_path in required_files:
        if Path(file_path).exists():
            print(f"  ✅ {file_path}")
        else:
            print(f"  ❌ {file_path}")
            missing.append(file_path)
    
    if missing:
        print(f"\n❌ Missing files: {', '.join(missing)}")
        return False
    else:
        print("\n✅ All required files present!")
        return True

def test_database_connection():
    """Test database connection"""
    print("\n🔍 Testing database connection...")
    
    try:
        from config import DB_PARAMS
        import psycopg2
        
        print(f"  Database: {DB_PARAMS['dbname']}")
        print(f"  Host: {DB_PARAMS['host']}:{DB_PARAMS['port']}")
        print(f"  User: {DB_PARAMS['user']}")
        
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        # Test if institutions table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'institutions'
            );
        """)
        table_exists = cur.fetchone()[0]
        
        if table_exists:
            cur.execute("SELECT COUNT(*) FROM institutions;")
            count = cur.fetchone()[0]
            print(f"  ✅ Database connected! Current records: {count:,}")
        else:
            print("  ⚠️  Database connected but 'institutions' table not found")
            print("  Run: psql -U postgres -f medical_institutions/repository/init_db.sql")
        
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"  ❌ Database connection failed: {e}")
        print("  Make sure PostgreSQL is running and database is created")
        return False

def test_extractor_imports():
    """Test extractor class imports"""
    print("\n🔍 Testing extractor imports...")
    
    try:
        from extractors import extractor_registry
        print(f"  Available extractors: {list(extractor_registry.keys())}")
        
        for country, extractor_class in extractor_registry.items():
            try:
                extractor = extractor_class()
                print(f"  ✅ {country}: {extractor_class.__name__}")
                extractor.close()  # Close any DB connections
            except Exception as e:
                print(f"  ❌ {country}: {e}")
                return False
        
        print("\n✅ All extractors imported successfully!")
        return True
        
    except Exception as e:
        print(f"  ❌ Extractor import failed: {e}")
        return False

def test_internet_connectivity():
    """Test internet connectivity to key sources"""
    print("\n🔍 Testing internet connectivity...")
    
    import requests
    
    test_urls = [
        'https://www.avma.org',
        'https://lcme.org',
        'https://en.wikipedia.org',
        'https://data.cms.gov'
    ]
    
    success_count = 0
    for url in test_urls:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                print(f"  ✅ {url}")
                success_count += 1
            else:
                print(f"  ⚠️  {url} (Status: {response.status_code})")
        except Exception as e:
            print(f"  ❌ {url}: {e}")
    
    if success_count == len(test_urls):
        print("\n✅ All connectivity tests passed!")
        return True
    else:
        print(f"\n⚠️  {success_count}/{len(test_urls)} connectivity tests passed")
        return True  # Don't fail on connectivity issues

def main():
    """Run all tests"""
    print("🚀 MEDICAL INSTITUTIONS EXTRACTION - SETUP TEST")
    print("=" * 60)
    
    tests = [
        test_imports,
        test_project_structure,
        test_database_connection,
        test_extractor_imports,
        test_internet_connectivity
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append(False)
    
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(results)
    total = len(results)
    
    if passed == total:
        print(f"🎉 ALL TESTS PASSED ({passed}/{total})")
        print("\nYou can now run:")
        print("  python run_extraction.py --countries USA,IND --verbose")
    else:
        print(f"⚠️  SOME TESTS FAILED ({passed}/{total})")
        print("Please fix the issues above before running extraction")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)