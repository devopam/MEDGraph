#!/usr/bin/env python3
"""
Simple test to verify imports work correctly
"""

import sys
import os

def test_basic_imports():
    print("Testing basic imports...")
    try:
        import requests
        import pandas
        import bs4
        import psycopg2
        print("✅ Basic imports successful")
        return True
    except ImportError as e:
        print(f"❌ Basic import failed: {e}")
        return False

def test_config_import():
    print("Testing config import...")
    try:
        from config import DB_PARAMS, InstitutionType
        print("✅ Config import successful")
        print(f"Available institution types: {[t.value for t in InstitutionType]}")
        return True
    except Exception as e:
        print(f"❌ Config import failed: {e}")
        return False

def test_extractor_imports():
    print("Testing extractor imports...")
    try:
        # Test individual extractor imports
        from extractors.base import BaseExtractor
        print("✅ BaseExtractor imported")
        
        from extractors.usa import USAExtractor
        print("✅ USAExtractor imported")
        
        from extractors.ind import INDExtractor
        print("✅ INDExtractor imported")
        
        from extractors.can import CANExtractor
        print("✅ CANExtractor imported") 
        
        from extractors.chn import CHNExtractor
        print("✅ CHNExtractor imported")
        
        # Test registry
        from extractors import extractor_registry
        print(f"✅ Registry imported with: {list(extractor_registry.keys())}")
        
        return True
    except Exception as e:
        print(f"❌ Extractor import failed: {e}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("🔍 QUICK IMPORT TEST")
    print("=" * 40)
    
    # Add current directory to Python path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
        print(f"Added {current_dir} to Python path")
    
    tests = [
        test_basic_imports,
        test_config_import, 
        test_extractor_imports
    ]
    
    results = []
    for test in tests:
        print()
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append(False)
    
    print("\n" + "=" * 40)
    passed = sum(results)
    total = len(results)
    
    if passed == total:
        print(f"🎉 ALL TESTS PASSED ({passed}/{total})")
    else:
        print(f"⚠️ SOME TESTS FAILED ({passed}/{total})")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)