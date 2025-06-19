#!/usr/bin/env python3
"""
Test script to verify the refactored subscription system works with the new database structure.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from daily_notify import get_subscriptions, get_properties_by_district, find_matching_properties_for_district
from sqlalchemy import create_engine, text

# Database connection
DB_URL = "postgresql://postgres:postgres@localhost:5433/real_estate"

def test_get_subscriptions():
    """Test the get_subscriptions function"""
    print("Testing get_subscriptions function...")
    
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as connection:
            # First, let's check if we have any subscriptions
            query = "SELECT COUNT(*) as count FROM subscription"
            result = connection.execute(text(query))
            count = result.fetchone().count
            print(f"Total subscriptions in database: {count}")
            
            if count == 0:
                print("No subscriptions found. Creating a test subscription...")
                create_test_subscription(connection)
            
            # Now test the get_subscriptions function
            subscriptions = get_subscriptions()
            print(f"Retrieved {len(subscriptions)} subscriptions")
            
            for i, sub in enumerate(subscriptions):
                print(f"\nSubscription {i+1}:")
                print(f"  User ID: {sub['user_id']}")
                print(f"  User Name: {sub['user_name']}")
                print(f"  User Type: {sub['user_type']}")
                print(f"  Price Range: {sub['min_price']} - {sub['max_price']}")
                print(f"  Area Range: {sub['min_area']} - {sub['max_area']}")
                print(f"  Bedrooms: {sub['num_bedrooms']}")
                print(f"  Toilets: {sub['num_toilets']}")
                print(f"  District IDs: {sub['district_ids']}")
                print(f"  Property Type IDs: {sub['property_type_ids']}")
                print(f"  Legal IDs: {sub['legal_ids']}")
                
        return True
        
    except Exception as e:
        print(f"Error testing get_subscriptions: {str(e)}")
        return False

def create_test_subscription(connection):
    """Create a test subscription for testing purposes"""
    try:
        # Insert main subscription
        subscription_query = """
        INSERT INTO subscription (user_id, user_name, user_type, min_price, max_price, min_area, max_area, num_bedrooms, num_toilets)
        VALUES (:user_id, :user_name, :user_type, :min_price, :max_price, :min_area, :max_area, :num_bedrooms, :num_toilets)
        RETURNING id
        """
        subscription_data = {
            'user_id': 'test_user_123',
            'user_name': 'Test User',
            'user_type': 'email',
            'min_price': 1.0,
            'max_price': 5.0,
            'min_area': 50.0,
            'max_area': 100.0,
            'num_bedrooms': 2,
            'num_toilets': 2
        }
        
        result = connection.execute(text(subscription_query), subscription_data)
        subscription_id = result.fetchone()[0]
        print(f"Created test subscription with ID: {subscription_id}")
        
        # Insert district subscription (Ba Đình - district_id = 1)
        district_query = """
        INSERT INTO district_subscription (user_id, district_id)
        VALUES (:user_id, :district_id)
        """
        connection.execute(text(district_query), {
            'user_id': 'test_user_123',
            'district_id': 1
        })
        
        # Insert property type subscription (Chung cư - property_type_id = 1)
        property_type_query = """
        INSERT INTO property_type_subscription (user_id, property_type_id)
        VALUES (:user_id, :property_type_id)
        """
        connection.execute(text(property_type_query), {
            'user_id': 'test_user_123',
            'property_type_id': 1
        })
        
        # Insert legal status subscription (Sổ đỏ - legal_id = 2)
        legal_query = """
        INSERT INTO legal_subscription (user_id, legal_id)
        VALUES (:user_id, :legal_id)
        """
        connection.execute(text(legal_query), {
            'user_id': 'test_user_123',
            'legal_id': 2
        })
        
        connection.commit()
        print("Test subscription created successfully")
        
    except Exception as e:
        print(f"Error creating test subscription: {str(e)}")
        connection.rollback()

def test_get_properties_by_district():
    """Test the get_properties_by_district function"""
    print("\nTesting get_properties_by_district function...")
    
    try:
        properties = get_properties_by_district(1)  # Ba Đình district
        print(f"Found {len(properties)} properties in Ba Đình district")
        
        if properties:
            print("Sample property:")
            prop = properties[0]
            print(f"  Title: {prop.title}")
            print(f"  Price: {prop.price}")
            print(f"  Area: {prop.area}")
            print(f"  Legal ID: {prop.legal_id}")
            print(f"  Property Type ID: {prop.property_type_id}")
        
        return True
        
    except Exception as e:
        print(f"Error testing get_properties_by_district: {str(e)}")
        return False

def test_find_matching_properties():
    """Test the find_matching_properties_for_district function"""
    print("\nTesting find_matching_properties_for_district function...")
    
    try:
        search_params = {
            'price_range': 3.0,  # 3 billion VND
            'area_range': 75.0,  # 75 m²
            'num_bedrooms': 2,
            'num_toilets': 2,
            'legal_status_id': 2,  # Sổ đỏ
            'property_type_id': 1  # Chung cư
        }
        
        matches = find_matching_properties_for_district(1, search_params)  # Ba Đình district
        print(f"Found {len(matches)} matching properties")
        
        for i, match in enumerate(matches):
            print(f"\nMatch {i+1}:")
            print(f"  Title: {match['title']}")
            print(f"  Price: {match['price']}")
            print(f"  Area: {match['area']}")
            print(f"  Similarity Score: {match['similarity_score']:.3f}")
        
        return True
        
    except Exception as e:
        print(f"Error testing find_matching_properties_for_district: {str(e)}")
        return False

def main():
    """Main test function"""
    print("Testing refactored subscription system...")
    print("=" * 50)
    
    # Test 1: Get subscriptions
    test1_passed = test_get_subscriptions()
    
    # Test 2: Get properties by district
    test2_passed = test_get_properties_by_district()
    
    # Test 3: Find matching properties
    test3_passed = test_find_matching_properties()
    
    print("\n" + "=" * 50)
    print("Test Results:")
    print(f"Test 1 (get_subscriptions): {'PASSED' if test1_passed else 'FAILED'}")
    print(f"Test 2 (get_properties_by_district): {'PASSED' if test2_passed else 'FAILED'}")
    print(f"Test 3 (find_matching_properties): {'PASSED' if test3_passed else 'FAILED'}")
    
    if all([test1_passed, test2_passed, test3_passed]):
        print("\n✅ All tests passed! The refactored subscription system is working correctly.")
    else:
        print("\n❌ Some tests failed. Please check the errors above.")

if __name__ == "__main__":
    main() 