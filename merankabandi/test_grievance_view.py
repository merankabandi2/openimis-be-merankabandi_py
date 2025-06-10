"""
Test script to verify grievance dashboard data
"""
import os
import sys
import django

# Setup Django
sys.path.insert(0, '/Users/anthbel/projects/wb/buimis/openimis-be_py/openIMIS')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'openIMIS.settings')
os.environ.setdefault('DB_DEFAULT', 'postgresql')
django.setup()

from django.db import connection
from merankabandi.optimized_dashboard_service import OptimizedDashboardService
import json

def test_grievance_data():
    print("=== Testing Grievance Dashboard Data ===\n")
    
    # 1. Check materialized view directly
    print("1. Checking materialized view directly:")
    with connection.cursor() as cursor:
        cursor.execute("SELECT sensitive_tickets FROM dashboard_grievances WHERE summary_type = 'SUMMARY'")
        result = cursor.fetchone()
        print(f"   sensitive_tickets from view: {result[0] if result else 'No data'}")
    
    # 2. Check service layer
    print("\n2. Checking service layer:")
    data = OptimizedDashboardService.get_grievance_dashboard()
    print(f"   sensitive_tickets from service: {data['summary'].get('sensitive_tickets', 'Not found')}")
    
    # 3. Check GraphQL layer
    print("\n3. Testing GraphQL query:")
    from graphene.test import Client
    from merankabandi.schema import schema
    
    client = Client(schema)
    
    query = '''
    query {
        optimizedGrievanceDashboard {
            summary {
                totalTickets
                sensitiveTickets
            }
            categoryDistribution {
                category
                count
            }
        }
    }
    '''
    
    result = client.execute(query)
    
    if result.get('data'):
        summary = result['data']['optimizedGrievanceDashboard']['summary']
        print(f"   totalTickets from GraphQL: {summary.get('totalTickets', 'Not found')}")
        print(f"   sensitiveTickets from GraphQL: {summary.get('sensitiveTickets', 'Not found')}")
        
        print("\n   Category distribution:")
        for cat in result['data']['optimizedGrievanceDashboard']['categoryDistribution']:
            print(f"     {cat['category']}: {cat['count']}")
    else:
        print(f"   GraphQL Error: {result.get('errors', 'Unknown error')}")
    
    # 4. Check raw ticket count
    print("\n4. Checking raw ticket count:")
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*) FROM grievance_social_protection_ticket
            WHERE "isDeleted" = false
            AND (
                category::text LIKE '%violence_vbg%' 
                OR category::text LIKE '%corruption%'
                OR category::text LIKE '%discrimination_ethnie_religion%'
            )
        """)
        result = cursor.fetchone()
        print(f"   Raw sensitive ticket count: {result[0] if result else 0}")

if __name__ == "__main__":
    test_grievance_data()