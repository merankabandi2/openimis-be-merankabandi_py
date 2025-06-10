"""
Enhanced GraphQL Queries for Dashboard with Vulnerable Groups Reporting
Includes: Twa/Batwa, People with disabilities, Chronic illness, Refugees/Returnees
"""

import graphene
from graphene import ObjectType, String, Int, Float, List, Field, Boolean
from django.core.cache import cache
from django.db import connection
from datetime import datetime
import json


# Enhanced GraphQL Types for Vulnerable Groups
class VulnerableGroupCountsType(graphene.ObjectType):
    """Counts for each vulnerable group"""
    twa_count = graphene.Int()
    disabled_count = graphene.Int()
    chronic_illness_count = graphene.Int()
    refugee_count = graphene.Int()
    returnee_count = graphene.Int()
    total_vulnerable = graphene.Int()
    
    # Percentages
    twa_percentage = graphene.Float()
    disabled_percentage = graphene.Float()
    chronic_illness_percentage = graphene.Float()
    refugee_percentage = graphene.Float()
    returnee_percentage = graphene.Float()
    vulnerable_percentage = graphene.Float()


class VulnerableGroupDetailsType(graphene.ObjectType):
    """Detailed breakdown for vulnerable groups"""
    # Households counts
    twa_households = graphene.Int()
    disabled_households = graphene.Int()
    chronic_illness_households = graphene.Int()
    refugee_households = graphene.Int()
    returnee_households = graphene.Int()
    
    # Members counts
    twa_members = graphene.Int()
    disabled_members = graphene.Int()
    chronic_illness_members = graphene.Int()
    refugee_members = graphene.Int()
    returnee_members = graphene.Int()
    
    # Beneficiaries (primary recipients) counts
    twa_beneficiaries = graphene.Int()
    disabled_beneficiaries = graphene.Int()
    chronic_illness_beneficiaries = graphene.Int()
    refugee_beneficiaries = graphene.Int()
    returnee_beneficiaries = graphene.Int()
    
    # Disability types breakdown
    physical_disability_count = graphene.Int()
    mental_disability_count = graphene.Int()
    visual_disability_count = graphene.Int()
    hearing_disability_count = graphene.Int()
    
    # Percentages
    twa_coverage = graphene.Float()  # % of twa members who are beneficiaries
    disabled_coverage = graphene.Float()
    chronic_illness_coverage = graphene.Float()
    refugee_coverage = graphene.Float()
    returnee_coverage = graphene.Float()


class EnhancedGenderBreakdownType(graphene.ObjectType):
    """Gender breakdown with vulnerable groups"""
    male = graphene.Int()
    female = graphene.Int()
    twa = graphene.Int()
    disabled = graphene.Int()
    chronic_illness = graphene.Int()
    total = graphene.Int()
    
    male_percentage = graphene.Float()
    female_percentage = graphene.Float()
    twa_percentage = graphene.Float()
    disabled_percentage = graphene.Float()
    chronic_illness_percentage = graphene.Float()


class CommunityBreakdownType(graphene.ObjectType):
    """Community type breakdown"""
    community_type = graphene.String()
    count = graphene.Int()
    percentage = graphene.Float()


class LocationBreakdownType(graphene.ObjectType):
    """Location breakdown"""
    province = graphene.String()
    province_id = graphene.Int()
    count = graphene.Int()
    percentage = graphene.Float()


class EnhancedBeneficiaryBreakdownType(graphene.ObjectType):
    """Enhanced beneficiary breakdown including vulnerable groups"""
    gender_breakdown = graphene.Field(EnhancedGenderBreakdownType)
    vulnerable_groups = graphene.Field(VulnerableGroupCountsType)
    vulnerable_details = graphene.Field(VulnerableGroupDetailsType)
    community_breakdown = graphene.List(CommunityBreakdownType)
    location_breakdown = graphene.List(LocationBreakdownType)
    last_updated = graphene.String()


# Payment reporting removed - focus on beneficiary data only


class VulnerableGroupsService:
    """Service class to fetch vulnerable groups data from materialized views"""
    
    @staticmethod
    def get_vulnerable_groups_summary(filters=None):
        """Get summary of vulnerable groups from enhanced materialized view"""
        cache_key = f"vulnerable_groups_summary_{json.dumps(filters or {})}"
        cached_data = cache.get(cache_key)
        
        if cached_data:
            return cached_data
        
        with connection.cursor() as cursor:
            # Build query with optional filters
            query = """
                SELECT 
                    SUM(beneficiary_count) as total,
                    SUM(male_count) as male,
                    SUM(female_count) as female,
                    SUM(twa_count) as twa,
                    SUM(disabled_count) as disabled,
                    SUM(chronic_illness_count) as chronic_illness,
                    SUM(refugee_count) as refugee,
                    SUM(returnee_count) as returnee,
                    SUM(vulnerable_count) as vulnerable
                FROM dashboard_beneficiary_summary_enhanced
                WHERE 1=1
            """
            
            params = []
            if filters:
                if filters.get('province_id'):
                    query += " AND province_id = %s"
                    params.append(filters['province_id'])
                if filters.get('community_type'):
                    query += " AND community_type = %s"
                    params.append(filters['community_type'])
                if filters.get('benefit_plan_code'):
                    query += " AND benefit_plan_code = %s"
                    params.append(filters['benefit_plan_code'])
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            if result:
                total = result[0] or 0
                data = {
                    'total': total,
                    'male': result[1] or 0,
                    'female': result[2] or 0,
                    'twa_count': result[3] or 0,
                    'disabled_count': result[4] or 0,
                    'chronic_illness_count': result[5] or 0,
                    'refugee_count': result[6] or 0,
                    'returnee_count': result[7] or 0,
                    'total_vulnerable': result[8] or 0,
                    # Calculate percentages
                    'twa_percentage': (result[3] / total * 100) if total > 0 else 0,
                    'disabled_percentage': (result[4] / total * 100) if total > 0 else 0,
                    'chronic_illness_percentage': (result[5] / total * 100) if total > 0 else 0,
                    'refugee_percentage': (result[6] / total * 100) if total > 0 else 0,
                    'returnee_percentage': (result[7] / total * 100) if total > 0 else 0,
                    'vulnerable_percentage': (result[8] / total * 100) if total > 0 else 0,
                }
                
                cache.set(cache_key, data, 300)  # Cache for 5 minutes
                return data
        
        return None
    
    @staticmethod
    def get_vulnerable_groups_details(filters=None):
        """Get detailed vulnerable groups data from dedicated view"""
        cache_key = f"vulnerable_groups_details_{json.dumps(filters or {})}"
        cached_data = cache.get(cache_key)
        
        if cached_data:
            return cached_data
        
        with connection.cursor() as cursor:
            query = """
                SELECT 
                    SUM(total_households) as total_households,
                    SUM(total_members) as total_members,
                    SUM(total_beneficiaries) as total_beneficiaries,
                    -- Twa
                    SUM(twa_households) as twa_households,
                    SUM(twa_members) as twa_members,
                    SUM(twa_beneficiaries) as twa_beneficiaries,
                    -- Disability
                    SUM(disabled_households) as disabled_households,
                    SUM(disabled_members) as disabled_members,
                    SUM(disabled_beneficiaries) as disabled_beneficiaries,
                    -- Chronic illness
                    SUM(chronic_illness_households) as chronic_illness_households,
                    SUM(chronic_illness_members) as chronic_illness_members,
                    SUM(chronic_illness_beneficiaries) as chronic_illness_beneficiaries,
                    -- Refugee
                    SUM(refugee_households) as refugee_households,
                    SUM(refugee_members) as refugee_members,
                    SUM(refugee_beneficiaries) as refugee_beneficiaries,
                    -- Returnee
                    SUM(returnee_households) as returnee_households,
                    SUM(returnee_members) as returnee_members,
                    SUM(returnee_beneficiaries) as returnee_beneficiaries,
                    -- Disability types
                    SUM(physical_disability_count) as physical_disability,
                    SUM(mental_disability_count) as mental_disability,
                    SUM(visual_disability_count) as visual_disability,
                    SUM(hearing_disability_count) as hearing_disability
                FROM dashboard_vulnerable_groups_summary
                WHERE 1=1
            """
            
            params = []
            if filters:
                if filters.get('province_id'):
                    query += " AND province_id = %s"
                    params.append(filters['province_id'])
                if filters.get('benefit_plan_code'):
                    query += " AND benefit_plan_code = %s"
                    params.append(filters['benefit_plan_code'])
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            if result:
                data = {
                    # Households
                    'twa_households': result[3] or 0,
                    'disabled_households': result[6] or 0,
                    'chronic_illness_households': result[9] or 0,
                    'refugee_households': result[12] or 0,
                    'returnee_households': result[15] or 0,
                    # Members
                    'twa_members': result[4] or 0,
                    'disabled_members': result[7] or 0,
                    'chronic_illness_members': result[10] or 0,
                    'refugee_members': result[13] or 0,
                    'returnee_members': result[16] or 0,
                    # Beneficiaries
                    'twa_beneficiaries': result[5] or 0,
                    'disabled_beneficiaries': result[8] or 0,
                    'chronic_illness_beneficiaries': result[11] or 0,
                    'refugee_beneficiaries': result[14] or 0,
                    'returnee_beneficiaries': result[17] or 0,
                    # Disability types
                    'physical_disability_count': result[18] or 0,
                    'mental_disability_count': result[19] or 0,
                    'visual_disability_count': result[20] or 0,
                    'hearing_disability_count': result[21] or 0,
                    # Coverage rates
                    'twa_coverage': (result[5] / result[4] * 100) if result[4] > 0 else 0,
                    'disabled_coverage': (result[8] / result[7] * 100) if result[7] > 0 else 0,
                    'chronic_illness_coverage': (result[11] / result[10] * 100) if result[10] > 0 else 0,
                    'refugee_coverage': (result[14] / result[13] * 100) if result[13] > 0 else 0,
                    'returnee_coverage': (result[17] / result[16] * 100) if result[16] > 0 else 0,
                }
                
                cache.set(cache_key, data, 300)  # Cache for 5 minutes
                return data
        
        return None
    


# GraphQL Query definitions
class VulnerableGroupsQuery(graphene.ObjectType):
    """GraphQL queries for vulnerable groups data"""
    
    vulnerable_groups_summary = graphene.Field(
        VulnerableGroupCountsType,
        province_id=graphene.Int(),
        community_type=graphene.String(),
        benefit_plan_code=graphene.String()
    )
    
    vulnerable_groups_details = graphene.Field(
        VulnerableGroupDetailsType,
        province_id=graphene.Int(),
        benefit_plan_code=graphene.String()
    )
    
    
    enhanced_beneficiary_breakdown = graphene.Field(
        EnhancedBeneficiaryBreakdownType,
        province_id=graphene.Int(),
        community_type=graphene.String(),
        benefit_plan_code=graphene.String()
    )
    
    def resolve_vulnerable_groups_summary(self, info, **kwargs):
        """Resolve vulnerable groups summary data"""
        return VulnerableGroupsService.get_vulnerable_groups_summary(kwargs)
    
    def resolve_vulnerable_groups_details(self, info, **kwargs):
        """Resolve vulnerable groups detailed data"""
        return VulnerableGroupsService.get_vulnerable_groups_details(kwargs)
    
    
    def resolve_enhanced_beneficiary_breakdown(self, info, **kwargs):
        """Resolve enhanced beneficiary breakdown with vulnerable groups"""
        summary = VulnerableGroupsService.get_vulnerable_groups_summary(kwargs)
        details = VulnerableGroupsService.get_vulnerable_groups_details(kwargs)
        
        if summary and details:
            return {
                'gender_breakdown': {
                    'male': summary.get('male', 0),
                    'female': summary.get('female', 0),
                    'twa': summary.get('twa_count', 0),
                    'disabled': summary.get('disabled_count', 0),
                    'chronic_illness': summary.get('chronic_illness_count', 0),
                    'total': summary.get('total', 0),
                    'male_percentage': (summary.get('male', 0) / summary.get('total', 1) * 100) if summary.get('total', 0) > 0 else 0,
                    'female_percentage': (summary.get('female', 0) / summary.get('total', 1) * 100) if summary.get('total', 0) > 0 else 0,
                    'twa_percentage': summary.get('twa_percentage', 0),
                    'disabled_percentage': summary.get('disabled_percentage', 0),
                    'chronic_illness_percentage': summary.get('chronic_illness_percentage', 0),
                },
                'vulnerable_groups': summary,
                'vulnerable_details': details,
                'last_updated': datetime.now().isoformat()
            }
        
        return None