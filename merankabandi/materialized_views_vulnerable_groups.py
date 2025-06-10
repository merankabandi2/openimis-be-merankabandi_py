"""
Enhanced Materialized Views with Vulnerable Groups Reporting
Includes: Twa/Batwa, People with disabilities, Refugees (at group level)
Note: Returnee data not found in current database schema
"""

from django.db import models
from django.core.management.sql import emit_post_migrate_signal
from django.db.models.signals import post_migrate
from django.dispatch import receiver
from django.db import connection

# Enhanced SQL for materialized views with vulnerable groups data
ENHANCED_MATERIALIZED_VIEWS_SQL = {
    
    # 1. ENHANCED BENEFICIARY SUMMARY WITH VULNERABLE GROUPS
    'dashboard_beneficiary_summary_enhanced': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_beneficiary_summary_enhanced AS
        SELECT 
            -- Time dimensions
            DATE_TRUNC('month', gb."DateCreated") as month,
            DATE_TRUNC('quarter', gb."DateCreated") as quarter,
            EXTRACT(year FROM gb."DateCreated") as year,
            
            -- Location dimensions
            l3."LocationName" as province,
            l2."LocationName" as commune,
            l1."LocationName" as colline,
            l3."LocationId" as province_id,
            l2."LocationId" as commune_id,
            l1."LocationId" as colline_id,
            
            -- Community type from group level fields
            CASE 
                WHEN g."Json_ext"->>'menage_refugie' = 'OUI' THEN 'REFUGEE'
                WHEN g."Json_ext"->>'menage_rapatrie' = 'OUI' THEN 'RETURNEE'
                WHEN g."Json_ext"->>'menage_deplace' = 'OUI' THEN 'DISPLACED'
                ELSE 'HOST'
            END as community_type,
            
            -- Demographic dimensions
            COALESCE(i."Json_ext"->>'sexe', 'UNKNOWN') as gender,
            
            -- Vulnerable groups flags
            CASE WHEN g."Json_ext"->>'menage_mutwa' = 'OUI' THEN true ELSE false END as is_twa,
            CASE 
                WHEN i."Json_ext"->>'handicap' = 'OUI' THEN true 
                WHEN i."Json_ext"->>'handicap' = 'NON' THEN false
                ELSE NULL 
            END as has_disability,
            COALESCE(i."Json_ext"->>'type_handicap', '') as disability_type,
            CASE 
                WHEN i."Json_ext"->>'maladie_chro' = 'OUI' THEN true 
                WHEN i."Json_ext"->>'maladie_chro' = 'NON' THEN false
                ELSE NULL 
            END as has_chronic_illness,
            COALESCE(i."Json_ext"->>'maladie_chro_type', '') as chronic_illness_type,
            
            -- Age calculations
            CASE 
                WHEN i.dob IS NULL THEN 'UNKNOWN'
                WHEN DATE_PART('year', AGE(i.dob)) < 18 THEN 'UNDER_18'
                WHEN DATE_PART('year', AGE(i.dob)) BETWEEN 18 AND 35 THEN 'ADULT_18_35'
                WHEN DATE_PART('year', AGE(i.dob)) BETWEEN 36 AND 60 THEN 'ADULT_36_60'
                ELSE 'OVER_60'
            END as age_group,
            
            -- Benefit plan information
            bp.code as benefit_plan_code,
            bp.name as benefit_plan_name,
            
            -- Special benefit plan categories
            CASE 
                WHEN bp.code LIKE 'TMR%' THEN 'REFUGEE_TRANSFER'
                WHEN bp.code LIKE 'TMO%' THEN 'ORDINARY_TRANSFER'
                WHEN bp.code LIKE 'TMU-C%' THEN 'CLIMATE_EMERGENCY'
                WHEN bp.code LIKE 'TMU-CERC%' THEN 'CERC_EMERGENCY'
                ELSE 'OTHER'
            END as benefit_category,
            
            -- Status tracking
            gb.status,
            gb."isDeleted",
            
            -- Enhanced aggregated metrics
            COUNT(*) as beneficiary_count,
            COUNT(CASE WHEN i."Json_ext"->>'sexe' = 'M' THEN 1 END) as male_count,
            COUNT(CASE WHEN i."Json_ext"->>'sexe' = 'F' THEN 1 END) as female_count,
            
            -- Vulnerable groups counts
            COUNT(CASE WHEN g."Json_ext"->>'menage_mutwa' = 'OUI' THEN 1 END) as twa_count,
            COUNT(CASE WHEN i."Json_ext"->>'handicap' = 'OUI' THEN 1 END) as disabled_count,
            COUNT(CASE WHEN i."Json_ext"->>'maladie_chro' = 'OUI' THEN 1 END) as chronic_illness_count,
            COUNT(CASE WHEN g."Json_ext"->>'menage_refugie' = 'OUI' THEN 1 END) as refugee_count,
            COUNT(CASE WHEN g."Json_ext"->>'menage_rapatrie' = 'OUI' THEN 1 END) as returnee_count,
            COUNT(CASE WHEN g."Json_ext"->>'menage_deplace' = 'OUI' THEN 1 END) as displaced_count,
            
            -- Combined vulnerable groups
            COUNT(CASE WHEN g."Json_ext"->>'menage_mutwa' = 'OUI' 
                       OR i."Json_ext"->>'handicap' = 'OUI' 
                       OR i."Json_ext"->>'maladie_chro' = 'OUI'
                       OR g."Json_ext"->>'menage_refugie' = 'OUI'
                       OR g."Json_ext"->>'menage_rapatrie' = 'OUI'
                       OR g."Json_ext"->>'menage_deplace' = 'OUI' THEN 1 END) as vulnerable_count,
            
            -- Status counts
            COUNT(CASE WHEN gb.status = 'ACTIVE' THEN 1 END) as active_count,
            COUNT(CASE WHEN gb.status = 'SUSPENDED' THEN 1 END) as suspended_count
            
        FROM social_protection_groupbeneficiary gb
        JOIN social_protection_benefitplan bp ON gb.benefit_plan_id = bp."UUID"
        JOIN individual_group g ON gb.group_id = g."UUID"
        JOIN individual_groupindividual gi ON gi.group_id = gb.group_id
        JOIN individual_individual i ON gi.individual_id = i."UUID"
        LEFT JOIN "tblLocations" l1 ON i.location_id = l1."LocationId"
        LEFT JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
        LEFT JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
        WHERE gb."isDeleted" = false
        GROUP BY 
            month, quarter, year,
            province, commune, colline, province_id, commune_id, colline_id,
            community_type, gender, is_twa, has_disability, disability_type, has_chronic_illness, chronic_illness_type, age_group,
            bp.code, bp.name, benefit_category, gb.status, gb."isDeleted";
    """,
    
    # 2. VULNERABLE GROUPS SUMMARY VIEW
    'dashboard_vulnerable_groups_summary': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_vulnerable_groups_summary AS
        SELECT 
            -- Location
            l3."LocationName" as province,
            l3."LocationId" as province_id,
            
            -- Group type from household data
            g."Json_ext"->>'type_menage' as household_type,
            
            -- Benefit plan
            bp.code as benefit_plan_code,
            bp.name as benefit_plan_name,
            
            -- Vulnerable groups totals
            COUNT(DISTINCT g."UUID") as total_households,
            COUNT(DISTINCT i."UUID") as total_members,
            COUNT(DISTINCT CASE WHEN gi.recipient_type = 'PRIMARY' THEN i."UUID" END) as total_beneficiaries,
            
            -- Twa/Batwa counts (based on household/group level)
            COUNT(DISTINCT CASE WHEN g."Json_ext"->>'menage_mutwa' = 'OUI' THEN g."UUID" END) as twa_households,
            COUNT(DISTINCT CASE WHEN g."Json_ext"->>'menage_mutwa' = 'OUI' THEN i."UUID" END) as twa_members,
            COUNT(DISTINCT CASE WHEN g."Json_ext"->>'menage_mutwa' = 'OUI' AND gi.recipient_type = 'PRIMARY' THEN i."UUID" END) as twa_beneficiaries,
            
            -- Disability counts  
            COUNT(DISTINCT CASE WHEN EXISTS (
                SELECT 1 FROM individual_groupindividual gi2
                JOIN individual_individual i2 ON gi2.individual_id = i2."UUID"
                WHERE gi2.group_id = g."UUID" AND i2."Json_ext"->>'handicap' = 'OUI'
            ) THEN g."UUID" END) as disabled_households,
            COUNT(DISTINCT CASE WHEN i."Json_ext"->>'handicap' = 'OUI' THEN i."UUID" END) as disabled_members,
            COUNT(DISTINCT CASE WHEN i."Json_ext"->>'handicap' = 'OUI' AND gi.recipient_type = 'PRIMARY' THEN i."UUID" END) as disabled_beneficiaries,
            
            -- Chronic illness counts
            COUNT(DISTINCT CASE WHEN EXISTS (
                SELECT 1 FROM individual_groupindividual gi2
                JOIN individual_individual i2 ON gi2.individual_id = i2."UUID"
                WHERE gi2.group_id = g."UUID" AND i2."Json_ext"->>'maladie_chro' = 'OUI'
            ) THEN g."UUID" END) as chronic_illness_households,
            COUNT(DISTINCT CASE WHEN i."Json_ext"->>'maladie_chro' = 'OUI' THEN i."UUID" END) as chronic_illness_members,
            COUNT(DISTINCT CASE WHEN i."Json_ext"->>'maladie_chro' = 'OUI' AND gi.recipient_type = 'PRIMARY' THEN i."UUID" END) as chronic_illness_beneficiaries,
            
            -- Refugee counts (based on group menage_refugie field)
            COUNT(DISTINCT CASE WHEN g."Json_ext"->>'menage_refugie' = 'OUI' THEN g."UUID" END) as refugee_households,
            COUNT(DISTINCT CASE WHEN g."Json_ext"->>'menage_refugie' = 'OUI' THEN i."UUID" END) as refugee_members,
            COUNT(DISTINCT CASE WHEN g."Json_ext"->>'menage_refugie' = 'OUI' AND gi.recipient_type = 'PRIMARY' THEN i."UUID" END) as refugee_beneficiaries,
            
            -- Returnee counts (based on group menage_rapatrie field)
            COUNT(DISTINCT CASE WHEN g."Json_ext"->>'menage_rapatrie' = 'OUI' THEN g."UUID" END) as returnee_households,
            COUNT(DISTINCT CASE WHEN g."Json_ext"->>'menage_rapatrie' = 'OUI' THEN i."UUID" END) as returnee_members,
            COUNT(DISTINCT CASE WHEN g."Json_ext"->>'menage_rapatrie' = 'OUI' AND gi.recipient_type = 'PRIMARY' THEN i."UUID" END) as returnee_beneficiaries,
            
            -- Displaced counts (based on group menage_deplace field)
            COUNT(DISTINCT CASE WHEN g."Json_ext"->>'menage_deplace' = 'OUI' THEN g."UUID" END) as displaced_households,
            COUNT(DISTINCT CASE WHEN g."Json_ext"->>'menage_deplace' = 'OUI' THEN i."UUID" END) as displaced_members,
            COUNT(DISTINCT CASE WHEN g."Json_ext"->>'menage_deplace' = 'OUI' AND gi.recipient_type = 'PRIMARY' THEN i."UUID" END) as displaced_beneficiaries,
            
            -- Disability types breakdown
            COUNT(DISTINCT CASE WHEN i."Json_ext"->>'type_handicap' LIKE '%physique%' THEN i."UUID" END) as physical_disability_count,
            COUNT(DISTINCT CASE WHEN i."Json_ext"->>'type_handicap' LIKE '%mental%' THEN i."UUID" END) as mental_disability_count,
            COUNT(DISTINCT CASE WHEN i."Json_ext"->>'type_handicap' LIKE '%visuel%' THEN i."UUID" END) as visual_disability_count,
            COUNT(DISTINCT CASE WHEN i."Json_ext"->>'type_handicap' LIKE '%auditif%' THEN i."UUID" END) as hearing_disability_count,
            
            -- Current date for freshness
            CURRENT_DATE as report_date
            
        FROM social_protection_groupbeneficiary gb
        JOIN social_protection_benefitplan bp ON gb.benefit_plan_id = bp."UUID"
        JOIN individual_group g ON gb.group_id = g."UUID"
        JOIN individual_groupindividual gi ON gi.group_id = g."UUID"
        JOIN individual_individual i ON gi.individual_id = i."UUID"
        LEFT JOIN "tblLocations" l1 ON g.location_id = l1."LocationId"
        LEFT JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
        LEFT JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
        WHERE gb."isDeleted" = false AND gb.status = 'ACTIVE'
        GROUP BY 
            province, province_id, household_type,
            bp.code, bp.name, report_date;
    """,
    
    # 3. ENHANCED MASTER SUMMARY WITH VULNERABLE GROUPS
    'dashboard_master_summary_enhanced': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_master_summary_enhanced AS
        SELECT 
            'MASTER_SUMMARY' as summary_type,
            
            -- Total beneficiaries
            (SELECT COUNT(*) FROM social_protection_groupbeneficiary WHERE "isDeleted" = false AND status = 'ACTIVE') as total_beneficiaries,
            (SELECT COUNT(DISTINCT group_id) FROM social_protection_groupbeneficiary WHERE "isDeleted" = false AND status = 'ACTIVE') as total_households,
            
            -- Gender breakdown
            (SELECT COUNT(*) FROM social_protection_groupbeneficiary gb
             JOIN individual_groupindividual gi ON gi.group_id = gb.group_id
             JOIN individual_individual i ON gi.individual_id = i."UUID"
             WHERE gb."isDeleted" = false AND gb.status = 'ACTIVE' AND i."Json_ext"->>'sexe' = 'M') as total_male,
            
            (SELECT COUNT(*) FROM social_protection_groupbeneficiary gb
             JOIN individual_groupindividual gi ON gi.group_id = gb.group_id
             JOIN individual_individual i ON gi.individual_id = i."UUID"
             WHERE gb."isDeleted" = false AND gb.status = 'ACTIVE' AND i."Json_ext"->>'sexe' = 'F') as total_female,
            
            -- Vulnerable groups totals
            (SELECT COUNT(DISTINCT i."UUID") FROM social_protection_groupbeneficiary gb
             JOIN individual_group g ON gb.group_id = g."UUID"
             JOIN individual_groupindividual gi ON gi.group_id = gb.group_id
             JOIN individual_individual i ON gi.individual_id = i."UUID"
             WHERE gb."isDeleted" = false AND gb.status = 'ACTIVE' AND g."Json_ext"->>'menage_mutwa' = 'OUI') as total_twa,
            
            (SELECT COUNT(DISTINCT i."UUID") FROM social_protection_groupbeneficiary gb
             JOIN individual_groupindividual gi ON gi.group_id = gb.group_id
             JOIN individual_individual i ON gi.individual_id = i."UUID"
             WHERE gb."isDeleted" = false AND gb.status = 'ACTIVE' AND i."Json_ext"->>'handicap' = 'OUI') as total_disabled,
            
            (SELECT COUNT(DISTINCT i."UUID") FROM social_protection_groupbeneficiary gb
             JOIN individual_groupindividual gi ON gi.group_id = gb.group_id
             JOIN individual_individual i ON gi.individual_id = i."UUID"
             WHERE gb."isDeleted" = false AND gb.status = 'ACTIVE' AND i."Json_ext"->>'maladie_chro' = 'OUI') as total_chronic_illness,
            
            (SELECT COUNT(DISTINCT i."UUID") FROM social_protection_groupbeneficiary gb
             JOIN individual_groupindividual gi ON gi.group_id = gb.group_id
             JOIN individual_individual i ON gi.individual_id = i."UUID"
             JOIN individual_group g ON gi.group_id = g."UUID"
             WHERE gb."isDeleted" = false AND gb.status = 'ACTIVE' AND g."Json_ext"->>'menage_refugie' = 'OUI') as total_refugees,
            
            (SELECT COUNT(DISTINCT i."UUID") FROM social_protection_groupbeneficiary gb
             JOIN individual_groupindividual gi ON gi.group_id = gb.group_id
             JOIN individual_individual i ON gi.individual_id = i."UUID"
             JOIN individual_group g ON gi.group_id = g."UUID"
             WHERE gb."isDeleted" = false AND gb.status = 'ACTIVE' AND g."Json_ext"->>'menage_rapatrie' = 'OUI') as total_returnees,
            
            (SELECT COUNT(DISTINCT i."UUID") FROM social_protection_groupbeneficiary gb
             JOIN individual_groupindividual gi ON gi.group_id = gb.group_id
             JOIN individual_individual i ON gi.individual_id = i."UUID"
             JOIN individual_group g ON gi.group_id = g."UUID"
             WHERE gb."isDeleted" = false AND gb.status = 'ACTIVE' AND g."Json_ext"->>'menage_deplace' = 'OUI') as total_displaced,
            
            -- Vulnerable households
            (SELECT COUNT(DISTINCT g."UUID") FROM social_protection_groupbeneficiary gb
             JOIN individual_group g ON gb.group_id = g."UUID"
             WHERE gb."isDeleted" = false AND gb.status = 'ACTIVE' 
             AND g."Json_ext"->>'menage_mutwa' = 'OUI') as twa_households,
            
            (SELECT COUNT(DISTINCT g."UUID") FROM social_protection_groupbeneficiary gb
             JOIN individual_group g ON gb.group_id = g."UUID"
             WHERE gb."isDeleted" = false AND gb.status = 'ACTIVE' 
             AND EXISTS (
                 SELECT 1 FROM individual_groupindividual gi2
                 JOIN individual_individual i2 ON gi2.individual_id = i2."UUID"
                 WHERE gi2.group_id = g."UUID" AND i2."Json_ext"->>'handicap' = 'OUI'
             )) as disabled_households,
            
            (SELECT COUNT(DISTINCT g."UUID") FROM social_protection_groupbeneficiary gb
             JOIN individual_group g ON gb.group_id = g."UUID"
             WHERE gb."isDeleted" = false AND gb.status = 'ACTIVE' 
             AND EXISTS (
                 SELECT 1 FROM individual_groupindividual gi2
                 JOIN individual_individual i2 ON gi2.individual_id = i2."UUID"
                 WHERE gi2.group_id = g."UUID" AND i2."Json_ext"->>'maladie_chro' = 'OUI'
             )) as chronic_illness_households,
            
            
            -- Activities participation
            (SELECT SUM(twa_participants) FROM merankabandi_sensitizationtraining) +
            (SELECT SUM(twa_participants) FROM merankabandi_behaviorchangepromotion) as total_twa_activity_participants,
            
            -- Timestamp
            CURRENT_TIMESTAMP as last_updated;
    """
}

# Create indexes for better query performance on vulnerable groups
VULNERABLE_GROUPS_INDEXES = [
    # Individual json_ext indexes for vulnerable group queries
    "CREATE INDEX IF NOT EXISTS idx_individual_handicap ON individual_individual ((\"Json_ext\"->>'handicap'));",
    "CREATE INDEX IF NOT EXISTS idx_individual_type_handicap ON individual_individual ((\"Json_ext\"->>'type_handicap'));",
    "CREATE INDEX IF NOT EXISTS idx_individual_maladie_chro ON individual_individual ((\"Json_ext\"->>'maladie_chro'));",
    "CREATE INDEX IF NOT EXISTS idx_individual_maladie_chro_type ON individual_individual ((\"Json_ext\"->>'maladie_chro_type'));",
    
    # Group json_ext indexes for household vulnerable status fields
    "CREATE INDEX IF NOT EXISTS idx_group_menage_mutwa ON individual_group ((\"Json_ext\"->>'menage_mutwa'));",
    "CREATE INDEX IF NOT EXISTS idx_group_menage_refugie ON individual_group ((\"Json_ext\"->>'menage_refugie'));",
    "CREATE INDEX IF NOT EXISTS idx_group_menage_rapatrie ON individual_group ((\"Json_ext\"->>'menage_rapatrie'));",
    "CREATE INDEX IF NOT EXISTS idx_group_menage_deplace ON individual_group ((\"Json_ext\"->>'menage_deplace'));",
    
    # Composite indexes for common query patterns
    "CREATE INDEX IF NOT EXISTS idx_beneficiary_vulnerable ON individual_individual (location_id) WHERE \"Json_ext\"->>'handicap' = 'OUI' OR \"Json_ext\"->>'maladie_chro' = 'OUI';",
]


class MaterializedViewManager:
    @staticmethod
    def create_or_replace_view(view_name, view_sql):
        """Create or replace a materialized view"""
        with connection.cursor() as cursor:
            try:
                # Drop existing view if it exists
                cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")
                # Create new view
                cursor.execute(view_sql)
                
                # Create indexes based on which columns exist
                # Check which columns exist in the view
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                """, [view_name])
                columns = [row[0] for row in cursor.fetchall()]
                
                # Create indexes for columns that exist
                if 'month' in columns:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{view_name}_month ON {view_name} (month);")
                if 'year' in columns:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{view_name}_year ON {view_name} (year);")
                if 'province_id' in columns:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{view_name}_province ON {view_name} (province_id);")
                if 'benefit_plan_code' in columns:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{view_name}_benefit_plan ON {view_name} (benefit_plan_code);")
                
                print(f"Successfully created materialized view: {view_name}")
            except Exception as e:
                print(f"Error creating materialized view {view_name}: {e}")
                raise

    @staticmethod
    def refresh_view(view_name, concurrently=True):
        """Refresh a materialized view"""
        with connection.cursor() as cursor:
            try:
                refresh_sql = f"REFRESH MATERIALIZED VIEW {'CONCURRENTLY' if concurrently else ''} {view_name};"
                cursor.execute(refresh_sql)
                print(f"Successfully refreshed materialized view: {view_name}")
            except Exception as e:
                print(f"Error refreshing materialized view {view_name}: {e}")
                raise

    @staticmethod
    def create_all_views():
        """Create all enhanced materialized views with vulnerable groups"""
        for view_name, view_sql in ENHANCED_MATERIALIZED_VIEWS_SQL.items():
            MaterializedViewManager.create_or_replace_view(view_name, view_sql)
        
        # Create additional indexes for vulnerable groups
        with connection.cursor() as cursor:
            for index_sql in VULNERABLE_GROUPS_INDEXES:
                try:
                    cursor.execute(index_sql)
                    print(f"Created index: {index_sql[:50]}...")
                except Exception as e:
                    print(f"Error creating index: {e}")

    @staticmethod
    def refresh_all_views(concurrently=True):
        """Refresh all enhanced materialized views"""
        for view_name in ENHANCED_MATERIALIZED_VIEWS_SQL.keys():
            MaterializedViewManager.refresh_view(view_name, concurrently)


# Signal handler to create views after migrations
@receiver(post_migrate)
def create_materialized_views(sender, **kwargs):
    if sender.name == 'merankabandi':
        print("Creating enhanced materialized views with vulnerable groups...")
        MaterializedViewManager.create_all_views()