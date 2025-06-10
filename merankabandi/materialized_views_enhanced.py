"""
Enhanced Materialized Views with Comprehensive JSON_ext Field Dimensions
"""

DASHBOARD_ENHANCED_BENEFICIARY_VIEW = """
-- Enhanced beneficiary view with all dimensions from JSON_ext fields
CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_beneficiary_enhanced AS
WITH household_data AS (
    SELECT 
        g."UUID" as household_id,
        g."Json_ext"->>'social_id' as social_id,
        g.location_id,
        l1."LocationName" as colline,
        l2."LocationName" as zone,
        l3."LocationName" as commune,
        l4."LocationName" as province,
        l4."LocationId" as province_id,
        
        -- Household demographic dimensions
        g."Json_ext"->>'type_menage' as household_type,
        g."Json_ext"->>'vulnerable_ressenti' as vulnerability_level,
        g."Json_ext"->>'etat' as household_status,
        g."Json_ext"->>'milieu_residence' as residence_type,
        CAST(NULLIF(g."Json_ext"->>'score_pmt_initial', '') AS FLOAT) as pmt_score,
        CASE 
            WHEN CAST(NULLIF(g."Json_ext"->>'score_pmt_initial', '') AS FLOAT) < 30 THEN '<30'
            WHEN CAST(NULLIF(g."Json_ext"->>'score_pmt_initial', '') AS FLOAT) < 40 THEN '30-40'
            WHEN CAST(NULLIF(g."Json_ext"->>'score_pmt_initial', '') AS FLOAT) < 50 THEN '40-50'
            ELSE '>50'
        END as pmt_score_range,
        
        -- Vulnerable groups
        CASE WHEN g."Json_ext"->>'menage_mutwa' = 'OUI' THEN true ELSE false END as is_twa_household,
        CASE WHEN g."Json_ext"->>'menage_refugie' = 'OUI' THEN true ELSE false END as is_refugee_household,
        CASE WHEN g."Json_ext"->>'menage_rapatrie' = 'OUI' THEN true ELSE false END as is_returnee_household,
        CASE WHEN g."Json_ext"->>'menage_deplace' = 'OUI' THEN true ELSE false END as is_displaced_household,
        
        -- Housing conditions
        g."Json_ext"->>'logement_type' as housing_type,
        g."Json_ext"->>'logement_statut' as housing_ownership,
        g."Json_ext"->>'logement_murs' as housing_walls,
        g."Json_ext"->>'logement_toit' as housing_roof,
        g."Json_ext"->>'logement_sol' as housing_floor,
        g."Json_ext"->>'logement_eau_boisson' as water_source,
        g."Json_ext"->>'logement_cuisson' as cooking_fuel,
        g."Json_ext"->>'logement_electricite' as electricity_source,
        g."Json_ext"->>'logement_toilettes' as toilet_type,
        CAST(NULLIF(g."Json_ext"->>'logement_pieces', '') AS INTEGER) as num_rooms,
        
        -- Economic indicators
        CASE WHEN g."Json_ext"->>'a_terres' = 'OUI' THEN true ELSE false END as has_land,
        CASE WHEN g."Json_ext"->>'a_elevage' = 'OUI' THEN true ELSE false END as has_livestock,
        CASE WHEN g."Json_ext"->>'transfert_recoit' = 'OUI' THEN true ELSE false END as receives_transfers,
        
        -- Food security
        CASE WHEN g."Json_ext"->>'alimentaire_sans_nourriture' = 'ALIMENTAIRE_SANS_NOURRITURE_OUI' THEN true ELSE false END as food_insecure,
        g."Json_ext"->>'alimentaire_frequence' as food_shortage_frequency,
        
        -- Assets
        CAST(NULLIF(g."Json_ext"->>'possessions_radio', '') AS INTEGER) > 0 as has_radio,
        CAST(NULLIF(g."Json_ext"->>'possessions_smartphone', '') AS INTEGER) > 0 as has_smartphone,
        CAST(NULLIF(g."Json_ext"->>'possessions_velo', '') AS INTEGER) > 0 as has_bicycle,
        CAST(NULLIF(g."Json_ext"->>'possessions_tele', '') AS INTEGER) > 0 as has_television
        
    FROM individual_group g
    LEFT JOIN "tblLocations" l1 ON g.location_id = l1."LocationId"
    LEFT JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
    LEFT JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
    LEFT JOIN "tblLocations" l4 ON l3."ParentLocationId" = l4."LocationId"
    WHERE g."isDeleted" = false
),
individual_data AS (
    SELECT
        i."UUID" as individual_id,
        i.first_name,
        i.last_name,
        i.other_names,
        i.dob as birth_date,
        i."Json_ext"->>'social_id' as individual_social_id,
        
        -- Demographics
        i."Json_ext"->>'sexe' as gender,
        CASE 
            WHEN EXTRACT(YEAR FROM AGE(i.dob)) < 5 THEN '0-5'
            WHEN EXTRACT(YEAR FROM AGE(i.dob)) < 18 THEN '5-18'
            WHEN EXTRACT(YEAR FROM AGE(i.dob)) < 60 THEN '18-60'
            ELSE '60+'
        END as age_group,
        
        -- Disability and health
        CASE WHEN i."Json_ext"->>'handicap' = 'OUI' THEN true ELSE false END as has_disability,
        i."Json_ext"->>'type_handicap' as disability_type,
        CASE WHEN i."Json_ext"->>'maladie_chro' = 'OUI' THEN true ELSE false END as has_chronic_disease,
        i."Json_ext"->>'maladie_chro_type' as chronic_disease_type,
        
        -- Education
        i."Json_ext"->>'instruction' as education_level,
        CASE WHEN i."Json_ext"->>'lit' = 'OUI' THEN true ELSE false END as is_literate,
        CASE WHEN i."Json_ext"->>'va_ecole' = 'OUI' THEN true ELSE false END as attending_school,
        
        -- Household role
        CASE WHEN i."Json_ext"->>'est_chef' = 'OUI' THEN true ELSE false END as is_household_head,
        i."Json_ext"->>'lien' as relationship_to_head,
        
        -- Activity and civil status
        i."Json_ext"->>'activite' as main_activity,
        CASE WHEN i."Json_ext"->>'etat_civil' = 'OUI' THEN true ELSE false END as civil_status_documented,
        i."Json_ext"->>'statut_matrimonial' as marital_status,
        
        -- Contact
        i."Json_ext"->>'telephone_etat' as phone_status,
        
        -- Link to household (assuming social_id structure)
        SUBSTRING(i."Json_ext"->>'social_id', 1, LENGTH(i."Json_ext"->>'social_id') - 3) as household_social_id
        
    FROM individual_individual i
    WHERE i."isDeleted" = false
)
SELECT 
    -- Individual information
    ind.*,
    
    -- Household information
    hh.household_id,
    hh.province,
    hh.province_id,
    hh.commune,
    hh.zone,
    hh.colline,
    hh.location_id,
    hh.household_type,
    hh.vulnerability_level,
    hh.household_status,
    hh.residence_type,
    hh.pmt_score,
    hh.pmt_score_range,
    
    -- Vulnerable groups (combined)
    hh.is_twa_household as is_batwa,
    hh.is_refugee_household as is_refugee,
    hh.is_returnee_household as is_returnee,
    hh.is_displaced_household as is_displaced,
    CASE 
        WHEN hh.is_refugee_household THEN 'REFUGEE'
        WHEN hh.is_returnee_household THEN 'RETURNEE'
        WHEN hh.is_displaced_household THEN 'DISPLACED'
        ELSE 'HOST'
    END as community_type,
    
    -- Housing conditions
    hh.housing_type,
    hh.housing_ownership,
    hh.housing_walls,
    hh.housing_roof,
    hh.housing_floor,
    hh.water_source,
    hh.cooking_fuel,
    hh.electricity_source,
    hh.toilet_type,
    hh.num_rooms,
    
    -- Economic indicators
    hh.has_land,
    hh.has_livestock,
    hh.receives_transfers,
    hh.food_insecure,
    hh.food_shortage_frequency,
    
    -- Assets
    hh.has_radio,
    hh.has_smartphone,
    hh.has_bicycle,
    hh.has_television,
    
    -- Beneficiary information
    gb."UUID" as beneficiary_id,
    gb.status as beneficiary_status,
    gb."DateCreated" as date_created,
    gb."DateUpdated" as date_updated,
    bp.name as benefit_plan_name,
    bp.code as benefit_plan_code,
    
    -- Payment information
    gb."Json_ext"->'moyen_paiement'->>'agence' as payment_method,
    gb."Json_ext"->'moyen_paiement'->>'etat' as payment_state,
    gb."Json_ext"->'moyen_paiement'->>'status' as payment_status,
    
    -- Payment tracking
    pay_summary.total_payments,
    pay_summary.last_payment_date,
    
    -- Report metadata
    CURRENT_DATE as report_date

FROM individual_data ind
LEFT JOIN household_data hh ON ind.household_social_id = hh.social_id
LEFT JOIN individual_group g ON g."UUID" = hh.household_id
LEFT JOIN social_protection_groupbeneficiary gb ON gb.group_id = g."UUID" AND gb."isDeleted" = false
LEFT JOIN social_protection_benefitplan bp ON bp."UUID" = gb.benefit_plan_id AND bp."isDeleted" = false
LEFT JOIN LATERAL (
    SELECT 
        COUNT(*) as total_payments,
        MAX(pp."PayDate") as last_payment_date
    FROM payroll_benefitconsumption bc
    JOIN payroll_payroll pp ON pp."UUID" = bc.payroll_id
    WHERE bc.benefit_id = gb."UUID"
      AND bc."isDeleted" = false
      AND pp."isDeleted" = false
      AND pp."PaymentStatus" = 'SUCCESS'
) pay_summary ON true;

-- Create indexes for enhanced beneficiary view
CREATE INDEX IF NOT EXISTS idx_dash_ben_enh_province ON dashboard_beneficiary_enhanced (province);
CREATE INDEX IF NOT EXISTS idx_dash_ben_enh_community ON dashboard_beneficiary_enhanced (community_type);
CREATE INDEX IF NOT EXISTS idx_dash_ben_enh_disability ON dashboard_beneficiary_enhanced (has_disability);
CREATE INDEX IF NOT EXISTS idx_dash_ben_enh_batwa ON dashboard_beneficiary_enhanced (is_batwa);
CREATE INDEX IF NOT EXISTS idx_dash_ben_enh_pmt_range ON dashboard_beneficiary_enhanced (pmt_score_range);
CREATE INDEX IF NOT EXISTS idx_dash_ben_enh_household_type ON dashboard_beneficiary_enhanced (household_type);
CREATE INDEX IF NOT EXISTS idx_dash_ben_enh_education ON dashboard_beneficiary_enhanced (education_level);
CREATE INDEX IF NOT EXISTS idx_dash_ben_enh_benefit_plan ON dashboard_beneficiary_enhanced (benefit_plan_code);
"""

DASHBOARD_HOUSEHOLD_SUMMARY_VIEW = """
-- Household-level summary view with aggregated member information
CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_households AS
WITH household_base AS (
    SELECT 
        g."UUID" as household_id,
        g."Json_ext"->>'social_id' as social_id,
        g.location_id,
        l1."LocationName" as colline,
        l2."LocationName" as zone,
        l3."LocationName" as commune,
        l4."LocationName" as province,
        l4."LocationId" as province_id,
        
        -- Household characteristics
        g."Json_ext"->>'type_menage' as household_type,
        g."Json_ext"->>'vulnerable_ressenti' as vulnerability_level,
        g."Json_ext"->>'etat' as household_status,
        g."Json_ext"->>'milieu_residence' as residence_type,
        CAST(NULLIF(g."Json_ext"->>'score_pmt_initial', '') AS FLOAT) as pmt_score,
        
        -- Vulnerable groups
        CASE WHEN g."Json_ext"->>'menage_mutwa' = 'OUI' THEN true ELSE false END as is_twa_household,
        CASE WHEN g."Json_ext"->>'menage_refugie' = 'OUI' THEN true ELSE false END as is_refugee_household,
        CASE WHEN g."Json_ext"->>'menage_rapatrie' = 'OUI' THEN true ELSE false END as is_returnee_household,
        CASE WHEN g."Json_ext"->>'menage_deplace' = 'OUI' THEN true ELSE false END as is_displaced_household,
        
        -- Housing quality indicators
        g."Json_ext"->>'logement_type' as housing_type,
        g."Json_ext"->>'logement_statut' as housing_ownership,
        CAST(NULLIF(g."Json_ext"->>'logement_pieces', '') AS INTEGER) as num_rooms,
        
        -- Basic services
        CASE 
            WHEN g."Json_ext"->>'logement_eau_boisson' IN ('LOGEMENT_EAU_BOISSON_ROBINET_DOMICILE', 'LOGEMENT_EAU_BOISSON_BORNE_FONTAINE') 
            THEN true ELSE false 
        END as has_improved_water,
        CASE 
            WHEN g."Json_ext"->>'logement_toilettes' IN ('LOGEMENT_TOILETTES_CHASSE_EAU', 'LOGEMENT_TOILETTES_LATRINE_DALLE_BETON') 
            THEN true ELSE false 
        END as has_improved_sanitation,
        CASE 
            WHEN g."Json_ext"->>'logement_electricite' IN ('LOGEMENT_ELECTRICITE_REGIDESO', 'LOGEMENT_ELECTRICITE_PLAQUE_SOLAIRE') 
            THEN true ELSE false 
        END as has_electricity,
        
        -- Economic indicators
        CASE WHEN g."Json_ext"->>'a_terres' = 'OUI' THEN true ELSE false END as has_land,
        CASE WHEN g."Json_ext"->>'a_elevage' = 'OUI' THEN true ELSE false END as has_livestock,
        
        -- Livestock details
        CAST(NULLIF(g."Json_ext"->>'elevage_bovins', '') AS INTEGER) as num_cattle,
        CAST(NULLIF(g."Json_ext"->>'elevage_caprins', '') AS INTEGER) as num_goats,
        CAST(NULLIF(g."Json_ext"->>'elevage_porcins', '') AS INTEGER) as num_pigs,
        CAST(NULLIF(g."Json_ext"->>'elevage_volailles', '') AS INTEGER) as num_chickens,
        
        -- Assets
        CAST(NULLIF(g."Json_ext"->>'possessions_radio', '') AS INTEGER) as num_radios,
        CAST(NULLIF(g."Json_ext"->>'possessions_smartphone', '') AS INTEGER) as num_smartphones,
        CAST(NULLIF(g."Json_ext"->>'possessions_velo', '') AS INTEGER) as num_bicycles,
        CAST(NULLIF(g."Json_ext"->>'possessions_tele', '') AS INTEGER) as num_televisions,
        
        g."DateCreated" as registration_date,
        g."DateUpdated" as last_update_date
        
    FROM individual_group g
    LEFT JOIN "tblLocations" l1 ON g.location_id = l1."LocationId"
    LEFT JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
    LEFT JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
    LEFT JOIN "tblLocations" l4 ON l3."ParentLocationId" = l4."LocationId"
    WHERE g."isDeleted" = false
),
member_aggregates AS (
    SELECT 
        gi.group_id,
        COUNT(DISTINCT gi.individual_id) as household_size,
        COUNT(CASE WHEN i."Json_ext"->>'sexe' = 'F' THEN 1 END) as num_women,
        COUNT(CASE WHEN i."Json_ext"->>'sexe' = 'M' THEN 1 END) as num_men,
        COUNT(CASE WHEN EXTRACT(YEAR FROM AGE(i.dob)) < 18 THEN 1 END) as num_children,
        COUNT(CASE WHEN EXTRACT(YEAR FROM AGE(i.dob)) >= 60 THEN 1 END) as num_elderly,
        COUNT(CASE WHEN i."Json_ext"->>'handicap' = 'OUI' THEN 1 END) as num_disabled,
        COUNT(CASE WHEN i."Json_ext"->>'maladie_chro' = 'OUI' THEN 1 END) as num_chronic_disease
    FROM individual_groupindividual gi
    JOIN individual_individual i ON i."UUID" = gi.individual_id
    WHERE gi."isDeleted" = false
      AND i."isDeleted" = false
    GROUP BY gi.group_id
),
benefit_status AS (
    SELECT 
        gb.group_id,
        bool_or(gb.status = 'ACTIVE') as benefit_plan_enrolled,
        COUNT(bc."UUID") as num_payments_received,
        SUM(bc."Amount") as total_amount_received
    FROM social_protection_groupbeneficiary gb
    LEFT JOIN payroll_benefitconsumption bc ON bc.benefit_id = gb."UUID" AND bc."isDeleted" = false
    WHERE gb."isDeleted" = false
    GROUP BY gb.group_id
)
SELECT 
    hb.*,
    
    -- Member aggregates
    COALESCE(ma.household_size, 0) as household_size,
    COALESCE(ma.num_women, 0) as num_women,
    COALESCE(ma.num_men, 0) as num_men,
    COALESCE(ma.num_children, 0) as num_children,
    COALESCE(ma.num_elderly, 0) as num_elderly,
    COALESCE(ma.num_disabled, 0) as num_disabled,
    COALESCE(ma.num_chronic_disease, 0) as num_chronic_disease,
    
    -- Housing quality score (0-100)
    (
        CASE WHEN hb.has_improved_water THEN 25 ELSE 0 END +
        CASE WHEN hb.has_improved_sanitation THEN 25 ELSE 0 END +
        CASE WHEN hb.has_electricity THEN 25 ELSE 0 END +
        CASE WHEN hb.num_rooms >= 2 THEN 25 ELSE 0 END
    ) as housing_quality_score,
    
    -- Asset score (count of different asset types)
    (
        CASE WHEN hb.num_radios > 0 THEN 1 ELSE 0 END +
        CASE WHEN hb.num_smartphones > 0 THEN 1 ELSE 0 END +
        CASE WHEN hb.num_bicycles > 0 THEN 1 ELSE 0 END +
        CASE WHEN hb.num_televisions > 0 THEN 1 ELSE 0 END
    ) as asset_score,
    
    -- Benefit status
    COALESCE(bs.benefit_plan_enrolled, false) as benefit_plan_enrolled,
    COALESCE(bs.num_payments_received, 0) as num_payments_received,
    COALESCE(bs.total_amount_received, 0) as total_amount_received,
    
    CURRENT_DATE as report_date
    
FROM household_base hb
LEFT JOIN member_aggregates ma ON ma.group_id = hb.household_id
LEFT JOIN benefit_status bs ON bs.group_id = hb.household_id;

-- Create indexes for household view
CREATE INDEX IF NOT EXISTS idx_dash_hh_province ON dashboard_households (province);
CREATE INDEX IF NOT EXISTS idx_dash_hh_twa ON dashboard_households (is_twa_household);
CREATE INDEX IF NOT EXISTS idx_dash_hh_refugee ON dashboard_households (is_refugee_household);
CREATE INDEX IF NOT EXISTS idx_dash_hh_pmt ON dashboard_households (pmt_score);
CREATE INDEX IF NOT EXISTS idx_dash_hh_housing_quality ON dashboard_households (housing_quality_score);
"""

DASHBOARD_VULNERABLE_GROUPS_VIEW = """
-- Vulnerable groups analysis view
CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_vulnerable_groups AS
WITH location_groups AS (
    SELECT 
        l4."LocationName" as province,
        l4."LocationId" as province_id,
        l3."LocationName" as commune,
        l2."LocationName" as zone,
        
        -- Group type determination
        CASE 
            WHEN g."Json_ext"->>'menage_mutwa' = 'OUI' THEN 'TWA'
            WHEN g."Json_ext"->>'menage_refugie' = 'OUI' THEN 'REFUGEE'
            WHEN g."Json_ext"->>'menage_rapatrie' = 'OUI' THEN 'RETURNEE'
            WHEN g."Json_ext"->>'menage_deplace' = 'OUI' THEN 'DISPLACED'
            ELSE 'HOST'
        END as group_type,
        
        g."UUID" as household_id,
        CAST(NULLIF(g."Json_ext"->>'score_pmt_initial', '') AS FLOAT) as pmt_score,
        CASE WHEN g."Json_ext"->>'a_terres' = 'OUI' THEN 1 ELSE 0 END as has_land,
        CASE WHEN g."Json_ext"->>'a_elevage' = 'OUI' THEN 1 ELSE 0 END as has_livestock,
        CASE WHEN g."Json_ext"->>'alimentaire_sans_nourriture' = 'ALIMENTAIRE_SANS_NOURRITURE_OUI' THEN 1 ELSE 0 END as food_insecure,
        CASE 
            WHEN g."Json_ext"->>'logement_eau_boisson' IN ('LOGEMENT_EAU_BOISSON_ROBINET_DOMICILE', 'LOGEMENT_EAU_BOISSON_BORNE_FONTAINE') 
            THEN 1 ELSE 0 
        END as has_improved_water
        
    FROM individual_group g
    LEFT JOIN "tblLocations" l1 ON g.location_id = l1."LocationId"
    LEFT JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
    LEFT JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
    LEFT JOIN "tblLocations" l4 ON l3."ParentLocationId" = l4."LocationId"
    WHERE g."isDeleted" = false
),
member_details AS (
    SELECT 
        gi.group_id,
        COUNT(DISTINCT i."UUID") as num_individuals,
        COUNT(CASE WHEN i."Json_ext"->>'sexe' = 'F' THEN 1 END) as women_count,
        COUNT(CASE WHEN i."Json_ext"->>'sexe' = 'M' THEN 1 END) as men_count,
        COUNT(CASE WHEN EXTRACT(YEAR FROM AGE(i.dob)) < 18 THEN 1 END) as children_count,
        COUNT(CASE WHEN EXTRACT(YEAR FROM AGE(i.dob)) >= 60 THEN 1 END) as elderly_count,
        COUNT(CASE WHEN i."Json_ext"->>'handicap' = 'OUI' THEN 1 END) as disabled_count,
        AVG(CASE WHEN i."Json_ext"->>'lit' = 'OUI' THEN 1 ELSE 0 END) as literacy_rate,
        AVG(CASE WHEN i."Json_ext"->>'va_ecole' = 'OUI' AND EXTRACT(YEAR FROM AGE(i.dob)) < 18 THEN 1 ELSE 0 END) as school_attendance_rate
    FROM individual_groupindividual gi
    JOIN individual_individual i ON i."UUID" = gi.individual_id
    WHERE gi."isDeleted" = false AND i."isDeleted" = false
    GROUP BY gi.group_id
),
benefit_details AS (
    SELECT 
        gb.group_id,
        COUNT(DISTINCT gb."UUID") as enrolled_beneficiaries,
        COUNT(bc."UUID") as total_payments,
        COUNT(CASE WHEN pp."PaymentStatus" = 'SUCCESS' THEN 1 END) as successful_payments,
        SUM(bc."Amount") as total_amount_paid
    FROM social_protection_groupbeneficiary gb
    LEFT JOIN payroll_benefitconsumption bc ON bc.benefit_id = gb."UUID" AND bc."isDeleted" = false
    LEFT JOIN payroll_payroll pp ON pp."UUID" = bc.payroll_id AND pp."isDeleted" = false
    WHERE gb."isDeleted" = false
    GROUP BY gb.group_id
)
SELECT 
    lg.group_type,
    lg.province,
    lg.commune,
    lg.zone,
    
    -- Demographics
    COUNT(DISTINCT lg.household_id) as total_households,
    COALESCE(SUM(md.num_individuals), 0) as total_individuals,
    COALESCE(SUM(md.women_count), 0) as women_count,
    COALESCE(SUM(md.men_count), 0) as men_count,
    COALESCE(SUM(md.children_count), 0) as children_count,
    COALESCE(SUM(md.elderly_count), 0) as elderly_count,
    COALESCE(SUM(md.disabled_count), 0) as disabled_count,
    
    -- Program coverage
    COUNT(DISTINCT CASE WHEN bd.enrolled_beneficiaries > 0 THEN lg.household_id END) as enrolled_households,
    COALESCE(SUM(bd.enrolled_beneficiaries), 0) as enrolled_individuals,
    CASE 
        WHEN COUNT(DISTINCT lg.household_id) > 0 
        THEN COUNT(DISTINCT CASE WHEN bd.enrolled_beneficiaries > 0 THEN lg.household_id END)::FLOAT / COUNT(DISTINCT lg.household_id)
        ELSE 0
    END as coverage_rate,
    
    -- Payment performance
    COALESCE(SUM(bd.total_payments), 0) as total_payments,
    COALESCE(SUM(bd.successful_payments), 0) as successful_payments,
    CASE 
        WHEN SUM(bd.total_payments) > 0 
        THEN SUM(bd.successful_payments)::FLOAT / SUM(bd.total_payments)
        ELSE 0
    END as payment_success_rate,
    COALESCE(SUM(bd.total_amount_paid), 0) as total_amount_paid,
    CASE 
        WHEN COUNT(DISTINCT lg.household_id) > 0
        THEN SUM(bd.total_amount_paid)::FLOAT / COUNT(DISTINCT lg.household_id)
        ELSE 0
    END as avg_amount_per_household,
    
    -- Socio-economic indicators
    AVG(lg.pmt_score) as avg_pmt_score,
    AVG(lg.has_land) as percent_with_land,
    AVG(lg.has_livestock) as percent_with_livestock,
    AVG(lg.food_insecure) as percent_food_insecure,
    AVG(lg.has_improved_water) as percent_improved_housing,
    
    -- Education
    AVG(md.literacy_rate) as literacy_rate,
    AVG(md.school_attendance_rate) as school_attendance_rate,
    
    CURRENT_DATE as report_date,
    EXTRACT(YEAR FROM CURRENT_DATE) as year,
    EXTRACT(QUARTER FROM CURRENT_DATE) as quarter,
    EXTRACT(MONTH FROM CURRENT_DATE) as month
    
FROM location_groups lg
LEFT JOIN member_details md ON md.group_id = lg.household_id
LEFT JOIN benefit_details bd ON bd.group_id = lg.household_id
GROUP BY lg.group_type, lg.province, lg.commune, lg.zone;

-- Create indexes for vulnerable groups view
CREATE INDEX IF NOT EXISTS idx_dash_vg_group_type ON dashboard_vulnerable_groups (group_type);
CREATE INDEX IF NOT EXISTS idx_dash_vg_province ON dashboard_vulnerable_groups (province);
CREATE INDEX IF NOT EXISTS idx_dash_vg_coverage ON dashboard_vulnerable_groups (coverage_rate);
"""

# Management command to create enhanced views
CREATE_ENHANCED_VIEWS = """
-- Create all enhanced materialized views
DO $$
BEGIN
    RAISE NOTICE 'Creating enhanced dashboard materialized views...';
    
    -- Drop existing views if needed
    DROP MATERIALIZED VIEW IF EXISTS dashboard_beneficiary_enhanced CASCADE;
    DROP MATERIALIZED VIEW IF EXISTS dashboard_households CASCADE;
    DROP MATERIALIZED VIEW IF EXISTS dashboard_vulnerable_groups CASCADE;
    
    -- Create views
    EXECUTE $VIEW1$
    {beneficiary_view}
    $VIEW1$;
    
    EXECUTE $VIEW2$
    {household_view}
    $VIEW2$;
    
    EXECUTE $VIEW3$
    {vulnerable_view}
    $VIEW3$;
    
    RAISE NOTICE 'Enhanced dashboard materialized views created successfully';
END $$;
""".format(
    beneficiary_view=DASHBOARD_ENHANCED_BENEFICIARY_VIEW,
    household_view=DASHBOARD_HOUSEHOLD_SUMMARY_VIEW,
    vulnerable_view=DASHBOARD_VULNERABLE_GROUPS_VIEW
)

# Refresh command
REFRESH_ENHANCED_VIEWS = """
-- Refresh enhanced materialized views
DO $$
BEGIN
    RAISE NOTICE 'Refreshing enhanced dashboard materialized views...';
    
    REFRESH MATERIALIZED VIEW CONCURRENTLY dashboard_beneficiary_enhanced;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dashboard_households;
    REFRESH MATERIALIZED VIEW CONCURRENTLY dashboard_vulnerable_groups;
    
    RAISE NOTICE 'Enhanced dashboard materialized views refreshed successfully';
END $$;
"""