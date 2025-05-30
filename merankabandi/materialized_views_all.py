"""
Consolidated Materialized Views for Dashboard Optimization
All views managed in one place to avoid duplication and simplify maintenance
"""

from django.db import connection
import logging

logger = logging.getLogger(__name__)


class ConsolidatedMaterializedViews:
    """
    Single source of truth for all materialized views in the Merankabandi dashboard
    """
    
    # Define all views in a dictionary for easy management
    VIEWS = {
        # ========== BENEFICIARY VIEWS ==========
        'dashboard_beneficiary_summary': """
            CREATE MATERIALIZED VIEW dashboard_beneficiary_summary AS
            WITH beneficiary_groups AS (
                SELECT 
                    g.id as group_id,
                    g.location_id,
                    COUNT(DISTINCT gi.individual_id) as group_size,
                    COUNT(DISTINCT CASE WHEN i.json_ext->>'sexe' = 'F' THEN gi.individual_id END) as female_count,
                    COUNT(DISTINCT CASE WHEN i.json_ext->>'sexe' = 'M' THEN gi.individual_id END) as male_count,
                    CASE 
                        WHEN g.json_ext->>'menage_mutwa' = 'OUI' THEN true
                        ELSE false
                    END as is_twa,
                    CASE 
                        WHEN g.json_ext->>'type_menage' = 'Communauté hôte' THEN 'HOST'
                        WHEN g.json_ext->>'type_menage' = 'Refugie' THEN 'REFUGEE'
                        ELSE 'OTHER'
                    END as community_type
                FROM social_protection_group g
                LEFT JOIN individual_groupindividual gi ON gi.group_id = g.id AND gi.is_deleted = false
                LEFT JOIN individual_individual i ON i.id = gi.individual_id AND i.is_deleted = false
                WHERE g.is_deleted = false
                GROUP BY g.id, g.location_id, g.json_ext
            )
            SELECT 
                loc."LocationId" as location_id,
                loc."LocationName" as location_name,
                com."LocationId" as commune_id,
                com."LocationName" as commune_name,
                prov."LocationId" as province_id,
                prov."LocationName" as province_name,
                COUNT(DISTINCT bg.group_id) as total_groups,
                SUM(bg.group_size) as total_beneficiaries,
                SUM(bg.female_count) as total_female,
                SUM(bg.male_count) as total_male,
                COUNT(DISTINCT CASE WHEN bg.is_twa THEN bg.group_id END) as twa_groups,
                SUM(CASE WHEN bg.is_twa THEN bg.group_size ELSE 0 END) as twa_beneficiaries,
                bg.community_type,
                CURRENT_DATE as last_updated
            FROM beneficiary_groups bg
            LEFT JOIN "tblLocations" loc ON loc."LocationId" = bg.location_id
            LEFT JOIN "tblLocations" com ON com."LocationId" = loc."ParentLocationId"
            LEFT JOIN "tblLocations" prov ON prov."LocationId" = com."ParentLocationId"
            GROUP BY 
                loc."LocationId", loc."LocationName",
                com."LocationId", com."LocationName",
                prov."LocationId", prov."LocationName",
                bg.community_type
        """,
        
        # ========== MONETARY TRANSFER VIEWS ==========
        'dashboard_monetary_transfers': """
            CREATE MATERIALIZED VIEW dashboard_monetary_transfers AS
            SELECT 
                mt.id,
                mt.transfer_date,
                EXTRACT(YEAR FROM mt.transfer_date) as year,
                EXTRACT(MONTH FROM mt.transfer_date) as month,
                EXTRACT(QUARTER FROM mt.transfer_date) as quarter,
                mt.location_id,
                loc."LocationName" as location_name,
                com."LocationId" as commune_id,
                com."LocationName" as commune_name,
                prov."LocationId" as province_id,
                prov."LocationName" as province_name,
                mt.programme_id,
                bp."code" as programme_code,
                bp."name" as programme_name,
                mt.payment_agency_id,
                pp."name" as payment_agency_name,
                mt.planned_women,
                mt.planned_men,
                mt.planned_twa,
                mt.planned_women + mt.planned_men + mt.planned_twa as total_planned,
                mt.paid_women,
                mt.paid_men,
                mt.paid_twa,
                mt.paid_women + mt.paid_men + mt.paid_twa as total_paid,
                COALESCE(bp."ceiling_per_beneficiary", 0) * (mt.paid_women + mt.paid_men + mt.paid_twa) as total_amount,
                CASE WHEN (mt.planned_women + mt.planned_men + mt.planned_twa) > 0 
                    THEN (mt.paid_women + mt.paid_men + mt.paid_twa)::numeric / (mt.planned_women + mt.planned_men + mt.planned_twa) * 100 
                    ELSE 0 
                END as completion_rate,
                CURRENT_DATE as last_updated
            FROM merankabandi_monetarytransfer mt
            LEFT JOIN social_protection_benefitplan bp ON bp."UUID" = mt.programme_id
            LEFT JOIN payroll_paymentpoint pp ON pp."UUID" = mt.payment_agency_id
            LEFT JOIN "tblLocations" loc ON loc."LocationId" = mt.location_id
            LEFT JOIN "tblLocations" com ON com."LocationId" = loc."ParentLocationId"
            LEFT JOIN "tblLocations" prov ON prov."LocationId" = com."ParentLocationId"
        """,
        
        'dashboard_transfers_by_province': """
            CREATE MATERIALIZED VIEW dashboard_transfers_by_province AS
            SELECT 
                province_id,
                province_name,
                year,
                month,
                programme_id,
                programme_name,
                COUNT(*) as transfer_count,
                SUM(total_planned) as total_planned,
                SUM(total_paid) as total_paid,
                SUM(paid_women) as women_paid,
                SUM(paid_men) as men_paid,
                SUM(paid_twa) as twa_paid,
                SUM(total_amount) as total_amount,
                AVG(completion_rate) as avg_completion_rate
            FROM dashboard_monetary_transfers
            GROUP BY 
                province_id, province_name,
                year, month,
                programme_id, programme_name
        """,
        
        'dashboard_transfers_by_time': """
            CREATE MATERIALIZED VIEW dashboard_transfers_by_time AS
            SELECT 
                year,
                month,
                quarter,
                COUNT(*) as transfer_count,
                SUM(total_planned) as total_planned,
                SUM(total_paid) as total_paid,
                SUM(paid_women) as women_paid,
                SUM(paid_men) as men_paid,
                SUM(paid_twa) as twa_paid,
                SUM(total_amount) as total_amount,
                AVG(completion_rate) as avg_completion_rate
            FROM dashboard_monetary_transfers
            GROUP BY year, month, quarter
            ORDER BY year, month
        """,
        
        # ========== ACTIVITY VIEWS ==========
        'dashboard_activities_summary': """
            CREATE MATERIALIZED VIEW dashboard_activities_summary AS
            WITH all_activities AS (
                -- Behavior Change Promotion
                SELECT 
                    'BehaviorChangePromotion' as activity_type,
                    report_date as activity_date,
                    location_id,
                    male_participants + female_participants + twa_participants as total_participants,
                    male_participants,
                    female_participants,
                    twa_participants
                FROM merankabandi_behaviorchangepromotion
                WHERE validation_status = 'VALIDATED'
                
                UNION ALL
                
                -- Sensitization Training
                SELECT 
                    'SensitizationTraining' as activity_type,
                    report_date as activity_date,
                    location_id,
                    male_participants + female_participants + twa_participants as total_participants,
                    male_participants,
                    female_participants,
                    twa_participants
                FROM merankabandi_sensitizationtraining
                WHERE validation_status = 'VALIDATED'
            )
            SELECT 
                loc."LocationId" as location_id,
                loc."LocationName" as location_name,
                com."LocationId" as commune_id,
                com."LocationName" as commune_name,
                prov."LocationId" as province_id,
                prov."LocationName" as province_name,
                activity_type,
                EXTRACT(YEAR FROM activity_date) as year,
                EXTRACT(MONTH FROM activity_date) as month,
                COUNT(*) as activity_count,
                SUM(total_participants) as total_participants,
                SUM(male_participants) as male_participants,
                SUM(female_participants) as female_participants,
                SUM(twa_participants) as twa_participants
            FROM all_activities a
            LEFT JOIN "tblLocations" loc ON loc."LocationId" = a.location_id
            LEFT JOIN "tblLocations" com ON com."LocationId" = loc."ParentLocationId"
            LEFT JOIN "tblLocations" prov ON prov."LocationId" = com."ParentLocationId"
            GROUP BY 
                loc."LocationId", loc."LocationName",
                com."LocationId", com."LocationName",
                prov."LocationId", prov."LocationName",
                activity_type,
                EXTRACT(YEAR FROM activity_date),
                EXTRACT(MONTH FROM activity_date)
        """,
        
        'dashboard_activities_by_type': """
            CREATE MATERIALIZED VIEW dashboard_activities_by_type AS
            SELECT 
                activity_type,
                year,
                month,
                SUM(activity_count) as total_activities,
                SUM(total_participants) as total_participants,
                SUM(male_participants) as male_participants,
                SUM(female_participants) as female_participants,
                SUM(twa_participants) as twa_participants,
                COUNT(DISTINCT location_id) as locations_covered
            FROM dashboard_activities_summary
            GROUP BY activity_type, year, month
        """,
        
        # ========== MICROPROJECT VIEWS ==========
        'dashboard_microprojects': """
            CREATE MATERIALIZED VIEW dashboard_microprojects AS
            SELECT 
                mp.id,
                mp.report_date,
                EXTRACT(YEAR FROM mp.report_date) as year,
                EXTRACT(MONTH FROM mp.report_date) as month,
                mp.location_id,
                loc."LocationName" as location_name,
                com."LocationId" as commune_id,
                com."LocationName" as commune_name,
                prov."LocationId" as province_id,
                prov."LocationName" as province_name,
                mp.project_type,
                mp.number_of_projects,
                mp.male_participants,
                mp.female_participants,
                mp.twa_participants,
                mp.male_participants + mp.female_participants + mp.twa_participants as total_participants,
                mp.validation_status
            FROM merankabandi_microproject mp
            LEFT JOIN "tblLocations" loc ON loc."LocationId" = mp.location_id
            LEFT JOIN "tblLocations" com ON com."LocationId" = loc."ParentLocationId"
            LEFT JOIN "tblLocations" prov ON prov."LocationId" = com."ParentLocationId"
            WHERE mp.validation_status = 'VALIDATED'
        """,
        
        # ========== GRIEVANCE VIEWS ==========
        'dashboard_grievances': """
            CREATE MATERIALIZED VIEW dashboard_grievances AS
            WITH ticket_aggregates AS (
                SELECT 
                    'SUMMARY' as summary_type,
                    COUNT(*) as total_tickets,
                    COUNT(CASE WHEN status = 'OPEN' THEN 1 END) as open_tickets,
                    COUNT(CASE WHEN status = 'IN_PROGRESS' THEN 1 END) as in_progress_tickets,
                    COUNT(CASE WHEN status = 'RESOLVED' THEN 1 END) as resolved_tickets,
                    COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) as closed_tickets,
                    COUNT(CASE WHEN flags @> '["SENSITIVE"]' THEN 1 END) as sensitive_tickets,
                    COUNT(CASE WHEN flags @> '["ANONYMOUS"]' THEN 1 END) as anonymous_tickets,
                    AVG(
                        CASE 
                            WHEN status IN ('RESOLVED', 'CLOSED') AND resolution_date IS NOT NULL 
                            THEN EXTRACT(EPOCH FROM (resolution_date - date_of_incident))/86400
                            ELSE NULL
                        END
                    ) as avg_resolution_days
                FROM grievance_social_protection_ticket
                WHERE "isDeleted" = false
            )
            SELECT * FROM ticket_aggregates
        """,
        
        'dashboard_grievance_status': """
            CREATE MATERIALIZED VIEW dashboard_grievance_status AS
            SELECT 
                status,
                COUNT(*) as count,
                COUNT(*)::numeric / (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false)::numeric * 100 as percentage,
                CURRENT_DATE as report_date
            FROM grievance_social_protection_ticket 
            WHERE "isDeleted" = false AND status IS NOT NULL
            GROUP BY status
            ORDER BY 
                CASE status
                    WHEN 'OPEN' THEN 1
                    WHEN 'IN_PROGRESS' THEN 2
                    WHEN 'RESOLVED' THEN 3
                    WHEN 'CLOSED' THEN 4
                    ELSE 5
                END
        """,
        
        'dashboard_grievance_category': """
            CREATE MATERIALIZED VIEW dashboard_grievance_category AS
            SELECT 
                category,
                COUNT(*) as count,
                COUNT(*)::numeric / (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false)::numeric * 100 as percentage,
                CURRENT_DATE as report_date
            FROM grievance_social_protection_ticket 
            WHERE "isDeleted" = false AND category IS NOT NULL
            GROUP BY category
            ORDER BY count DESC
        """,
        
        # ========== GRIEVANCE CATEGORY VIEWS (JSON ARRAY HANDLING) ==========
        'dashboard_grievance_category_summary': """
            CREATE MATERIALIZED VIEW dashboard_grievance_category_summary AS
            WITH category_expanded AS (
                SELECT 
                    t.id,
                    t.status,
                    CASE 
                        WHEN t.category LIKE '[%' THEN
                            TRIM(BOTH '"' FROM json_array_elements_text(t.category::json))
                        ELSE t.category
                    END as individual_category
                FROM grievance_social_protection_ticket t
                WHERE t."isDeleted" = false 
                  AND t.category IS NOT NULL
                  AND t.category != ''
            ),
            category_mapped AS (
                SELECT 
                    id,
                    status,
                    individual_category,
                    CASE 
                        WHEN individual_category IN ('discrimination', 'abus_de_pouvoir', 'corruption_sollicitation_pot_de_vin', 
                                                   'violence_agression_physique', 'exclusion_du_programme') THEN 'cas_sensibles'
                        WHEN individual_category IN ('information', 'mise_a_jour_informations_personnelles', 'erreur_de_synchronisation',
                                                   'double_tete', 'erreur_dinclusion', 'erreur_exclusion', 'autres_cas_speciaux',
                                                   'menage_inexistant_dan_la_bd', 'mauvais_etat_de_la_carte', 'carte_non_recue',
                                                   'erreur_de_ciblage', 'non_reception_du_cash', 'validation_paiement', 'inscription',
                                                   'perte_de_carte', 'erreur_montant_recu', 'suspension_de_paiement', 'erreur_numero') THEN 'cas_speciaux'
                        WHEN individual_category IN ('autres', 'carte_expiree', 'carte_bloquee', 'code_pin', 'telephone',
                                                   'comment', 'demande_dinformations', 'felicitations', 'demande_de_paiement',
                                                   'paiement_non_recu', 'deces', 'aide_medicale', 'destruction_abris_par_catastrophe',
                                                   'deplacement_du_menage', 'erreur_operateur_paiement', 'suggestion') THEN 'cas_non_sensibles'
                        ELSE 'uncategorized'
                    END as category_group
                FROM category_expanded
            )
            SELECT 
                category_group,
                status,
                COUNT(DISTINCT id) as count,
                COUNT(DISTINCT id)::numeric / (
                    SELECT COUNT(*) 
                    FROM grievance_social_protection_ticket 
                    WHERE "isDeleted" = false
                )::numeric * 100 as percentage,
                CURRENT_DATE as report_date
            FROM category_mapped
            GROUP BY category_group, status
            ORDER BY category_group, status
        """,
        
        'dashboard_grievance_category_details': """
            CREATE MATERIALIZED VIEW dashboard_grievance_category_details AS
            WITH category_expanded AS (
                SELECT 
                    t.id,
                    t.status,
                    t.ticket_id,
                    t.date_of_incident,
                    t.channel,
                    CASE 
                        WHEN t.category LIKE '[%' THEN
                            TRIM(BOTH '"' FROM json_array_elements_text(t.category::json))
                        ELSE t.category
                    END as individual_category
                FROM grievance_social_protection_ticket t
                WHERE t."isDeleted" = false 
                  AND t.category IS NOT NULL
                  AND t.category != ''
            ),
            category_mapped AS (
                SELECT 
                    *,
                    CASE 
                        WHEN individual_category IN ('discrimination', 'abus_de_pouvoir', 'corruption_sollicitation_pot_de_vin', 
                                                   'violence_agression_physique', 'exclusion_du_programme') THEN 'cas_sensibles'
                        WHEN individual_category IN ('information', 'mise_a_jour_informations_personnelles', 'erreur_de_synchronisation',
                                                   'double_tete', 'erreur_dinclusion', 'erreur_exclusion', 'autres_cas_speciaux',
                                                   'menage_inexistant_dan_la_bd', 'mauvais_etat_de_la_carte', 'carte_non_recue',
                                                   'erreur_de_ciblage', 'non_reception_du_cash', 'validation_paiement', 'inscription',
                                                   'perte_de_carte', 'erreur_montant_recu', 'suspension_de_paiement', 'erreur_numero') THEN 'cas_speciaux'
                        WHEN individual_category IN ('autres', 'carte_expiree', 'carte_bloquee', 'code_pin', 'telephone',
                                                   'comment', 'demande_dinformations', 'felicitations', 'demande_de_paiement',
                                                   'paiement_non_recu', 'deces', 'aide_medicale', 'destruction_abris_par_catastrophe',
                                                   'deplacement_du_menage', 'erreur_operateur_paiement', 'suggestion') THEN 'cas_non_sensibles'
                        ELSE 'uncategorized'
                    END as category_group
                FROM category_expanded
            )
            SELECT 
                individual_category,
                category_group,
                status,
                COUNT(DISTINCT id) as count,
                COUNT(DISTINCT id)::numeric / (
                    SELECT COUNT(*) 
                    FROM grievance_social_protection_ticket 
                    WHERE "isDeleted" = false
                )::numeric * 100 as percentage,
                CURRENT_DATE as report_date
            FROM category_mapped
            GROUP BY individual_category, category_group, status
            ORDER BY count DESC
        """,
        
        # ========== ADDITIONAL GRIEVANCE VIEWS ==========
        'dashboard_grievance_channel': """
            CREATE MATERIALIZED VIEW dashboard_grievance_channel AS
            SELECT 
                channel,
                COUNT(*) as count,
                COUNT(*)::numeric / (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false)::numeric * 100 as percentage,
                CURRENT_DATE as report_date
            FROM grievance_social_protection_ticket 
            WHERE "isDeleted" = false AND channel IS NOT NULL
            GROUP BY channel
            ORDER BY count DESC
        """,
        
        'dashboard_grievance_priority': """
            CREATE MATERIALIZED VIEW dashboard_grievance_priority AS
            SELECT 
                priority,
                COUNT(*) as count,
                COUNT(*)::numeric / (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false)::numeric * 100 as percentage,
                CURRENT_DATE as report_date
            FROM grievance_social_protection_ticket 
            WHERE "isDeleted" = false AND priority IS NOT NULL
            GROUP BY priority
            ORDER BY count DESC
        """,
        
        'dashboard_grievance_gender': """
            CREATE MATERIALIZED VIEW dashboard_grievance_gender AS
            SELECT 
                i.gender,
                COUNT(DISTINCT t.id) as count,
                COUNT(DISTINCT t.id)::numeric / (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false)::numeric * 100 as percentage,
                CURRENT_DATE as report_date
            FROM grievance_social_protection_ticket t
            LEFT JOIN individual_individual i ON t.complainant_individual_id = i.id
            WHERE t."isDeleted" = false AND i.gender IS NOT NULL
            GROUP BY i.gender
            ORDER BY count DESC
        """,
        
        'dashboard_grievance_age': """
            CREATE MATERIALIZED VIEW dashboard_grievance_age AS
            SELECT 
                CASE 
                    WHEN AGE(CURRENT_DATE, i.dob) < INTERVAL '18 years' THEN '0-17'
                    WHEN AGE(CURRENT_DATE, i.dob) < INTERVAL '35 years' THEN '18-34'
                    WHEN AGE(CURRENT_DATE, i.dob) < INTERVAL '50 years' THEN '35-49'
                    WHEN AGE(CURRENT_DATE, i.dob) < INTERVAL '65 years' THEN '50-64'
                    ELSE '65+'
                END as age_group,
                COUNT(DISTINCT t.id) as count,
                COUNT(DISTINCT t.id)::numeric / (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false)::numeric * 100 as percentage,
                CURRENT_DATE as report_date
            FROM grievance_social_protection_ticket t
            LEFT JOIN individual_individual i ON t.complainant_individual_id = i.id
            WHERE t."isDeleted" = false AND i.dob IS NOT NULL
            GROUP BY age_group
            ORDER BY age_group
        """,
        
        # ========== RESULTS FRAMEWORK VIEWS ==========
        'dashboard_results_framework': """
            CREATE MATERIALIZED VIEW dashboard_results_framework AS
            SELECT 
                s.id as section_id,
                s.code as section_code,
                s.name as section_name,
                s.order_number as section_order,
                COUNT(DISTINCT i.id) as indicator_count,
                COUNT(DISTINCT a.id) as achievement_count,
                COALESCE(AVG(
                    CASE 
                        WHEN i.target_value > 0 THEN 
                            (a.achieved_value::numeric / i.target_value::numeric) * 100
                        ELSE NULL
                    END
                ), 0) as avg_achievement_percentage
            FROM merankabandi_section s
            LEFT JOIN merankabandi_indicator i ON i.section_id = s.id
            LEFT JOIN merankabandi_indicatorachievement a ON a.indicator_id = i.id
            GROUP BY s.id, s.code, s.name, s.order_number
            ORDER BY s.order_number
        """,
        
        'dashboard_indicators_by_section': """
            CREATE MATERIALIZED VIEW dashboard_indicators_by_section AS
            SELECT 
                s.id as section_id,
                s.code as section_code,
                s.name as section_name,
                i.id as indicator_id,
                i.code as indicator_code,
                i.name as indicator_name,
                i.measurement_unit,
                i.target_value,
                COALESCE(SUM(a.achieved_value), 0) as total_achieved,
                CASE 
                    WHEN i.target_value > 0 THEN 
                        (COALESCE(SUM(a.achieved_value), 0)::numeric / i.target_value::numeric) * 100
                    ELSE 0
                END as achievement_percentage,
                COUNT(DISTINCT a.id) as achievement_count
            FROM merankabandi_section s
            JOIN merankabandi_indicator i ON i.section_id = s.id
            LEFT JOIN merankabandi_indicatorachievement a ON a.indicator_id = i.id
            GROUP BY 
                s.id, s.code, s.name,
                i.id, i.code, i.name, i.measurement_unit, i.target_value
            ORDER BY s.order_number, i.code
        """,
        
        'dashboard_indicator_performance': """
            CREATE MATERIALIZED VIEW dashboard_indicator_performance AS
            WITH quarterly_achievements AS (
                SELECT 
                    i.id as indicator_id,
                    i.code as indicator_code,
                    i.name as indicator_name,
                    EXTRACT(YEAR FROM a.report_date) as year,
                    EXTRACT(QUARTER FROM a.report_date) as quarter,
                    SUM(a.achieved_value) as quarterly_achieved,
                    i.target_value / 4 as quarterly_target  -- Assuming annual target divided by 4
                FROM merankabandi_indicator i
                LEFT JOIN merankabandi_indicatorachievement a ON a.indicator_id = i.id
                GROUP BY 
                    i.id, i.code, i.name, i.target_value,
                    EXTRACT(YEAR FROM a.report_date),
                    EXTRACT(QUARTER FROM a.report_date)
            )
            SELECT 
                indicator_id,
                indicator_code,
                indicator_name,
                year,
                quarter,
                quarterly_achieved,
                quarterly_target,
                CASE 
                    WHEN quarterly_target > 0 THEN 
                        (quarterly_achieved::numeric / quarterly_target::numeric) * 100
                    ELSE 0
                END as performance_percentage
            FROM quarterly_achievements
            WHERE year IS NOT NULL
            ORDER BY year DESC, quarter DESC, indicator_code
        """,
        
        # ========== MASTER SUMMARY VIEW ==========
        'dashboard_master_summary': """
            CREATE MATERIALIZED VIEW dashboard_master_summary AS
            WITH location_hierarchy AS (
                SELECT DISTINCT
                    loc."LocationId" as location_id,
                    loc."LocationName" as location_name,
                    com."LocationId" as commune_id,
                    com."LocationName" as commune_name,
                    prov."LocationId" as province_id,
                    prov."LocationName" as province_name
                FROM "tblLocations" loc
                LEFT JOIN "tblLocations" com ON com."LocationId" = loc."ParentLocationId"
                LEFT JOIN "tblLocations" prov ON prov."LocationId" = com."ParentLocationId"
                WHERE loc."LocationType" = 'V'  -- Villages/Collines
            ),
            beneficiary_stats AS (
                SELECT 
                    province_id,
                    COUNT(DISTINCT location_id) as active_locations,
                    SUM(total_beneficiaries) as total_beneficiaries,
                    SUM(total_groups) as total_groups,
                    SUM(total_female) as female_beneficiaries,
                    SUM(total_male) as male_beneficiaries,
                    SUM(twa_beneficiaries) as twa_beneficiaries
                FROM dashboard_beneficiary_summary
                GROUP BY province_id
            ),
            transfer_stats AS (
                SELECT 
                    province_id,
                    COUNT(*) as total_transfers,
                    SUM(total_paid) as total_beneficiaries_paid,
                    SUM(total_amount) as total_amount_paid,
                    AVG(completion_rate) as avg_completion_rate
                FROM dashboard_monetary_transfers
                WHERE EXTRACT(YEAR FROM transfer_date) = EXTRACT(YEAR FROM CURRENT_DATE)
                GROUP BY province_id
            ),
            activity_stats AS (
                SELECT 
                    province_id,
                    SUM(activity_count) as total_activities,
                    SUM(total_participants) as total_participants
                FROM dashboard_activities_summary
                WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
                GROUP BY province_id
            ),
            project_stats AS (
                SELECT 
                    province_id,
                    COUNT(*) as total_projects,
                    SUM(number_of_projects) as number_of_projects,
                    SUM(total_participants) as project_participants
                FROM dashboard_microprojects
                WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
                GROUP BY province_id
            ),
            grievance_stats AS (
                SELECT 
                    total_tickets,
                    open_tickets + in_progress_tickets as active_grievances,
                    resolved_tickets + closed_tickets as resolved_grievances,
                    avg_resolution_days
                FROM dashboard_grievances
                WHERE summary_type = 'SUMMARY'
            )
            SELECT 
                EXTRACT(YEAR FROM CURRENT_DATE) as year,
                EXTRACT(MONTH FROM CURRENT_DATE) as month,
                h.province_id,
                h.province_name,
                COUNT(DISTINCT h.location_id) as total_locations,
                -- Beneficiary stats
                COALESCE(b.total_beneficiaries, 0) as total_beneficiaries,
                COALESCE(b.total_groups, 0) as total_groups,
                COALESCE(b.female_beneficiaries, 0) as female_beneficiaries,
                COALESCE(b.male_beneficiaries, 0) as male_beneficiaries,
                COALESCE(b.twa_beneficiaries, 0) as twa_beneficiaries,
                COALESCE(b.active_locations, 0) as active_locations,
                -- Transfer stats
                COALESCE(t.total_transfers, 0) as total_transfers,
                COALESCE(t.total_amount_paid, 0) as total_amount_paid,
                COALESCE(t.avg_completion_rate, 0) as avg_completion_rate,
                -- Activity stats
                COALESCE(a.total_activities, 0) as total_activities,
                COALESCE(a.total_participants, 0) as activity_participants,
                -- Project stats
                COALESCE(p.total_projects, 0) as total_projects,
                COALESCE(p.project_participants, 0) as project_participants,
                -- Grievance stats (province-agnostic)
                COALESCE(g.total_tickets, 0) as total_grievances,
                COALESCE(g.resolved_grievances, 0) as resolved_grievances,
                COALESCE(g.avg_resolution_days, 0) as avg_resolution_days,
                CURRENT_TIMESTAMP as last_updated
            FROM location_hierarchy h
            LEFT JOIN beneficiary_stats b ON b.province_id = h.province_id
            LEFT JOIN transfer_stats t ON t.province_id = h.province_id
            LEFT JOIN activity_stats a ON a.province_id = h.province_id
            LEFT JOIN project_stats p ON p.province_id = h.province_id
            CROSS JOIN grievance_stats g
            GROUP BY 
                h.province_id, h.province_name,
                b.total_beneficiaries, b.total_groups, b.female_beneficiaries,
                b.male_beneficiaries, b.twa_beneficiaries, b.active_locations,
                t.total_transfers, t.total_amount_paid, t.avg_completion_rate,
                a.total_activities, a.total_participants,
                p.total_projects, p.project_participants,
                g.total_tickets, g.resolved_grievances, g.avg_resolution_days
        """,
        
        # ========== INDIVIDUAL SUMMARY VIEW ==========
        'dashboard_individual_summary': """
            CREATE MATERIALIZED VIEW dashboard_individual_summary AS
            SELECT 
                i.id as individual_id,
                i.first_name,
                i.last_name,
                i.dob,
                i.json_ext->>'sexe' as gender,
                AGE(CURRENT_DATE, i.dob) as age,
                gi.group_id,
                g.json_ext->>'nom_du_groupe' as group_name,
                g.location_id,
                loc."LocationName" as location_name,
                com."LocationId" as commune_id,
                com."LocationName" as commune_name,
                prov."LocationId" as province_id,
                prov."LocationName" as province_name,
                CASE 
                    WHEN g.json_ext->>'menage_mutwa' = 'OUI' THEN true
                    ELSE false
                END as is_twa,
                CASE 
                    WHEN g.json_ext->>'type_menage' = 'Communauté hôte' THEN 'HOST'
                    WHEN g.json_ext->>'type_menage' = 'Refugie' THEN 'REFUGEE'
                    ELSE 'OTHER'
                END as community_type,
                CURRENT_DATE as last_updated
            FROM individual_individual i
            JOIN individual_groupindividual gi ON gi.individual_id = i.id AND gi.is_deleted = false
            JOIN social_protection_group g ON g.id = gi.group_id AND g.is_deleted = false
            LEFT JOIN "tblLocations" loc ON loc."LocationId" = g.location_id
            LEFT JOIN "tblLocations" com ON com."LocationId" = loc."ParentLocationId"
            LEFT JOIN "tblLocations" prov ON prov."LocationId" = com."ParentLocationId"
            WHERE i.is_deleted = false
        """,
        
        # ========== PAYMENT REPORTING VIEWS ==========
        'payment_reporting_monetary_transfers': """
            CREATE MATERIALIZED VIEW payment_reporting_monetary_transfers AS
            SELECT 
                EXTRACT(YEAR FROM mt.transfer_date) as year,
                EXTRACT(MONTH FROM mt.transfer_date) as month,
                EXTRACT(QUARTER FROM mt.transfer_date) as quarter,
                mt.transfer_date,
                loc."LocationId" as location_id,
                loc."LocationName" as location_name,
                loc."LocationType" as location_type,
                com."LocationId" as commune_id,
                com."LocationName" as commune_name,
                prov."LocationId" as province_id,
                prov."LocationName" as province_name,
                mt.programme_id,
                bp."code" as programme_code,
                bp."name" as programme_name,
                bp."ceiling_per_beneficiary" as amount_per_beneficiary,
                mt.payment_agency_id,
                pp."name" as payment_agency_name,
                mt.planned_women,
                mt.planned_men,
                mt.planned_twa,
                mt.planned_women + mt.planned_men + mt.planned_twa as total_planned,
                mt.paid_women,
                mt.paid_men,
                mt.paid_twa,
                mt.paid_women + mt.paid_men + mt.paid_twa as total_paid,
                COALESCE(bp."ceiling_per_beneficiary", 0) * (mt.paid_women + mt.paid_men + mt.paid_twa) as total_amount_paid,
                CASE WHEN (mt.paid_women + mt.paid_men + mt.paid_twa) > 0 
                    THEN mt.paid_women::numeric / (mt.paid_women + mt.paid_men + mt.paid_twa) * 100 
                    ELSE 0 
                END as female_percentage,
                CASE WHEN (mt.paid_women + mt.paid_men + mt.paid_twa) > 0 
                    THEN mt.paid_twa::numeric / (mt.paid_women + mt.paid_men + mt.paid_twa) * 100 
                    ELSE 0 
                END as twa_percentage,
                CASE WHEN (mt.planned_women + mt.planned_men + mt.planned_twa) > 0 
                    THEN (mt.paid_women + mt.paid_men + mt.paid_twa)::numeric / (mt.planned_women + mt.planned_men + mt.planned_twa) * 100 
                    ELSE 0 
                END as completion_rate
            FROM merankabandi_monetarytransfer mt
            LEFT JOIN social_protection_benefitplan bp ON bp."UUID" = mt.programme_id
            LEFT JOIN payroll_paymentpoint pp ON pp."UUID" = mt.payment_agency_id
            LEFT JOIN "tblLocations" loc ON loc."LocationId" = mt.location_id
            LEFT JOIN "tblLocations" com ON com."LocationId" = loc."ParentLocationId"
            LEFT JOIN "tblLocations" prov ON prov."LocationId" = com."ParentLocationId"
            WHERE mt.transfer_date IS NOT NULL
        """,
        
        'payment_reporting_location_summary': """
            CREATE MATERIALIZED VIEW payment_reporting_location_summary AS
            SELECT 
                year::integer,
                month::integer,
                province_id,
                province_name,
                commune_id,
                commune_name,
                programme_id,
                programme_name,
                COUNT(*) as transfer_count,
                SUM(total_planned) as total_planned_beneficiaries,
                SUM(total_paid) as total_paid_beneficiaries,
                SUM(total_amount_paid) as total_amount,
                SUM(paid_women) as total_women,
                SUM(paid_men) as total_men,
                SUM(paid_twa) as total_twa,
                AVG(female_percentage) as avg_female_percentage,
                AVG(twa_percentage) as avg_twa_percentage,
                AVG(completion_rate) as avg_completion_rate
            FROM payment_reporting_monetary_transfers
            GROUP BY 
                year, month,
                province_id, province_name,
                commune_id, commune_name,
                programme_id, programme_name
        """
    }
    
    # Index definitions for each view
    INDEXES = {
        'dashboard_beneficiary_summary': [
            ('idx_beneficiary_location', ['location_id']),
            ('idx_beneficiary_province', ['province_id']),
            ('idx_beneficiary_community', ['community_type'])
        ],
        'dashboard_monetary_transfers': [
            ('idx_mt_date', ['year', 'month']),
            ('idx_mt_location', ['location_id']),
            ('idx_mt_province', ['province_id']),
            ('idx_mt_programme', ['programme_id'])
        ],
        'dashboard_transfers_by_province': [
            ('idx_transfers_prov', ['province_id', 'year', 'month'])
        ],
        'dashboard_transfers_by_time': [
            ('idx_transfers_time', ['year', 'month'])
        ],
        'dashboard_activities_summary': [
            ('idx_activities_location', ['location_id']),
            ('idx_activities_type_date', ['activity_type', 'year', 'month'])
        ],
        'dashboard_microprojects': [
            ('idx_microprojects_location', ['location_id']),
            ('idx_microprojects_date', ['year', 'month'])
        ],
        'dashboard_grievance_status': [
            ('idx_grievance_status', ['status'])
        ],
        'dashboard_grievance_category': [
            ('idx_grievance_category', ['category'])
        ],
        'dashboard_grievance_category_summary': [
            ('idx_grievance_cat_summary_group', ['category_group']),
            ('idx_grievance_cat_summary_status', ['status'])
        ],
        'dashboard_grievance_category_details': [
            ('idx_grievance_cat_details_category', ['individual_category']),
            ('idx_grievance_cat_details_group', ['category_group']),
            ('idx_grievance_cat_details_status', ['status'])
        ],
        'dashboard_results_framework': [
            ('idx_results_section', ['section_id'])
        ],
        'dashboard_master_summary': [
            ('idx_master_province_date', ['province_id', 'year', 'month'])
        ],
        'dashboard_individual_summary': [
            ('idx_individual_id', ['individual_id']),
            ('idx_individual_group', ['group_id']),
            ('idx_individual_location', ['location_id'])
        ],
        'payment_reporting_monetary_transfers': [
            ('idx_payment_mt_date', ['year', 'month']),
            ('idx_payment_mt_location', ['province_id', 'commune_id', 'location_id']),
            ('idx_payment_mt_programme', ['programme_id'])
        ],
        'payment_reporting_location_summary': [
            ('idx_payment_summary_location', ['province_id', 'commune_id']),
            ('idx_payment_summary_time', ['year', 'month'])
        ]
    }
    
    @classmethod
    def create_all_views(cls):
        """Create all materialized views"""
        with connection.cursor() as cursor:
            # First create the views
            for view_name, view_sql in cls.VIEWS.items():
                try:
                    # Drop existing view if it exists
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")
                    
                    # Create the view
                    full_sql = f"CREATE MATERIALIZED VIEW {view_name} AS {view_sql.split('AS', 1)[1]}"
                    cursor.execute(full_sql)
                    
                    # Create indexes
                    if view_name in cls.INDEXES:
                        for index_name, columns in cls.INDEXES[view_name]:
                            index_sql = f"CREATE INDEX {index_name} ON {view_name}({', '.join(columns)})"
                            cursor.execute(index_sql)
                    
                    logger.info(f"Created materialized view: {view_name}")
                    
                except Exception as e:
                    logger.error(f"Error creating view {view_name}: {e}")
                    raise
            
            # Create refresh functions
            cls._create_refresh_functions(cursor)
            
            # Initial refresh
            cls.refresh_all_views()
    
    @classmethod
    def refresh_all_views(cls, concurrent=True):
        """Refresh all materialized views"""
        concurrently = "CONCURRENTLY" if concurrent else ""
        
        with connection.cursor() as cursor:
            for view_name in cls.VIEWS.keys():
                try:
                    sql = f"REFRESH MATERIALIZED VIEW {concurrently} {view_name}"
                    cursor.execute(sql)
                    logger.info(f"Refreshed {view_name}")
                except Exception as e:
                    logger.error(f"Error refreshing {view_name}: {e}")
    
    @classmethod
    def drop_all_views(cls):
        """Drop all materialized views"""
        with connection.cursor() as cursor:
            # Drop in reverse order to handle dependencies
            for view_name in reversed(list(cls.VIEWS.keys())):
                try:
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")
                    logger.info(f"Dropped {view_name}")
                except Exception as e:
                    logger.error(f"Error dropping {view_name}: {e}")
    
    @classmethod
    def _create_refresh_functions(cls, cursor):
        """Create database functions for refreshing views"""
        sql = """
        CREATE OR REPLACE FUNCTION refresh_all_dashboard_views()
        RETURNS void AS $$
        DECLARE
            view_name text;
        BEGIN
            FOR view_name IN 
                SELECT matviewname 
                FROM pg_matviews 
                WHERE schemaname = 'public' 
                AND (matviewname LIKE 'dashboard_%' OR matviewname LIKE 'payment_reporting_%')
            LOOP
                EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY ' || view_name;
                RAISE NOTICE 'Refreshed view: %', view_name;
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;
        """
        cursor.execute(sql)
        logger.info("Created refresh functions")