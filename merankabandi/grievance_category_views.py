"""
Grievance Category Materialized Views with JSON Array Support
"""

# SQL definitions for grievance category views
GRIEVANCE_CATEGORY_VIEWS = {
    'dashboard_grievance_category_summary': """
    CREATE MATERIALIZED VIEW dashboard_grievance_category_summary AS
    WITH category_expanded AS (
        -- Extract individual categories from JSON arrays using LATERAL
        SELECT 
            t."UUID" as id,
            t.status,
            CASE 
                WHEN t.category LIKE '[%' THEN
                    TRIM(BOTH '"' FROM elem.value)
                ELSE t.category
            END as individual_category
        FROM grievance_social_protection_ticket t
        LEFT JOIN LATERAL json_array_elements_text(
            CASE WHEN t.category LIKE '[%' THEN t.category::json ELSE '[""]'::json END
        ) AS elem(value) ON true
        WHERE t."isDeleted" = false 
          AND t.category IS NOT NULL
          AND t.category != ''
          AND (t.category NOT LIKE '[%' OR elem.value IS NOT NULL)
    ),
    category_mapped AS (
        -- Map individual categories to their groups
        SELECT 
            id,
            status,
            individual_category,
            CASE 
                -- cas_sensibles mappings
                WHEN individual_category IN ('discrimination', 'abus_de_pouvoir', 'corruption_sollicitation_pot_de_vin', 
                                           'violence_agression_physique', 'exclusion_du_programme') THEN 'cas_sensibles'
                -- cas_speciaux mappings
                WHEN individual_category IN ('information', 'mise_a_jour_informations_personnelles', 'erreur_de_synchronisation',
                                           'double_tete', 'erreur_dinclusion', 'erreur_exclusion', 'autres_cas_speciaux',
                                           'menage_inexistant_dan_la_bd', 'mauvais_etat_de_la_carte', 'carte_non_recue',
                                           'erreur_de_ciblage', 'non_reception_du_cash', 'validation_paiement', 'inscription',
                                           'perte_de_carte', 'erreur_montant_recu', 'suspension_de_paiement', 'erreur_numero') THEN 'cas_speciaux'
                -- cas_non_sensibles mappings
                WHEN individual_category IN ('autres', 'carte_expiree', 'carte_bloquee', 'code_pin', 'telephone',
                                           'comment', 'demande_dinformations', 'felicitations', 'demande_de_paiement',
                                           'paiement_non_recu', 'deces', 'aide_medicale', 'destruction_abris_par_catastrophe',
                                           'deplacement_du_menage', 'erreur_operateur_paiement', 'suggestion') THEN 'cas_non_sensibles'
                -- Default to uncategorized
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
    ORDER BY category_group, status;
    """,
    
    'dashboard_grievance_category_details': """
    CREATE MATERIALIZED VIEW dashboard_grievance_category_details AS
    WITH category_expanded AS (
        -- Extract individual categories from JSON arrays using LATERAL
        SELECT 
            t."UUID" as id,
            t.status,
            COALESCE(t.code, '') as code,
            t.date_of_incident,
            t.channel,
            CASE 
                WHEN t.category LIKE '[%' THEN
                    TRIM(BOTH '"' FROM elem.value)
                ELSE t.category
            END as individual_category
        FROM grievance_social_protection_ticket t
        LEFT JOIN LATERAL json_array_elements_text(
            CASE WHEN t.category LIKE '[%' THEN t.category::json ELSE '[""]'::json END
        ) AS elem(value) ON true
        WHERE t."isDeleted" = false 
          AND t.category IS NOT NULL
          AND t.category != ''
          AND (t.category NOT LIKE '[%' OR elem.value IS NOT NULL)
    ),
    category_mapped AS (
        -- Map individual categories to their groups
        SELECT 
            *,
            CASE 
                -- cas_sensibles mappings
                WHEN individual_category IN ('discrimination', 'abus_de_pouvoir', 'corruption_sollicitation_pot_de_vin', 
                                           'violence_agression_physique', 'exclusion_du_programme') THEN 'cas_sensibles'
                -- cas_speciaux mappings
                WHEN individual_category IN ('information', 'mise_a_jour_informations_personnelles', 'erreur_de_synchronisation',
                                           'double_tete', 'erreur_dinclusion', 'erreur_exclusion', 'autres_cas_speciaux',
                                           'menage_inexistant_dan_la_bd', 'mauvais_etat_de_la_carte', 'carte_non_recue',
                                           'erreur_de_ciblage', 'non_reception_du_cash', 'validation_paiement', 'inscription',
                                           'perte_de_carte', 'erreur_montant_recu', 'suspension_de_paiement', 'erreur_numero') THEN 'cas_speciaux'
                -- cas_non_sensibles mappings
                WHEN individual_category IN ('autres', 'carte_expiree', 'carte_bloquee', 'code_pin', 'telephone',
                                           'comment', 'demande_dinformations', 'felicitations', 'demande_de_paiement',
                                           'paiement_non_recu', 'deces', 'aide_medicale', 'destruction_abris_par_catastrophe',
                                           'deplacement_du_menage', 'erreur_operateur_paiement', 'suggestion') THEN 'cas_non_sensibles'
                -- Default to uncategorized
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
    ORDER BY count DESC;
    """
}