"""
Grievance Materialized Views
Consolidated from 8 views down to 2:
  - dashboard_grievance_details: ticket-level detail with all dimensions
  - dashboard_grievances: master summary with status counts
"""

# Category → group mapping (single source of truth)
# Used in the SQL CTE to avoid copy-pasting across views
_CATEGORY_GROUP_CASE = """
    CASE
        WHEN (ce.individual_category)::text = ANY(ARRAY[
            'discrimination', 'abus_de_pouvoir',
            'corruption_sollicitation_pot_de_vin',
            'violence_agression_physique', 'exclusion_du_programme'
        ]) THEN 'cas_sensibles'
        WHEN (ce.individual_category)::text = ANY(ARRAY[
            'information', 'mise_a_jour_informations_personnelles',
            'erreur_de_synchronisation', 'double_tete',
            'erreur_dinclusion', 'erreur_exclusion',
            'autres_cas_speciaux', 'menage_inexistant_dan_la_bd',
            'mauvais_etat_de_la_carte', 'carte_non_recue',
            'erreur_de_ciblage', 'non_reception_du_cash',
            'validation_paiement', 'inscription',
            'perte_de_carte', 'erreur_montant_recu',
            'suspension_de_paiement', 'erreur_numero'
        ]) THEN 'cas_speciaux'
        WHEN (ce.individual_category)::text = ANY(ARRAY[
            'autres', 'carte_expiree', 'carte_bloquee',
            'code_pin', 'telephone', 'comment',
            'demande_dinformations', 'felicitations',
            'demande_de_paiement', 'paiement_non_recu',
            'deces', 'aide_medicale',
            'destruction_abris_par_catastrophe',
            'deplacement_du_menage', 'erreur_operateur_paiement',
            'suggestion'
        ]) THEN 'cas_non_sensibles'
        ELSE 'uncategorized'
    END"""

GRIEVANCE_VIEWS = {
    # ─── DETAIL VIEW ───────────────────────────────────────────────
    # Replaces: dashboard_grievance_category, _category_details,
    #           _category_summary, _channel, _channels, _priority, _status
    # One row per (ticket × expanded_category) with all dimensions.
    # The service layer groups/filters as needed.
    'dashboard_grievance_details': {
        'sql': f'''CREATE MATERIALIZED VIEW dashboard_grievance_details AS
WITH
-- Total ticket count (for percentage calculations)
ticket_total AS (
    SELECT COUNT(*) AS total
    FROM grievance_social_protection_ticket
    WHERE "isDeleted" = false
),
-- Expand JSON array categories into individual rows
category_expanded AS (
    SELECT
        t."UUID" AS id,
        t.status,
        t.channel,
        t.priority,
        t."Json_ext"->'reporter'->>'is_anonymous' AS is_anonymous,
        t."DateCreated" AS date_created,
        t."DateUpdated" AS date_updated,
        t."Json_ext"->'resolution_initial'->>'is_resolved' AS is_resolved,
        t.date_of_incident,
        CASE
            WHEN (t.category)::text LIKE '[%'
            THEN (TRIM(BOTH '"' FROM elem.value))::varchar
            ELSE t.category
        END AS individual_category
    FROM grievance_social_protection_ticket t
    LEFT JOIN LATERAL json_array_elements_text(
        CASE
            WHEN (t.category)::text LIKE '[%' THEN (t.category)::json
            ELSE '[""]'::json
        END
    ) elem(value) ON true
    WHERE t."isDeleted" = false
      AND t.category IS NOT NULL
      AND (t.category)::text <> ''
      AND ((t.category)::text NOT LIKE '[%' OR elem.value IS NOT NULL)
),
-- Map individual categories to groups
ce AS (
    SELECT *
    FROM category_expanded
)
SELECT
    ce.id,
    ce.status,
    ce.channel,
    ce.priority,
    ce.is_anonymous,
    ce.date_created,
    ce.date_updated,
    ce.is_resolved,
    ce.date_of_incident,
    ce.individual_category,
    {_CATEGORY_GROUP_CASE} AS category_group,
    EXTRACT(year FROM ce.date_created) AS year,
    EXTRACT(month FROM ce.date_created) AS month,
    EXTRACT(quarter FROM ce.date_created) AS quarter,
    -- Resolution time in days (only for resolved tickets)
    CASE
        WHEN ce.is_resolved = 'oui' AND ce.date_updated IS NOT NULL
        THEN EXTRACT(epoch FROM (ce.date_updated - ce.date_created)) / 86400.0
        ELSE NULL
    END AS resolution_days
FROM ce''',
        'indexes': [
            """CREATE INDEX idx_grievance_details_status ON dashboard_grievance_details USING btree (status);""",
            """CREATE INDEX idx_grievance_details_channel ON dashboard_grievance_details USING btree (channel);""",
            """CREATE INDEX idx_grievance_details_priority ON dashboard_grievance_details USING btree (priority);""",
            """CREATE INDEX idx_grievance_details_category ON dashboard_grievance_details USING btree (individual_category);""",
            """CREATE INDEX idx_grievance_details_group ON dashboard_grievance_details USING btree (category_group);""",
            """CREATE INDEX idx_grievance_details_year_month ON dashboard_grievance_details USING btree (year, month);""",
            """CREATE INDEX idx_grievance_details_id ON dashboard_grievance_details USING btree (id);""",
        ]
    },

    # ─── SUMMARY VIEW ──────────────────────────────────────────────
    # Single-row master summary with status counts and resolution metrics
    'dashboard_grievances': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_grievances AS
SELECT
    COUNT(*) AS total_tickets,
    COUNT(*) FILTER (WHERE status = 'OPEN') AS open_tickets,
    COUNT(*) FILTER (WHERE status = 'IN_PROGRESS') AS in_progress_tickets,
    COUNT(*) FILTER (WHERE status = 'RESOLVED') AS resolved_tickets,
    COUNT(*) FILTER (WHERE status = 'CLOSED') AS closed_tickets,
    COUNT(*) FILTER (WHERE
        (category)::text LIKE ANY(ARRAY[
            '%"discrimination"%', '%"abus_de_pouvoir"%',
            '%"corruption_sollicitation_pot_de_vin"%',
            '%"violence_agression_physique"%', '%"exclusion_du_programme"%',
            '%"violence_vbg"%', '%"corruption"%',
            '%"discrimination_ethnie_religion"%'
        ])
    ) AS sensitive_tickets,
    COUNT(*) FILTER (WHERE "Json_ext"->'reporter'->>'is_anonymous' = 'oui') AS anonymous_tickets,
    AVG(
        CASE
            WHEN "Json_ext"->'resolution_initial'->>'is_resolved' = 'oui' AND "DateUpdated" IS NOT NULL
            THEN EXTRACT(epoch FROM ("DateUpdated" - "DateCreated")) / 86400.0
            ELSE NULL
        END
    ) AS avg_resolution_days,
    CURRENT_DATE AS report_date
FROM grievance_social_protection_ticket
WHERE "isDeleted" = false''',
        'indexes': []
    },
}
