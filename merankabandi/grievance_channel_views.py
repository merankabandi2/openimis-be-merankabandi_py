"""
Grievance Channel Materialized Views with Multi-Value Support
"""

# SQL definitions for grievance channel views that handle space-separated multi-value channels
GRIEVANCE_CHANNEL_VIEWS = {
    'dashboard_grievance_channel_summary': """
    CREATE MATERIALIZED VIEW dashboard_grievance_channel_summary AS
    WITH channel_expanded AS (
        -- Extract individual channels from space-separated values
        SELECT 
            t."UUID" as id,
            t.status,
            TRIM(channel_value) as individual_channel
        FROM grievance_social_protection_ticket t
        CROSS JOIN LATERAL unnest(
            -- Split on spaces and filter out empty strings
            string_to_array(
                TRIM(regexp_replace(COALESCE(t.channel, ''), E'[\\s\\r\\n\\t]+', ' ', 'g')), 
                ' '
            )
        ) AS channel_value
        WHERE t."isDeleted" = false 
          AND t.channel IS NOT NULL
          AND TRIM(t.channel) != ''
          AND TRIM(channel_value) != ''
          AND LENGTH(TRIM(channel_value)) > 1
    ),
    channel_mapped AS (
        -- Map individual channels to standard values and count occurrences
        SELECT 
            id,
            status,
            individual_channel,
            CASE 
                -- Normalize channel names
                WHEN individual_channel IN ('telephone', 'tel') THEN 'telephone'
                WHEN individual_channel IN ('en_personne', 'personne') THEN 'en_personne'
                WHEN individual_channel IN ('sms', 'text') THEN 'sms'
                WHEN individual_channel IN ('courrier_simple', 'courrier') THEN 'courrier_simple'
                WHEN individual_channel IN ('courrier_electronique', 'email') THEN 'courrier_electronique'
                WHEN individual_channel IN ('ligne_verte', 'hotline') THEN 'ligne_verte'
                WHEN individual_channel IN ('boite_suggestion', 'suggestion_box') THEN 'boite_suggestion'
                WHEN individual_channel IN ('autre', 'other') THEN 'autre'
                ELSE individual_channel
            END as normalized_channel
        FROM channel_expanded
    )
    SELECT 
        normalized_channel as channel,
        COUNT(DISTINCT id) as ticket_count,
        COUNT(*) as total_mentions,  -- Total times this channel was mentioned (including multi-channel tickets)
        COUNT(DISTINCT id)::numeric / (
            SELECT COUNT(DISTINCT "UUID") 
            FROM grievance_social_protection_ticket 
            WHERE "isDeleted" = false AND channel IS NOT NULL
        )::numeric * 100 as ticket_percentage,
        COUNT(*)::numeric / (
            SELECT COUNT(*) 
            FROM (
                SELECT TRIM(val) as channel_mention
                FROM grievance_social_protection_ticket t
                CROSS JOIN LATERAL regexp_split_to_table(COALESCE(t.channel, ''), E'\\s+') AS val
                WHERE t."isDeleted" = false AND TRIM(val) != ''
            ) mentions
        )::numeric * 100 as mention_percentage,
        CURRENT_DATE as report_date
    FROM channel_mapped
    GROUP BY normalized_channel
    ORDER BY ticket_count DESC;
    """,
    
    'dashboard_grievance_channel_details': """
    CREATE MATERIALIZED VIEW dashboard_grievance_channel_details AS
    WITH channel_expanded AS (
        -- Extract individual channels from space-separated values with ticket details
        SELECT 
            t."UUID" as id,
            t.status,
            COALESCE(t.code, '') as code,
            t.date_of_incident,
            t.channel as original_channel,
            TRIM(channel_value) as individual_channel
        FROM grievance_social_protection_ticket t
        CROSS JOIN LATERAL unnest(
            -- Split on spaces and filter out empty strings
            string_to_array(
                TRIM(regexp_replace(COALESCE(t.channel, ''), E'[\\s\\r\\n\\t]+', ' ', 'g')), 
                ' '
            )
        ) AS channel_value
        WHERE t."isDeleted" = false 
          AND t.channel IS NOT NULL
          AND TRIM(t.channel) != ''
          AND TRIM(channel_value) != ''
          AND LENGTH(TRIM(channel_value)) > 1
    ),
    channel_mapped AS (
        -- Map individual channels to standard values
        SELECT 
            *,
            CASE 
                -- Normalize channel names
                WHEN individual_channel IN ('telephone', 'tel') THEN 'telephone'
                WHEN individual_channel IN ('en_personne', 'personne') THEN 'en_personne'
                WHEN individual_channel IN ('sms', 'text') THEN 'sms'
                WHEN individual_channel IN ('courrier_simple', 'courrier') THEN 'courrier_simple'
                WHEN individual_channel IN ('courrier_electronique', 'email') THEN 'courrier_electronique'
                WHEN individual_channel IN ('ligne_verte', 'hotline') THEN 'ligne_verte'
                WHEN individual_channel IN ('boite_suggestion', 'suggestion_box') THEN 'boite_suggestion'
                WHEN individual_channel IN ('autre', 'other') THEN 'autre'
                ELSE individual_channel
            END as normalized_channel,
            -- Count how many channels this ticket uses
            (SELECT array_length(
                regexp_split_to_array(COALESCE(t2.channel, ''), E'\\s+'), 1
            ) FROM grievance_social_protection_ticket t2 WHERE t2."UUID" = channel_expanded.id) as channel_count
        FROM channel_expanded
    )
    SELECT 
        individual_channel,
        normalized_channel,
        status,
        original_channel,
        channel_count,
        COUNT(DISTINCT id) as ticket_count,
        COUNT(*) as total_mentions,
        ROUND(AVG(channel_count), 2) as avg_channels_per_ticket,
        COUNT(DISTINCT id)::numeric / (
            SELECT COUNT(DISTINCT "UUID") 
            FROM grievance_social_protection_ticket 
            WHERE "isDeleted" = false AND channel IS NOT NULL
        )::numeric * 100 as percentage,
        CURRENT_DATE as report_date
    FROM channel_mapped
    GROUP BY individual_channel, normalized_channel, status, original_channel, channel_count
    ORDER BY ticket_count DESC;
    """
}