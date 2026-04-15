-- Gold Layer: Daily digest view with market analytics
-- Provides the ranked job list for the morning Slack alert
-- and aggregated metrics for the dashboard.

{{ config(materialized='view') }}

SELECT
    d.id AS digest_id,
    d.digest_date,
    d.rank_position,
    d.digest_sent,

    s.title,
    s.company,
    s.location,
    s.salary_min,
    s.salary_max,
    s.url,
    s.source,
    s.role_score,
    s.skills_match_score,
    s.overall_score,
    s.matched_skills,
    s.missing_skills,
    s.posted_date,

    -- Application status (if flagged)
    a.status AS application_status,
    a.flagged_at,
    a.applied_at,

    -- Derived fields
    CASE
        WHEN s.salary_min IS NOT NULL AND s.salary_max IS NOT NULL
        THEN (s.salary_min + s.salary_max) / 2
    END AS salary_midpoint,

    CASE
        WHEN s.overall_score >= 80 THEN 'Top Match'
        WHEN s.overall_score >= 60 THEN 'Good Match'
        ELSE 'Worth Reviewing'
    END AS match_label

FROM {{ source('gold', 'daily_digest') }} d
JOIN {{ source('silver', 'classified_jobs') }} s ON d.silver_id = s.id
LEFT JOIN {{ source('gold', 'application_tracker') }} a ON a.silver_id = s.id

ORDER BY d.digest_date DESC, d.rank_position ASC
