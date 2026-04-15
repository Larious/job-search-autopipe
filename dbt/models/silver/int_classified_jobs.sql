-- Silver Layer: Classified jobs with scoring
-- Filters to genuine DE roles and removes cross-source duplicates.

{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        id,
        bronze_id,
        title,
        company,
        location,
        salary_min,
        salary_max,
        description_clean,
        url,
        posted_date,
        source,
        role_score,
        skills_match_score,
        overall_score,
        is_genuine_de_role,
        matched_skills,
        missing_skills,
        dedup_hash,
        is_duplicate,
        classified_at,

        -- Rank within dedup groups: keep highest-scoring version
        ROW_NUMBER() OVER (
            PARTITION BY dedup_hash
            ORDER BY overall_score DESC, classified_at ASC
        ) AS dedup_rank

    FROM {{ source('silver', 'classified_jobs') }}
    WHERE is_genuine_de_role = TRUE
)

SELECT
    id,
    bronze_id,
    title,
    company,
    location,
    salary_min,
    salary_max,
    description_clean,
    url,
    posted_date,
    source,
    role_score,
    skills_match_score,
    overall_score,
    matched_skills,
    missing_skills,
    dedup_hash,
    classified_at,

    -- Salary midpoint for analytics
    CASE
        WHEN salary_min IS NOT NULL AND salary_max IS NOT NULL
        THEN (salary_min + salary_max) / 2
        WHEN salary_min IS NOT NULL THEN salary_min
        WHEN salary_max IS NOT NULL THEN salary_max
    END AS salary_midpoint,

    -- Skills match tier for digest formatting
    CASE
        WHEN skills_match_score >= 80 THEN 'excellent'
        WHEN skills_match_score >= 60 THEN 'good'
        WHEN skills_match_score >= 40 THEN 'fair'
        ELSE 'low'
    END AS match_tier

FROM ranked
WHERE dedup_rank = 1
ORDER BY overall_score DESC
