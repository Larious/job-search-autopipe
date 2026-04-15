-- ============================================================
-- Gold Layer: Personal ATS Analytics
-- ============================================================
-- Tracks application outcomes and pipeline conversion rates.
-- Answers: What's working? Which match scores convert?
-- Where do applications go to die?
--
-- This is the "analytics engineering flex" — it closes the
-- feedback loop and turns raw tracking data into actionable
-- insights about your job search strategy.
-- ============================================================

{{ config(materialized='view') }}

WITH applications AS (
    SELECT
        a.id AS tracker_id,
        a.silver_id,
        a.status,
        a.flagged_at,
        a.applied_at,
        a.response_at,
        a.interview_at,
        a.rejected_at,
        a.offer_at,
        a.rejection_reason,
        a.interview_rounds,
        a.salary_offered,
        a.application_method,
        a.referral,
        a.days_to_response,
        a.updated_at,

        s.title,
        s.company,
        s.location,
        s.source,
        s.role_score,
        s.skills_match_score,
        s.overall_score,
        s.matched_skills,
        s.missing_skills,
        s.salary_min AS listed_salary_min,
        s.salary_max AS listed_salary_max,
        s.posted_date,

        -- Derived status categories
        CASE
            WHEN a.status = 'offer' THEN 'converted'
            WHEN a.status = 'interviewing' THEN 'active'
            WHEN a.status = 'applied' AND a.response_at IS NULL
                 AND a.applied_at < NOW() - INTERVAL '14 days' THEN 'ghosted'
            WHEN a.status = 'applied' AND a.response_at IS NULL THEN 'waiting'
            WHEN a.status = 'rejected' THEN 'rejected'
            ELSE a.status
        END AS outcome_category,

        -- Score buckets for conversion analysis
        CASE
            WHEN s.overall_score >= 80 THEN 'excellent (80-100)'
            WHEN s.overall_score >= 60 THEN 'good (60-79)'
            WHEN s.overall_score >= 40 THEN 'fair (40-59)'
            ELSE 'low (0-39)'
        END AS score_bucket,

        -- Week cohort for trending
        DATE_TRUNC('week', a.applied_at)::DATE AS applied_week

    FROM {{ source('gold', 'application_tracker') }} a
    JOIN {{ source('silver', 'classified_jobs') }} s ON a.silver_id = s.id
    WHERE a.status != 'new'  -- Exclude unflagged
)

SELECT
    *,

    -- Funnel stage (numeric for ordering)
    CASE outcome_category
        WHEN 'waiting'   THEN 1
        WHEN 'ghosted'   THEN 2
        WHEN 'rejected'  THEN 3
        WHEN 'active'    THEN 4
        WHEN 'converted' THEN 5
    END AS funnel_stage,

    -- Response rate flag
    (response_at IS NOT NULL)::INTEGER AS got_response,

    -- Interview conversion flag
    (interview_at IS NOT NULL)::INTEGER AS got_interview,

    -- Offer conversion flag
    (offer_at IS NOT NULL)::INTEGER AS got_offer

FROM applications
ORDER BY applied_at DESC
