-- Bronze Layer: Raw job postings as-ingested
-- This is a view over the raw table for dbt lineage tracking.

{{ config(materialized='view') }}

SELECT
    id,
    source,
    source_job_id,
    content_hash,
    raw_json,
    raw_json->>'title'                          AS title_raw,
    raw_json->'company'->>'display_name'        AS company_raw,
    raw_json->'location'->>'display_name'       AS location_raw,
    raw_json->>'description'                    AS description_raw,
    raw_json->>'redirect_url'                   AS url_raw,
    (raw_json->>'salary_min')::NUMERIC          AS salary_min_raw,
    (raw_json->>'salary_max')::NUMERIC          AS salary_max_raw,
    ingested_at

FROM {{ source('bronze', 'raw_job_postings') }}
