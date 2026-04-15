"""
Database utilities for Job Search AutoPipe.
Manages PostgreSQL connections and schema setup (medallion architecture).
"""

import hashlib
import json
import logging
from datetime import datetime
from contextlib import contextmanager
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

logger = logging.getLogger(__name__)


# ── Schema Definition (Medallion Architecture) ────────────────────────

SCHEMA_SQL = """
-- ============================================================
-- BRONZE LAYER — Raw ingestion, untouched source data
-- ============================================================
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_job_postings (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(50) NOT NULL,          -- 'adzuna', 'reed', 'the_muse'
    source_job_id   VARCHAR(255) NOT NULL,         -- Original ID from source
    content_hash    VARCHAR(64) NOT NULL,          -- SHA-256 of raw_json
    raw_json        JSONB NOT NULL,                -- Full API response
    ingested_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE(source, source_job_id)
);

CREATE INDEX IF NOT EXISTS idx_bronze_content_hash
    ON bronze.raw_job_postings(content_hash);
CREATE INDEX IF NOT EXISTS idx_bronze_ingested_at
    ON bronze.raw_job_postings(ingested_at);


-- ============================================================
-- SILVER LAYER — Cleaned, classified, and scored
-- ============================================================
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.classified_jobs (
    id                  SERIAL PRIMARY KEY,
    bronze_id           INTEGER REFERENCES bronze.raw_job_postings(id),
    title               VARCHAR(500),
    company             VARCHAR(500),
    location            VARCHAR(500),
    salary_min          INTEGER,
    salary_max          INTEGER,
    description_clean   TEXT,                      -- Cleaned description text
    url                 VARCHAR(2000),
    posted_date         DATE,
    source              VARCHAR(50),

    -- Classification scores
    role_score          FLOAT DEFAULT 0,           -- 0-100: is this a DE role?
    skills_match_score  FLOAT DEFAULT 0,           -- 0-100: how well does it match you?
    overall_score       FLOAT DEFAULT 0,           -- Weighted composite
    is_genuine_de_role  BOOLEAN DEFAULT FALSE,      -- Passes role threshold?
    matched_skills      JSONB DEFAULT '[]',        -- Skills found in JD
    missing_skills      JSONB DEFAULT '[]',        -- Required skills you lack

    -- Deduplication
    dedup_hash          VARCHAR(64),               -- Hash of title+company for cross-source dedup
    is_duplicate        BOOLEAN DEFAULT FALSE,

    classified_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_overall_score
    ON silver.classified_jobs(overall_score DESC);
CREATE INDEX IF NOT EXISTS idx_silver_is_genuine
    ON silver.classified_jobs(is_genuine_de_role);
CREATE INDEX IF NOT EXISTS idx_silver_dedup_hash
    ON silver.classified_jobs(dedup_hash);


-- ============================================================
-- GOLD LAYER — Digest-ready, human-reviewed
-- ============================================================
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.daily_digest (
    id              SERIAL PRIMARY KEY,
    digest_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    silver_id       INTEGER REFERENCES silver.classified_jobs(id),
    rank_position   INTEGER,                       -- Rank in today's digest
    digest_sent     BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.application_tracker (
    id              SERIAL PRIMARY KEY,
    silver_id       INTEGER REFERENCES silver.classified_jobs(id),
    status          VARCHAR(50) DEFAULT 'new',     -- new, reviewing, applied,
                                                    -- interviewing, rejected, offer
    flagged_at      TIMESTAMP,                      -- When user flagged for application
    applied_at      TIMESTAMP,
    cover_letter    TEXT,                           -- Generated cover letter
    cv_notes        TEXT,                           -- Tailoring notes for CV
    notes           TEXT,                           -- User notes

    -- Outcome tracking (the analytics engineering flex)
    response_at     TIMESTAMP,                      -- When company first responded
    interview_at    TIMESTAMP,                      -- First interview date
    rejected_at     TIMESTAMP,
    offer_at        TIMESTAMP,
    rejection_reason VARCHAR(200),                  -- e.g. "experience", "visa", "ghosted"
    interview_rounds INTEGER DEFAULT 0,             -- How many rounds completed
    salary_offered  INTEGER,                        -- If offer received
    application_method VARCHAR(100),                -- "company_site", "linkedin", "indeed", etc.
    referral        BOOLEAN DEFAULT FALSE,           -- Was this a referral?
    
    -- Time-to-response analytics
    days_to_response INTEGER GENERATED ALWAYS AS (
        EXTRACT(DAY FROM (response_at - applied_at))::INTEGER
    ) STORED,

    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tracker_status
    ON gold.application_tracker(status);


-- ============================================================
-- PIPELINE METADATA — Observability
-- ============================================================
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(100),
    run_id          VARCHAR(255),
    phase           VARCHAR(50),                   -- ingestion, classification, digest, etc.
    status          VARCHAR(20),                   -- running, success, failed
    records_in      INTEGER DEFAULT 0,
    records_out     INTEGER DEFAULT 0,
    started_at      TIMESTAMP DEFAULT NOW(),
    completed_at    TIMESTAMP,
    error_message   TEXT
);
"""


class Database:
    """PostgreSQL connection manager for the pipeline."""

    def __init__(self, config: dict):
        self.config = config

    @contextmanager
    def connection(self):
        """Context manager for database connections."""
        conn = psycopg2.connect(
            host=self.config["host"],
            port=self.config["port"],
            dbname=self.config["name"],
            user=self.config["user"],
            password=self.config["password"],
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def cursor(self, dict_cursor=True):
        """Context manager for cursors."""
        with self.connection() as conn:
            cursor_factory = RealDictCursor if dict_cursor else None
            cur = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cur
                conn.commit()
            finally:
                cur.close()

    def initialize_schema(self):
        """Create all tables and schemas."""
        with self.cursor(dict_cursor=False) as cur:
            cur.execute(SCHEMA_SQL)
        logger.info("Database schema initialized successfully.")

    def insert_raw_posting(self, source: str, source_job_id: str, raw_json: dict) -> Optional[int]:
        """
        Insert a raw job posting into bronze layer.
        Returns the row ID, or None if duplicate (already exists).
        """
        content_hash = hashlib.sha256(
            json.dumps(raw_json, sort_keys=True).encode()
        ).hexdigest()

        sql = """
            INSERT INTO bronze.raw_job_postings (source, source_job_id, content_hash, raw_json)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (source, source_job_id) DO NOTHING
            RETURNING id;
        """
        with self.cursor() as cur:
            cur.execute(sql, (source, source_job_id, content_hash, json.dumps(raw_json)))
            result = cur.fetchone()
            return result["id"] if result else None

    def get_unclassified_postings(self, limit: int = 500) -> list:
        """Get bronze postings that haven't been classified yet."""
        sql = """
            SELECT b.id, b.source, b.source_job_id, b.raw_json
            FROM bronze.raw_job_postings b
            LEFT JOIN silver.classified_jobs s ON s.bronze_id = b.id
            WHERE s.id IS NULL
            ORDER BY b.ingested_at DESC
            LIMIT %s;
        """
        with self.cursor() as cur:
            cur.execute(sql, (limit,))
            return cur.fetchall()

    def insert_classified_job(self, data: dict) -> int:
        """Insert a classified job into silver layer."""
        sql = """
            INSERT INTO silver.classified_jobs (
                bronze_id, title, company, location, salary_min, salary_max,
                description_clean, url, posted_date, source,
                role_score, skills_match_score, overall_score,
                is_genuine_de_role, matched_skills, missing_skills,
                dedup_hash, is_duplicate
            ) VALUES (
                %(bronze_id)s, %(title)s, %(company)s, %(location)s,
                %(salary_min)s, %(salary_max)s, %(description_clean)s,
                %(url)s, %(posted_date)s, %(source)s,
                %(role_score)s, %(skills_match_score)s, %(overall_score)s,
                %(is_genuine_de_role)s, %(matched_skills)s, %(missing_skills)s,
                %(dedup_hash)s, %(is_duplicate)s
            )
            RETURNING id;
        """
        data["matched_skills"] = json.dumps(data.get("matched_skills", []))
        data["missing_skills"] = json.dumps(data.get("missing_skills", []))

        with self.cursor() as cur:
            cur.execute(sql, data)
            return cur.fetchone()["id"]

    def get_digest_candidates(self, limit: int = 20) -> list:
        """Get top-ranked genuine DE roles not yet in a digest."""
        sql = """
            SELECT s.*
            FROM silver.classified_jobs s
            LEFT JOIN gold.daily_digest d ON d.silver_id = s.id
            WHERE s.is_genuine_de_role = TRUE
              AND s.is_duplicate = FALSE
              AND d.id IS NULL
            ORDER BY s.overall_score DESC
            LIMIT %s;
        """
        with self.cursor() as cur:
            cur.execute(sql, (limit,))
            return cur.fetchall()

    def create_digest(self, silver_ids: list, digest_date=None) -> int:
        """Create a daily digest entry."""
        if digest_date is None:
            digest_date = datetime.now().date()

        sql = """
            INSERT INTO gold.daily_digest (digest_date, silver_id, rank_position)
            VALUES (%s, %s, %s);
        """
        with self.cursor() as cur:
            for rank, sid in enumerate(silver_ids, 1):
                cur.execute(sql, (digest_date, sid, rank))
        return len(silver_ids)

    def flag_for_application(self, silver_id: int) -> int:
        """Flag a job for application preparation."""
        sql = """
            INSERT INTO gold.application_tracker (silver_id, status, flagged_at)
            VALUES (%s, 'reviewing', NOW())
            ON CONFLICT DO NOTHING
            RETURNING id;
        """
        with self.cursor() as cur:
            cur.execute(sql, (silver_id,))
            result = cur.fetchone()
            return result["id"] if result else None

    def update_application(self, tracker_id: int, **kwargs):
        """Update application tracker fields."""
        allowed = {"status", "cover_letter", "cv_notes", "notes", "applied_at"}
        fields = {k: v for k, v in kwargs.items() if k in allowed}
        if not fields:
            return

        set_clause = ", ".join(f"{k} = %({k})s" for k in fields)
        fields["id"] = tracker_id
        sql = f"UPDATE gold.application_tracker SET {set_clause}, updated_at = NOW() WHERE id = %(id)s;"

        with self.cursor() as cur:
            cur.execute(sql, fields)

    def log_pipeline_run(self, dag_id: str, run_id: str, phase: str,
                         status: str, records_in: int = 0, records_out: int = 0,
                         error_message: str = None):
        """Log a pipeline execution for observability."""
        sql = """
            INSERT INTO meta.pipeline_runs
                (dag_id, run_id, phase, status, records_in, records_out, error_message, completed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, CASE WHEN %s IN ('success','failed') THEN NOW() END);
        """
        with self.cursor() as cur:
            cur.execute(sql, (dag_id, run_id, phase, status, records_in, records_out, error_message, status))
