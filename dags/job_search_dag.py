"""
Job Search AutoPipe — Airflow DAG

Orchestrates the full pipeline daily:
  1. Ingest  → Pull new listings from job APIs (Bronze)
  2. Classify → Score and filter genuine DE roles (Silver)
  3. Quality → Run data validation suite
  4. Digest  → Send ranked Slack alert (Gold)

Cover letter generation is triggered separately when the user
flags a job for application — it's not part of the daily run.

Schedule: 7:00 AM daily (configurable in config.yaml)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

import sys
import os
import json
import logging

# Ensure src is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logger = logging.getLogger(__name__)


# ── Default DAG arguments ──────────────────────────────────────────

default_args = {
    "owner": "autopipe",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}


# ── Task Functions ─────────────────────────────────────────────────

def task_ingest(**context):
    """
    Phase 1: Pull new job listings from all configured APIs.
    Stores raw JSON in bronze.raw_job_postings.
    """
    from src.utils.config_loader import PipelineConfig, load_skills_profile
    from src.utils.database import Database
    from src.ingestion.adzuna_client import AdzunaClient
    from src.ingestion.reed_client import ReedClient

    config = PipelineConfig.from_yaml()
    db = Database(config.database)

    all_keywords = config.search.primary_keywords + config.search.secondary_keywords
    total_ingested = 0
    total_dupes = 0

    # --- Adzuna ---
    if config.apis.get("adzuna", {}).get("app_id"):
        client = AdzunaClient(config.apis["adzuna"])
        postings = client.fetch_jobs(
            keywords=all_keywords,
            location=config.search.city,
            radius_miles=config.search.radius_miles,
            posted_within_days=config.search.posted_within_days,
        )
        for p in postings:
            result = db.insert_raw_posting(p.source, p.source_job_id, p.raw_json)
            if result:
                total_ingested += 1
            else:
                total_dupes += 1
        logger.info(f"Adzuna: {len(postings)} fetched, {total_ingested} new")

    # --- Reed ---
    if config.apis.get("reed", {}).get("api_key"):
        client = ReedClient(config.apis["reed"])
        postings = client.fetch_jobs(
            keywords=all_keywords,
            location=config.search.city,
            radius_miles=config.search.radius_miles,
        )
        for p in postings:
            result = db.insert_raw_posting(p.source, p.source_job_id, p.raw_json)
            if result:
                total_ingested += 1
            else:
                total_dupes += 1
        logger.info(f"Reed: {len(postings)} fetched")

    # Log pipeline run
    run_id = context.get("run_id", "manual")
    db.log_pipeline_run(
        dag_id="job_search_autopipe",
        run_id=run_id,
        phase="ingestion",
        status="success",
        records_in=total_ingested + total_dupes,
        records_out=total_ingested,
    )

    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="ingested_count", value=total_ingested)
    logger.info(f"Ingestion complete: {total_ingested} new, {total_dupes} duplicates skipped")


def task_classify(**context):
    """
    Phase 2: Classify and score unprocessed bronze postings.
    Writes results to silver.classified_jobs.
    """
    from src.utils.config_loader import PipelineConfig, load_skills_profile
    from src.utils.database import Database
    from src.transformation.role_classifier import RoleClassifier
    from src.transformation.skills_matcher import SkillsMatcher

    config = PipelineConfig.from_yaml()
    db = Database(config.database)
    profile = load_skills_profile()

    classifier = RoleClassifier(config.classifier)
    matcher = SkillsMatcher(profile)

    # Get unclassified postings from bronze
    unclassified = db.get_unclassified_postings(limit=500)
    logger.info(f"Found {len(unclassified)} unclassified postings")

    classified_count = 0
    genuine_count = 0

    for row in unclassified:
        raw = row["raw_json"] if isinstance(row["raw_json"], dict) else json.loads(row["raw_json"])

        # Extract fields from raw JSON (source-dependent)
        title = raw.get("title", raw.get("jobTitle", ""))
        company = raw.get("company", {}).get("display_name", raw.get("employerName", "Unknown"))
        if isinstance(company, dict):
            company = company.get("display_name", "Unknown")
        location = raw.get("location", {}).get("display_name", raw.get("locationName", ""))
        if isinstance(location, dict):
            location = location.get("display_name", "")
        description = raw.get("description", raw.get("jobDescription", ""))
        url = raw.get("redirect_url", raw.get("jobUrl", ""))
        salary_min = raw.get("salary_min", raw.get("minimumSalary"))
        salary_max = raw.get("salary_max", raw.get("maximumSalary"))
        posted_date_raw = raw.get("created", raw.get("date"))
        posted_date = None
        if posted_date_raw:
            try:
                if 'T' in str(posted_date_raw):
                    posted_date = datetime.fromisoformat(str(posted_date_raw).replace('Z', '+00:00')).date()
                elif '/' in str(posted_date_raw):
                    posted_date = datetime.strptime(str(posted_date_raw), '%d/%m/%Y').date()
                else:
                    posted_date = datetime.strptime(str(posted_date_raw), '%Y-%m-%d').date()
            except (ValueError, TypeError):
                posted_date = None

        # Clean description
        import re
        desc_clean = re.sub(r"<[^>]+>", " ", description or "")
        desc_clean = re.sub(r"\s+", " ", desc_clean).strip()

        # Classify role
        role_score, is_genuine, _ = classifier.classify(title, desc_clean)

        # Match skills
        skills_score, matched, missing = matcher.match(title, desc_clean)

        # Overall score
        overall = matcher.compute_overall_score(role_score, skills_score)

        # Dedup hash
        dedup_hash = matcher.compute_dedup_hash(title, company)

        # Check for cross-source duplicates
        is_duplicate = False
        with db.cursor() as cur:
            cur.execute(
                "SELECT id FROM silver.classified_jobs WHERE dedup_hash = %s LIMIT 1;",
                (dedup_hash,)
            )
            if cur.fetchone():
                is_duplicate = True

        # Insert into silver
        db.insert_classified_job({
            "bronze_id": row["id"],
            "title": title[:500],
            "company": company[:500] if isinstance(company, str) else "Unknown",
            "location": location[:500] if isinstance(location, str) else "",
            "salary_min": int(salary_min) if salary_min else None,
            "salary_max": int(salary_max) if salary_max else None,
            "description_clean": desc_clean,
            "url": url,
            "posted_date": posted_date,
            "source": row["source"],
            "role_score": role_score,
            "skills_match_score": skills_score,
            "overall_score": overall,
            "is_genuine_de_role": is_genuine,
            "matched_skills": matched,
            "missing_skills": missing,
            "dedup_hash": dedup_hash,
            "is_duplicate": is_duplicate,
        })

        classified_count += 1
        if is_genuine and not is_duplicate:
            genuine_count += 1

    run_id = context.get("run_id", "manual")
    db.log_pipeline_run(
        dag_id="job_search_autopipe",
        run_id=run_id,
        phase="classification",
        status="success",
        records_in=len(unclassified),
        records_out=genuine_count,
    )

    context["ti"].xcom_push(key="classified_count", value=classified_count)
    context["ti"].xcom_push(key="genuine_count", value=genuine_count)
    logger.info(f"Classification complete: {classified_count} processed, {genuine_count} genuine DE roles")


def task_quality_check(**context):
    """
    Phase 2.5: Run data quality validation suite.
    Alerts on failures but doesn't block the digest.
    """
    from src.utils.config_loader import PipelineConfig
    from src.utils.database import Database
    from src.utils.notifier_factory import create_notifier
    from src.quality.expectations import QualityValidator

    config = PipelineConfig.from_yaml()
    db = Database(config.database)
    validator = QualityValidator(db)

    all_passed, results = validator.run_full_suite()

    # Send alert if any checks failed
    if not all_passed:
        notifier = create_notifier(config.notifications)
        if notifier:
            failed_checks = []
            for report in results:
                for check in report["checks"]:
                    if not check["passed"]:
                        failed_checks.append(f"[{report['layer']}] {check['name']}: {check['detail']}")

            notifier.send_pipeline_alert(
                phase="quality_check",
                status="failed",
                error="\n".join(failed_checks),
            )

    context["ti"].xcom_push(key="quality_passed", value=all_passed)
    logger.info(f"Quality check {'PASSED' if all_passed else 'FAILED'}")


def task_send_digest(**context):
    """
    Phase 3: Create and send the daily job digest.
    """
    from src.utils.config_loader import PipelineConfig
    from src.utils.database import Database
    from src.utils.notifier_factory import create_notifier

    config = PipelineConfig.from_yaml()
    db = Database(config.database)

    # Get top candidates
    candidates = db.get_digest_candidates(limit=20)
    logger.info(f"Found {len(candidates)} digest candidates")

    if candidates:
        # Create digest records
        silver_ids = [c["id"] for c in candidates]
        db.create_digest(silver_ids)

        # Format for notification
        digest_jobs = []
        for rank, job in enumerate(candidates, 1):
            matched = job.get("matched_skills", [])
            if isinstance(matched, str):
                matched = json.loads(matched)

            digest_jobs.append({
                "rank": rank,
                "id": job["id"],
                "title": job["title"],
                "company": job["company"],
                "location": job["location"],
                "overall_score": job["overall_score"],
                "skills_match_score": job["skills_match_score"],
                "url": job["url"],
                "matched_skills": matched,
            })

        # Send digest via configured channel(s)
        notifier = create_notifier(config.notifications)
        if notifier:
            notifier.send_daily_digest(digest_jobs)

    run_id = context.get("run_id", "manual")
    db.log_pipeline_run(
        dag_id="job_search_autopipe",
        run_id=run_id,
        phase="digest",
        status="success",
        records_in=len(candidates),
        records_out=len(candidates),
    )

    logger.info(f"Digest sent with {len(candidates)} jobs")


# ── DAG Definition ─────────────────────────────────────────────────

with DAG(
    dag_id="job_search_autopipe",
    default_args=default_args,
    description="Daily job discovery, classification, and digest pipeline",
    schedule_interval="0 7 * * *",  # 7 AM daily
    catchup=False,
    max_active_runs=1,
    tags=["job-search", "data-engineering", "portfolio"],
) as dag:

    start = DummyOperator(task_id="start")

    ingest = PythonOperator(
        task_id="ingest_job_listings",
        python_callable=task_ingest,
        provide_context=True,
    )

    classify = PythonOperator(
        task_id="classify_and_score",
        python_callable=task_classify,
        provide_context=True,
    )

    quality = PythonOperator(
        task_id="quality_check",
        python_callable=task_quality_check,
        provide_context=True,
    )

    digest = PythonOperator(
        task_id="send_daily_digest",
        python_callable=task_send_digest,
        provide_context=True,
    )

    end = DummyOperator(task_id="end")

    # Pipeline flow
    start >> ingest >> classify >> quality >> digest >> end
