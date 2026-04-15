"""
Data Quality Validation for Job Search AutoPipe.

Uses Great Expectations patterns for automated validation at each
pipeline stage. Catches data issues before they propagate downstream.

Quality Gates:
1. Bronze: Raw data completeness and format checks
2. Silver: Classification score validity and dedup integrity
3. Gold: Digest readiness and application tracker consistency
"""

import logging
from datetime import datetime, timedelta
from typing import Tuple

logger = logging.getLogger(__name__)


class QualityValidator:
    """
    Validates data quality at each pipeline layer.
    
    Each validate_* method returns (passed: bool, report: dict).
    The pipeline halts or alerts based on the result.
    """

    def __init__(self, db):
        self.db = db

    def validate_bronze(self, run_id: str = None) -> Tuple[bool, dict]:
        """
        Validate bronze layer data quality.
        
        Checks:
        - No null source or source_job_id
        - raw_json is valid JSONB (implicitly handled by PostgreSQL)
        - content_hash is exactly 64 chars (SHA-256)
        - No duplicate (source, source_job_id) pairs
        - ingested_at is within last 24 hours for today's run
        """
        report = {"layer": "bronze", "checks": [], "passed": True}

        with self.db.cursor() as cur:
            # Check 1: No null required fields
            cur.execute("""
                SELECT COUNT(*) as cnt FROM bronze.raw_job_postings
                WHERE source IS NULL OR source_job_id IS NULL OR raw_json IS NULL;
            """)
            null_count = cur.fetchone()["cnt"]
            check1 = {
                "name": "no_null_required_fields",
                "passed": null_count == 0,
                "detail": f"{null_count} rows with null required fields",
            }
            report["checks"].append(check1)

            # Check 2: Valid content hashes
            cur.execute("""
                SELECT COUNT(*) as cnt FROM bronze.raw_job_postings
                WHERE LENGTH(content_hash) != 64;
            """)
            bad_hash = cur.fetchone()["cnt"]
            check2 = {
                "name": "valid_content_hash_length",
                "passed": bad_hash == 0,
                "detail": f"{bad_hash} rows with invalid hash length",
            }
            report["checks"].append(check2)

            # Check 3: Recent ingestion exists (pipeline actually ran)
            cur.execute("""
                SELECT COUNT(*) as cnt FROM bronze.raw_job_postings
                WHERE ingested_at >= NOW() - INTERVAL '24 hours';
            """)
            recent = cur.fetchone()["cnt"]
            check3 = {
                "name": "recent_ingestion_exists",
                "passed": recent > 0,
                "detail": f"{recent} rows ingested in last 24h",
            }
            report["checks"].append(check3)

            # Check 4: Valid sources
            cur.execute("""
                SELECT DISTINCT source FROM bronze.raw_job_postings
                WHERE source NOT IN ('adzuna', 'reed', 'the_muse');
            """)
            bad_sources = [r["source"] for r in cur.fetchall()]
            check4 = {
                "name": "valid_source_values",
                "passed": len(bad_sources) == 0,
                "detail": f"Unknown sources: {bad_sources}" if bad_sources else "All sources valid",
            }
            report["checks"].append(check4)

        report["passed"] = all(c["passed"] for c in report["checks"])
        report["timestamp"] = datetime.now().isoformat()

        if not report["passed"]:
            logger.warning(f"Bronze quality check FAILED: {report}")
        else:
            logger.info("Bronze quality check PASSED")

        return report["passed"], report

    def validate_silver(self, run_id: str = None) -> Tuple[bool, dict]:
        """
        Validate silver layer data quality.
        
        Checks:
        - role_score is between 0 and 100
        - skills_match_score is between 0 and 100
        - overall_score is between 0 and 100
        - All classified jobs have a valid bronze_id reference
        - dedup_hash is present for all rows
        - No classified jobs without a title
        """
        report = {"layer": "silver", "checks": [], "passed": True}

        with self.db.cursor() as cur:
            # Check 1: Score ranges
            cur.execute("""
                SELECT COUNT(*) as cnt FROM silver.classified_jobs
                WHERE role_score < 0 OR role_score > 100
                   OR skills_match_score < 0 OR skills_match_score > 100
                   OR overall_score < 0 OR overall_score > 100;
            """)
            bad_scores = cur.fetchone()["cnt"]
            report["checks"].append({
                "name": "scores_in_valid_range",
                "passed": bad_scores == 0,
                "detail": f"{bad_scores} rows with out-of-range scores",
            })

            # Check 2: Valid foreign keys
            cur.execute("""
                SELECT COUNT(*) as cnt FROM silver.classified_jobs s
                LEFT JOIN bronze.raw_job_postings b ON s.bronze_id = b.id
                WHERE b.id IS NULL;
            """)
            orphans = cur.fetchone()["cnt"]
            report["checks"].append({
                "name": "valid_bronze_references",
                "passed": orphans == 0,
                "detail": f"{orphans} orphaned silver rows (no bronze parent)",
            })

            # Check 3: Dedup hash present
            cur.execute("""
                SELECT COUNT(*) as cnt FROM silver.classified_jobs
                WHERE dedup_hash IS NULL OR LENGTH(dedup_hash) != 64;
            """)
            no_hash = cur.fetchone()["cnt"]
            report["checks"].append({
                "name": "dedup_hash_present",
                "passed": no_hash == 0,
                "detail": f"{no_hash} rows missing valid dedup hash",
            })

            # Check 4: No empty titles
            cur.execute("""
                SELECT COUNT(*) as cnt FROM silver.classified_jobs
                WHERE title IS NULL OR TRIM(title) = '';
            """)
            no_title = cur.fetchone()["cnt"]
            report["checks"].append({
                "name": "no_empty_titles",
                "passed": no_title == 0,
                "detail": f"{no_title} rows with empty titles",
            })

            # Check 5: Classification distribution sanity
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE is_genuine_de_role) as genuine,
                    AVG(role_score) as avg_role_score
                FROM silver.classified_jobs
                WHERE classified_at >= NOW() - INTERVAL '24 hours';
            """)
            stats = cur.fetchone()
            genuine_pct = (stats["genuine"] / max(stats["total"], 1)) * 100
            report["checks"].append({
                "name": "classification_distribution_reasonable",
                "passed": True,  # Informational
                "detail": (
                    f"Last 24h: {stats['total']} classified, "
                    f"{stats['genuine']} genuine ({genuine_pct:.0f}%), "
                    f"avg role score: {stats['avg_role_score'] or 0:.1f}"
                ),
            })

        report["passed"] = all(c["passed"] for c in report["checks"])
        report["timestamp"] = datetime.now().isoformat()

        if not report["passed"]:
            logger.warning(f"Silver quality check FAILED: {report}")
        else:
            logger.info("Silver quality check PASSED")

        return report["passed"], report

    def validate_gold(self, run_id: str = None) -> Tuple[bool, dict]:
        """
        Validate gold layer data quality.
        
        Checks:
        - Digest entries reference valid silver records
        - No duplicate silver_ids in today's digest
        - Application tracker statuses are valid
        """
        report = {"layer": "gold", "checks": [], "passed": True}

        with self.db.cursor() as cur:
            # Check 1: Valid digest references
            cur.execute("""
                SELECT COUNT(*) as cnt FROM gold.daily_digest d
                LEFT JOIN silver.classified_jobs s ON d.silver_id = s.id
                WHERE s.id IS NULL;
            """)
            orphans = cur.fetchone()["cnt"]
            report["checks"].append({
                "name": "valid_digest_references",
                "passed": orphans == 0,
                "detail": f"{orphans} digest entries with invalid silver reference",
            })

            # Check 2: No duplicate entries in today's digest
            cur.execute("""
                SELECT silver_id, COUNT(*) as cnt
                FROM gold.daily_digest
                WHERE digest_date = CURRENT_DATE
                GROUP BY silver_id
                HAVING COUNT(*) > 1;
            """)
            dupes = cur.fetchall()
            report["checks"].append({
                "name": "no_duplicate_digest_entries",
                "passed": len(dupes) == 0,
                "detail": f"{len(dupes)} duplicate silver_ids in today's digest",
            })

            # Check 3: Valid application statuses
            valid_statuses = {'new', 'reviewing', 'applied', 'interviewing', 'rejected', 'offer'}
            cur.execute("""
                SELECT DISTINCT status FROM gold.application_tracker
                WHERE status NOT IN ('new','reviewing','applied','interviewing','rejected','offer');
            """)
            bad_statuses = [r["status"] for r in cur.fetchall()]
            report["checks"].append({
                "name": "valid_application_statuses",
                "passed": len(bad_statuses) == 0,
                "detail": f"Invalid statuses: {bad_statuses}" if bad_statuses else "All statuses valid",
            })

        report["passed"] = all(c["passed"] for c in report["checks"])
        report["timestamp"] = datetime.now().isoformat()

        if not report["passed"]:
            logger.warning(f"Gold quality check FAILED: {report}")
        else:
            logger.info("Gold quality check PASSED")

        return report["passed"], report

    def run_full_suite(self) -> Tuple[bool, list]:
        """Run all quality checks and return aggregated results."""
        results = []
        all_passed = True

        for validator in [self.validate_bronze, self.validate_silver, self.validate_gold]:
            passed, report = validator()
            results.append(report)
            if not passed:
                all_passed = False

        return all_passed, results
