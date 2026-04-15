#!/usr/bin/env python3
"""
Job Search AutoPipe — CLI Tool

Manual operations for the pipeline:
  - Flag a job for application
  - Generate a cover letter for a flagged job
  - View pipeline stats
  - Run a manual ingestion

Usage:
    python scripts/cli.py digest          # View today's digest
    python scripts/cli.py flag 42         # Flag silver job #42 for application
    python scripts/cli.py cover 42        # Generate cover letter for silver job #42
    python scripts/cli.py stats           # View pipeline statistics
    python scripts/cli.py init-db         # Initialize database schema
"""

import sys
import os
import json
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils.config_loader import PipelineConfig, load_skills_profile
from src.utils.database import Database


def cmd_init_db():
    """Initialize the database schema."""
    config = PipelineConfig.from_yaml()
    db = Database(config.database)
    db.initialize_schema()
    print("✅ Database schema initialized.")


def cmd_digest():
    """Show today's digest candidates."""
    config = PipelineConfig.from_yaml()
    db = Database(config.database)
    candidates = db.get_digest_candidates(limit=20)

    if not candidates:
        print("📭 No new matching jobs today.")
        return

    print(f"\n📬 Job Digest — {datetime.now().strftime('%A, %d %B %Y')}")
    print("=" * 70)

    for i, job in enumerate(candidates, 1):
        matched = job.get("matched_skills", [])
        if isinstance(matched, str):
            matched = json.loads(matched)

        score_icon = "🟢" if job["overall_score"] >= 80 else "🟡" if job["overall_score"] >= 60 else "🟠"

        print(f"\n#{i} {score_icon} {job['title']}")
        print(f"   🏢 {job['company']} · 📍 {job['location']}")
        print(f"   Score: {job['overall_score']:.0f}% · Skills Match: {job['skills_match_score']:.0f}%")
        if matched:
            print(f"   Matched: {', '.join(matched[:6])}")
        if job.get("salary_min") or job.get("salary_max"):
            sal = f"£{job.get('salary_min', '?'):,} - £{job.get('salary_max', '?'):,}"
            print(f"   💰 {sal}")
        print(f"   🔗 {job['url']}")
        print(f"   [Silver ID: {job['id']}]")

    print(f"\n{'=' * 70}")
    print(f"Use: python scripts/cli.py flag <silver_id>")


def cmd_flag(silver_id: int):
    """Flag a job for application preparation."""
    config = PipelineConfig.from_yaml()
    db = Database(config.database)
    tracker_id = db.flag_for_application(silver_id)

    if tracker_id:
        print(f"✅ Job #{silver_id} flagged for application (tracker #{tracker_id})")
        print(f"   Next: python scripts/cli.py cover {silver_id}")
    else:
        print(f"⚠️  Job #{silver_id} was already flagged.")


def cmd_cover(silver_id: int):
    """Generate a tailored cover letter for a flagged job."""
    from src.generation.cover_letter_generator import CoverLetterGenerator

    config = PipelineConfig.from_yaml()
    db = Database(config.database)
    profile = load_skills_profile()

    # Get job details from silver
    with db.cursor() as cur:
        cur.execute("SELECT * FROM silver.classified_jobs WHERE id = %s;", (silver_id,))
        job = cur.fetchone()

    if not job:
        print(f"❌ No job found with silver ID {silver_id}")
        return

    matched = job.get("matched_skills", [])
    missing = job.get("missing_skills", [])
    if isinstance(matched, str):
        matched = json.loads(matched)
    if isinstance(missing, str):
        missing = json.loads(missing)

    print(f"\n📝 Generating cover letter for:")
    print(f"   {job['title']} at {job['company']}")
    print(f"   Matched skills: {', '.join(matched[:6])}")
    print(f"   Missing skills: {', '.join(missing[:4])}")
    print()

    generator = CoverLetterGenerator(config.cover_letter, profile)
    body = generator.generate(
        job=dict(job),
        matched_skills=matched,
        missing_skills=missing,
    )

    # Format with header/footer
    full_letter = generator.format_full_letter(body, dict(job))

    # Save to tracker
    with db.cursor() as cur:
        cur.execute(
            "SELECT id FROM gold.application_tracker WHERE silver_id = %s;",
            (silver_id,)
        )
        tracker = cur.fetchone()
        if tracker:
            db.update_application(tracker["id"], cover_letter=full_letter)

    print("=" * 70)
    print(full_letter)
    print("=" * 70)

    # Also save to file
    filename = f"cover_letter_{job['company'].replace(' ', '_').lower()}_{silver_id}.txt"
    filepath = os.path.join(os.path.dirname(__file__), "..", "output", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w") as f:
        f.write(full_letter)
    print(f"\n💾 Saved to: output/{filename}")


def cmd_stats():
    """Show pipeline statistics."""
    config = PipelineConfig.from_yaml()
    db = Database(config.database)

    with db.cursor() as cur:
        # Bronze stats
        cur.execute("SELECT COUNT(*) as cnt, COUNT(DISTINCT source) as sources FROM bronze.raw_job_postings;")
        bronze = cur.fetchone()

        # Silver stats
        cur.execute("""
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE is_genuine_de_role) as genuine,
                   AVG(overall_score) as avg_score
            FROM silver.classified_jobs;
        """)
        silver = cur.fetchone()

        # Application stats
        cur.execute("""
            SELECT status, COUNT(*) as cnt
            FROM gold.application_tracker
            GROUP BY status
            ORDER BY cnt DESC;
        """)
        apps = cur.fetchall()

        # Recent pipeline runs
        cur.execute("""
            SELECT phase, status, records_in, records_out, completed_at
            FROM meta.pipeline_runs
            ORDER BY started_at DESC
            LIMIT 5;
        """)
        runs = cur.fetchall()

    print("\n📊 Pipeline Statistics")
    print("=" * 50)
    print(f"\n🥉 Bronze Layer:")
    print(f"   Total raw postings: {bronze['cnt']}")
    print(f"   Sources: {bronze['sources']}")

    print(f"\n🥈 Silver Layer:")
    print(f"   Total classified: {silver['total']}")
    print(f"   Genuine DE roles: {silver['genuine']}")
    print(f"   Avg overall score: {silver['avg_score'] or 0:.1f}%")

    print(f"\n🥇 Applications:")
    if apps:
        for app in apps:
            print(f"   {app['status']}: {app['cnt']}")
    else:
        print("   No applications tracked yet.")

    print(f"\n🔄 Recent Pipeline Runs:")
    for run in runs:
        print(f"   [{run['phase']}] {run['status']} — "
              f"in:{run['records_in']} out:{run['records_out']} "
              f"at {run['completed_at'] or 'running'}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "init-db":
        cmd_init_db()
    elif command == "digest":
        cmd_digest()
    elif command == "flag":
        if len(sys.argv) < 3:
            print("Usage: python scripts/cli.py flag <silver_id>")
            sys.exit(1)
        cmd_flag(int(sys.argv[2]))
    elif command == "cover":
        if len(sys.argv) < 3:
            print("Usage: python scripts/cli.py cover <silver_id>")
            sys.exit(1)
        cmd_cover(int(sys.argv[2]))
    elif command == "stats":
        cmd_stats()
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
