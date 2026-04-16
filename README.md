# Job Search AutoPipe 🔍➡️📊➡️✉️

A production-grade data engineering pipeline that automates job discovery, intelligent filtering, daily Telegram notifications, and AI-powered cover letter generation for data engineering roles.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        JOB SEARCH AUTOPIPE                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌────────────────┐    │
│  │  INGEST  │───▶│CLASSIFY  │───▶│  DIGEST  │───▶│   APP PREP     │    │
│  │ (Bronze) │    │ (Silver) │    │  (Gold)  │    │   (Output)     │    │
│  └──────────┘    └──────────┘    └──────────┘    └────────────────┘    │
│       │               │               │                  │             │
│   Job APIs        NLP/Skill      Telegram Bot       Cover Letter       │
│   - Adzuna        Matching       7 AM Daily         Claude API         │
│   - Reed          Dedup +        Ranked List        Tailored to JD     │
│                   Scoring        /flag /cover        Per-Role Output   │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│  ORCHESTRATION:  Apache Airflow 2.8                                     │
│  STORAGE:        PostgreSQL 15 (Medallion Architecture)                 │
│  TRANSFORMS:     dbt Core                                               │
│  QUALITY:        Great Expectations                                     │
│  NOTIFICATIONS:  Telegram Bot (Webhook)                                 │
│  COVER LETTERS:  Claude API                                             │
│  INFRA:          Docker Compose                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Pipeline Phases

### Phase 1 — Ingestion (Bronze Layer)
- Pulls live job listings from Adzuna and Reed APIs every morning
- Raw JSON stored in `bronze.raw_job_postings`
- SHA-256 content hashing for deduplication across sources
- Idempotent inserts — `ON CONFLICT DO NOTHING` prevents re-processing
- Handles both Adzuna (ISO dates) and Reed (DD/MM/YYYY dates) formats

### Phase 2 — Classification & Scoring (Silver Layer)
- **Role Classifier** — NLP-based weighted keyword scoring to verify genuine data engineering roles. Title and description signals with configurable weights and threshold
- **Skills Match Scorer** — Compares JD requirements against a skills profile with proficiency weighting (expert=1.0, proficient=0.7, familiar=0.4)
- **Deduplication** — Cross-source SHA-256 hash of title+company eliminates duplicate listings from different sources
- **Quality Gate** — Only postings scoring above the configured threshold proceed to the gold layer

### Phase 3 — Daily Digest (Gold Layer)
- Ranked Telegram message delivered at 7 AM daily
- Each listing shows: rank, match score (🟢🟡🟠), title, company, location, matched skills, ID, and apply link
- `gold.daily_digest` tracks what has been sent — no duplicates across days

### Phase 4 — Application Prep (Output Layer)
- `/flag <id>` — flag a job from Telegram to start an application
- `/cover <id>` — generate a tailored cover letter via Claude API, delivered to Telegram
- Cover letters reference actual portfolio projects, matched skills, and address skill gaps honestly
- Application outcomes tracked in `gold.application_tracker` with full ATS analytics

---

## Tech Stack

| Component         | Technology                    | Why                                         |
|-------------------|-------------------------------|---------------------------------------------|
| Orchestration     | Apache Airflow 2.8            | Industry standard for data pipelines        |
| Database          | PostgreSQL 15                 | Medallion architecture (bronze/silver/gold) |
| Transformations   | dbt Core                      | SQL-based transforms with lineage           |
| Data Quality      | Great Expectations            | Automated validation suites                 |
| Containerisation  | Docker Compose                | Reproducible local environment              |
| Notifications     | Telegram Bot + Webhook        | Real-time digest + bidirectional commands   |
| Cover Letters     | Claude API                    | AI-powered, JD-tailored generation          |
| Language          | Python 3.11                   | Pipeline logic and API clients              |

---

## Database Schema (Medallion Architecture)

```
BRONZE — Raw ingestion, untouched source data
  bronze.raw_job_postings     (id, source, source_job_id, content_hash, raw_json, ingested_at)

SILVER — Cleaned, classified, and scored
  silver.classified_jobs      (id, bronze_id, title, company, location, salary_min, salary_max,
                               description_clean, url, posted_date, source,
                               role_score, skills_match_score, overall_score,
                               is_genuine_de_role, matched_skills, missing_skills,
                               dedup_hash, is_duplicate, classified_at)

GOLD — Digest-ready and application tracking
  gold.daily_digest           (id, digest_date, silver_id, rank_position, digest_sent, created_at)
  gold.application_tracker    (id, silver_id, status, flagged_at, applied_at, cover_letter,
                               cv_notes, response_at, interview_at, days_to_response)

META — Pipeline observability
  meta.pipeline_runs          (id, dag_id, run_id, phase, status, records_in, records_out,
                               started_at, completed_at, error_message)
```

---

## Telegram Bot Commands

| Command | Description |
|---------|-------------|
| `/digest` | Get today's ranked job digest |
| `/flag <id>` | Flag a job to start an application |
| `/cover <id>` | Generate a tailored cover letter via Claude API |
| `/stats` | View pipeline statistics |

---

## Quick Start

**Prerequisites:** Docker Desktop, Git

```bash
# 1. Clone the repo
git clone https://github.com/Larious/job-search-autopipe.git
cd job-search-autopipe

# 2. Configure
cp config/config.example.yaml config/config.yaml
# Edit config.yaml with your API keys (see Configuration section below)

# 3. Start infrastructure
docker compose up -d pipeline-db airflow-db
docker compose up schema-init
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler

# 4. Trigger the pipeline
# Open http://localhost:8080 (admin/admin)
# Trigger the job_search_autopipe DAG manually

# 5. Check Telegram for your digest
```

---

## Configuration

Copy `config/config.example.yaml` to `config/config.yaml` and fill in your values.

> ⚠️ `config/config.yaml` is gitignored — never commit it. It contains your API keys.

You will need:
- **Adzuna API keys** — free at [developer.adzuna.com](https://developer.adzuna.com)
- **Reed API key** — free at [reed.co.uk/developers](https://www.reed.co.uk/developers)
- **Telegram bot token + chat ID** — via [@BotFather](https://t.me/botfather) on Telegram
- **Anthropic API key** — from [console.anthropic.com](https://console.anthropic.com)

---

## Project Structure

```
job-search-autopipe/
├── dags/
│   └── job_search_dag.py              # Airflow DAG: ingest→classify→quality→digest
├── src/
│   ├── ingestion/
│   │   ├── base_client.py             # Abstract base with SHA-256 hashing, date parsing
│   │   ├── adzuna_client.py           # Adzuna API client
│   │   └── reed_client.py             # Reed API client
│   ├── transformation/
│   │   ├── role_classifier.py         # NLP weighted keyword scorer
│   │   └── skills_matcher.py          # Proficiency-weighted skills matching
│   ├── quality/
│   │   └── expectations.py            # Great Expectations validation suite
│   ├── generation/
│   │   └── cover_letter_generator.py  # Claude API + template fallback
│   ├── utils/
│   │   ├── database.py                # PostgreSQL connection and CRUD methods
│   │   ├── config_loader.py           # YAML config loader
│   │   ├── telegram_notifier.py       # Telegram message formatting and sending
│   │   ├── slack_notifier.py          # Slack webhook notifier
│   │   └── notifier_factory.py        # Notification channel routing
│   └── webhook/
│       └── telegram_webhook_server.py # Handles incoming Telegram commands
├── dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── bronze/stg_raw_postings.sql
│       ├── silver/int_classified_jobs.sql
│       └── gold/mart_daily_digest.sql
│           mart_ats_analytics.sql
├── config/
│   ├── config.example.yaml            # Template — safe to commit
│   └── skills_profile.yaml            # Skills profile for matching
├── scripts/
│   └── cli.py                         # Terminal interface for pipeline operations
├── docs/
│   ├── STAGE_1_SETUP_GUIDE.md
│   ├── STAGE_2_INGESTION.md
│   ├── STAGE_3_CLASSIFICATION.md
│   ├── STAGE_4_NOTIFICATIONS.md
│   └── STAGE_5_GENERATION.md
├── docker-compose.yml
└── requirements.txt
```

---

## Built by Abraham Aroloye

- GitHub: [github.com/Larious](https://github.com/Larious)
- LinkedIn: [linkedin.com/in/abrahamaroloye](https://www.linkedin.com/in/abrahamaroloye/)
- Portfolio: [abrahamaroloye.com](https://abrahamaroloye.com)
