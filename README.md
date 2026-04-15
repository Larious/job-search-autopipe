# Job Search AutoPipe 🔍➡️📊➡️✉️

## _The data engineering pipeline that got me this interview._

A production-grade data engineering pipeline that automates job discovery, intelligent filtering, and application preparation for data engineering roles. Built with the same tools and patterns used in enterprise data platforms.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                     JOB SEARCH AUTOPIPE                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────┐  │
│  │  INGEST  │───▶│  FILTER  │───▶│  DIGEST  │───▶│  APP PREP    │  │
│  │ (Bronze) │    │ (Silver) │    │  (Gold)  │    │  (Output)    │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────────┘  │
│       │               │               │                │           │
│   Job APIs        NLP/Skill      Slack/Email      Cover Letter     │
│   - Adzuna        Matching       Morning Alert    & CV Tailoring   │
│   - Reed          Dedup +        Ranked List      Per-JD Output    │
│   - The Muse      Scoring        Human Review     Ready to Submit  │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│  ORCHESTRATION: Apache Airflow                                      │
│  STORAGE: PostgreSQL (Medallion Architecture)                       │
│  TRANSFORMS: dbt Core                                               │
│  QUALITY: Great Expectations                                        │
│  INFRA: Docker Compose                                              │
│  OBSERVABILITY: Slack Webhooks + Logging                            │
└─────────────────────────────────────────────────────────────────────┘
```

## Pipeline Phases

### Phase 1 — Ingestion (Bronze Layer)
- Pulls job listings from multiple APIs (Adzuna, Reed, The Muse)
- Raw JSON stored in `bronze.raw_job_postings`
- SHA-256 deduplication across sources
- Incremental loads — only new postings each run
- Schema: job_id, source, raw_json, ingested_at, content_hash

### Phase 2 — Intelligent Filtering (Silver Layer)
- **Role Classifier**: NLP-based scoring to verify genuine data engineering roles
  - Filters out "data entry", "support engineer", "data analyst" mismatches
  - Keyword weighting: tools (Spark, Airflow, dbt) score higher than generic terms
- **Skills Match Scorer**: Compares JD requirements against your skills profile
  - Weighted scoring across: tools, languages, cloud platforms, soft skills
- **Deduplication**: Cross-source matching to avoid duplicate listings
- **Quality Gate**: Only postings scoring above threshold proceed

### Phase 3 — Daily Digest (Gold Layer)
- Morning Slack/email alert with ranked job matches
- Each listing shows: title, company, match score, key requirements, apply link
- **Human-in-the-loop**: You review and flag jobs worth pursuing
- Dashboard view of pipeline health and job market trends

### Phase 4 — Application Prep (Output Layer)
- For flagged jobs: generates tailored cover letter using your profile + JD
- Highlights which CV sections to emphasize per role
- Outputs ready-to-use application package (PDF cover letter + notes)
- Tracks application status: applied, interviewing, rejected, offer

---

## Tech Stack

| Component         | Technology                  | Why                                      |
|-------------------|-----------------------------|------------------------------------------|
| Orchestration     | Apache Airflow 2.x          | Industry standard for data pipelines     |
| Database          | PostgreSQL 15               | Medallion architecture (bronze/silver/gold) |
| Transformations   | dbt Core                    | SQL-based transforms with lineage        |
| Data Quality      | Great Expectations          | Automated validation suites              |
| Containerisation  | Docker Compose              | Reproducible local environment           |
| Alerting          | Slack Webhooks              | Real-time pipeline notifications         |
| Cover Letters     | Claude API / Local LLM      | AI-powered document generation           |
| Language          | Python 3.11                 | Pipeline logic and API clients           |

---

## Quick Start

```bash
# 1. Clone and configure
cp config/config.example.yaml config/config.yaml
# Edit with your API keys and preferences

# 2. Launch infrastructure
docker-compose up -d

# 3. Run the pipeline
airflow dags trigger job_search_autopipe

# 4. Check your Slack for the morning digest
```

---

## Interview Talking Points

> "I built this pipeline to solve my own job search problem using the exact same
> tools I'd use on the job. It pulls from multiple APIs, applies intelligent
> filtering with NLP scoring, runs data quality checks, and delivers a daily
> digest of matched roles — the same patterns as any production ETL system."

### Key engineering decisions to discuss:
1. **Medallion architecture** — Same bronze/silver/gold pattern used at scale
2. **Idempotent pipelines** — SHA-256 hashing prevents duplicate processing
3. **Human-in-the-loop** — Deliberate choice over full automation (shows judgment)
4. **Observability** — Slack alerts, logging, data quality gates
5. **Infrastructure as code** — Entire stack in Docker Compose, config-driven
6. **Transferable patterns** — Same architecture as my Glasgow Traders AutoPipe

---

## Project Structure

```
job-search-autopipe/
├── dags/                          # Airflow DAG definitions
│   └── job_search_dag.py
├── src/
│   ├── ingestion/                 # API clients for job boards
│   │   ├── adzuna_client.py
│   │   ├── reed_client.py
│   │   └── base_client.py
│   ├── transformation/            # Filtering and scoring logic
│   │   ├── role_classifier.py
│   │   └── skills_matcher.py
│   ├── quality/                   # Great Expectations suites
│   │   └── expectations.py
│   ├── generation/                # Cover letter + CV tailoring
│   │   └── cover_letter_generator.py
│   └── utils/                     # Shared utilities
│       ├── slack_notifier.py
│       ├── database.py
│       └── config_loader.py
├── dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── bronze/                # Raw ingestion models
│       ├── silver/                # Filtered + scored models
│       └── gold/                  # Digest-ready views
├── config/
│   ├── config.example.yaml
│   ├── skills_profile.yaml        # Your skills for matching
│   └── cover_letter_template.yaml
├── docker/
│   └── Dockerfile
├── tests/
├── docker-compose.yml
├── requirements.txt
└── README.md
```
