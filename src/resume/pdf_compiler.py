#!/usr/bin/env python3
"""
pdf_compiler.py — Phase 3
Jinja2 HTML + WeasyPrint PDF Compiler.
Takes bullet_picker output and compiles a role-tailored PDF resume.
"""

import os
import re
import logging
from datetime import datetime
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML

logger = logging.getLogger(__name__)

JOB_ORDER = [
    {"title": "Data Engineer Consultant",
     "company": "10Alytics, Manchester, UK",
     "dates": "March 2026 – Present",
     "project_key": "job-search-autopipe"},
    {"title": "Data Engineer",
     "company": "Glasgow Traders, Glasgow, Scotland",
     "dates": "March 2025 – Present",
     "project_key": "glasgow-traders"},
    {"title": "Digital Web System Administrator",
     "company": "Rex Insurance Limited, Lagos, Nigeria",
     "dates": "Aug 2019 – Aug 2022",
     "project_key": "rex-insurance"},
    {"title": "E-commerce Web System Administrator",
     "company": "Newcoolmex Nigeria Limited, Lagos, Nigeria",
     "dates": "Feb 2016 – Jul 2019",
     "project_key": "newcoolmex"},
    {"title": "Digital Executive / Web Systems Administrator",
     "company": "Alphacom Online Investment Limited, Lagos, Nigeria",
     "dates": "Sept 2015 – Jan 2016",
     "project_key": "alphacom"},
]

# Fallback bullets per job shown when no selected bullets match that role
FALLBACK_BULLETS = {
    "job-search-autopipe": [
        "Built a 5-stage Airflow pipeline that pulls 797 UK job listings every day from the "
        "Adzuna and Reed APIs, scores them with an NLP classifier, and sends a ranked Telegram "
        "digest automatically. The whole thing runs on PostgreSQL with a medallion architecture "
        "inside Docker Compose with no manual trigger needed.",
        "Built a validation layer that caught 171 invalid records, 400 duplicate meter IDs and "
        "21 high-consumption anomalies automatically. The pipeline held a 98.25% data quality "
        "rate against a 95% target.",
        "Wired the Claude API into the pipeline so flagging a job from Telegram automatically "
        "generates a cover letter matched to that specific job description and skills profile.",
    ],
    "glasgow-traders": [
        "Built the whole pipeline from scratch. It discovers, enriches and publishes local trade "
        "business listings across Scotland, processing 1,632 businesses in 107 locations without "
        "a single pipeline failure. The Google Places ingestion layer cut irrelevant data by "
        "about 85% at source using haversine filtering.",
        "Modelled the data in dbt Core on Snowflake using a Bronze to Silver to Gold architecture. "
        "Migrated from PostgreSQL with all 4 dbt tests passing first time. Automated WordPress "
        "publishing of 1,056 trade listings with no manual CMS work.",
        "Containerised the full stack with Docker Compose and drafted Terraform config for "
        "AWS EC2 and ECS deployment via MWAA.",
    ],
    "rex-insurance": [
        "Kept the platform running at 99.9% uptime with over 10,000 active sessions each month.",
        "Set up Google Analytics, Tag Manager and Meta Pixel to capture over 100,000 user events "
        "a month. That data fed into conversion funnel analysis and cut the bounce rate by 25%.",
        "Did the keyword analysis and content work that drove a 45% increase in organic traffic "
        "over 12 months and moved 15 high-value keywords from page 3 to page 1.",
    ],
    "newcoolmex": [
        "Ran the daily data ingestion for pricing, inventory and catalogue metadata across the "
        "e-commerce platform, keeping data consistent across multiple storefronts.",
        "Built analytics workflows in Google Analytics, Ahrefs, SEMrush, Moz and Screaming Frog "
        "to pull performance data and turn it into actionable recommendations.",
    ],
    "alphacom": [
        "Pulled engagement data from Facebook, Instagram, Twitter and LinkedIn into a single "
        "reporting pipeline so performance could be compared across channels in one place.",
        "Produced Google Ads campaign reports that tracked conversion patterns and gave clear "
        "recommendations on where to shift budget.",
    ],
}


def extract_ats_keywords(jd_text: str) -> str:
    tech_pattern = re.compile(
        r"\b(Python|SQL|Airflow|dbt|Snowflake|Databricks|Spark|Kafka|Docker|"
        r"Kubernetes|Terraform|AWS|Azure|GCP|PostgreSQL|Redshift|BigQuery|"
        r"Pandas|PySpark|Git|GitHub|REST|API|ETL|ELT|Medallion|"
        r"EC2|ECS|MWAA|S3|Glue|Lambda)\b",
        re.IGNORECASE,
    )
    matches = tech_pattern.findall(jd_text)
    return " ".join(dict.fromkeys(m.lower() for m in matches))


def organise_bullets_by_job(selected_bullets: list) -> list:
    """
    Groups selected bullets by job. Arch bullets go to the job with fewest.
    Jobs with no selected bullets get their FALLBACK_BULLETS (2 max).
    All 5 jobs always appear.
    """
    project_map: dict = {}
    arch_bullets: list = []

    for b in selected_bullets:
        pid = b.get("source_project", "")
        text = b["text"]
        if pid == "cross-project":
            arch_bullets.append(text)
        else:
            project_map.setdefault(pid, []).append(text)

    # Distribute arch bullets to job with fewest bullets (top 2 jobs only)
    for arch_text in arch_bullets:
        target = min(JOB_ORDER[:2], key=lambda j: len(project_map.get(j["project_key"], [])))
        project_map.setdefault(target["project_key"], []).append(arch_text)

    jobs = []
    for job in JOB_ORDER:
        pk = job["project_key"]
        bullets = project_map.get(pk)
        if not bullets:
            # Use fallback bullets (max 2 for older roles, 3 for recent)
            fb = FALLBACK_BULLETS.get(pk, [])
            max_fb = 3 if pk in ("job-search-autopipe", "glasgow-traders") else 2
            bullets = fb[:max_fb]
        jobs.append({
            "title": job["title"],
            "company": job["company"],
            "dates": job["dates"],
            "bullets": bullets,
        })

    return jobs


def compile_pdf(
    picker_result: dict,
    jd_text: str,
    company_name: str,
    role_title: str,
    story_bank_path: str = "config/story_bank.yaml",
    template_path: str = "src/resume/resume_template.html",
    output_dir: str = "output/resumes",
) -> str:
    with open(story_bank_path) as f:
        bank = yaml.safe_load(f)
    meta = bank.get("meta", {})

    archetype = picker_result["archetype"]
    summary = picker_result["summary"]
    jobs = organise_bullets_by_job(picker_result["selected_bullets"])
    ats_keywords = extract_ats_keywords(jd_text)

    env = Environment(loader=FileSystemLoader(str(Path(template_path).parent)))
    template = env.get_template(Path(template_path).name)
    rendered_html = template.render(
        archetype=archetype, summary=summary,
        jobs=jobs, meta=meta, ats_keywords=ats_keywords,
    )

    date_str = datetime.now().strftime("%Y-%m-%d")
    safe_company = re.sub(r"[^a-zA-Z0-9]", "-", company_name)[:30]
    safe_role = re.sub(r"[^a-zA-Z0-9]", "-", role_title)[:25]
    filename = f"Abraham-Aroloye-{safe_company}-{safe_role}-{date_str}.pdf"

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    HTML(string=rendered_html, base_url=".").write_pdf(output_path)
    logger.info(f"PDF compiled: {output_path}")
    return output_path


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    # Full test using all 5 jobs via fallback bullets
    picker_result = {
        "archetype": "pipeline",
        "summary": (
            "I build data pipelines that actually work in production. Over the past year I have shipped "
            "end-to-end ETL and ELT systems on Airflow, PostgreSQL, Snowflake and Docker that handle "
            "everything from raw API ingestion through to validated, business-ready output. My recent work "
            "at 10Alytics involved processing 9,600 energy meter records daily at 98.25% quality and "
            "building a 5-stage Airflow pipeline ingesting 797 job listings a day from two APIs."
        ),
        "selected_bullets": [],  # Empty = all jobs use fallback = full CV shown
    }

    pdf_path = compile_pdf(
        picker_result=picker_result,
        jd_text="Data Engineer role requiring Python, Airflow, dbt, Snowflake, AWS, Docker, Terraform, PostgreSQL",
        company_name="TestCompany",
        role_title="DataEngineer",
    )
    print(f"\nPDF generated: {pdf_path}")
