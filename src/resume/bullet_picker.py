#!/usr/bin/env python3
"""
bullet_picker.py
Phase 2 — Constraint-Aware Bullet Picker
Reads a job description, uses Claude to select the 15 best bullets
from story_bank.yaml, enforcing structural constraints.
"""

import json
import yaml
import logging
from pathlib import Path
from typing import Optional
import anthropic

logger = logging.getLogger(__name__)


# ── Constraint constants ──────────────────────────────────────────────────────
MAX_BULLETS = 15
MAX_PER_PROJECT = 2          # No more than 2 bullets from the same source_project
REQUIRED_METRIC_TYPES = {    # At least 1 bullet of each of these types
    "scale",
    "reliability",
}
MIN_RECENCY_WEIGHT = 0.0     # Allows all bullets; picker naturally scores recent higher
SCORING_WEIGHTS = {
    "relevance":   0.60,
    "recency":     0.20,
    "conversion":  0.20,     # Falls back to 0.5 (neutral) when conversion_score is null
}


# ── Loader ────────────────────────────────────────────────────────────────────
def load_story_bank(path: str = "config/story_bank.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ── Archetype detector ────────────────────────────────────────────────────────
def detect_archetype(jd_text: str, client: anthropic.Anthropic) -> str:
    """Ask Claude to classify the JD into one of 4 archetypes."""
    prompt = f"""You are a recruitment analyst. Read this job description and classify it into EXACTLY ONE archetype.

Archetypes:
- pipeline   → ETL/ELT focus, Airflow, data ingestion, transformation, orchestration
- analytics  → dbt, Snowflake, data modelling, BI, metrics, SQL-heavy
- cloud      → AWS/Azure/GCP infrastructure, Terraform, cloud-native deployment
- platform   → Full-stack data infrastructure, internal tooling, observability, platform engineering

Job Description:
{jd_text[:3000]}

Reply with ONLY the single archetype word (pipeline / analytics / cloud / platform). No explanation."""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=10,
        messages=[{"role": "user", "content": prompt}]
    )
    archetype = response.content[0].text.strip().lower()
    if archetype not in ("pipeline", "analytics", "cloud", "platform"):
        archetype = "pipeline"  # safe default
    logger.info(f"Detected archetype: {archetype}")
    return archetype


# ── Claude bullet scorer ──────────────────────────────────────────────────────
def score_bullets_with_claude(
    jd_text: str,
    bullets: list[dict],
    archetype: str,
    client: anthropic.Anthropic
) -> list[dict]:
    """
    Ask Claude to score each bullet 0.0–1.0 for relevance to the JD.
    Returns bullets list with added 'relevance_score' key.
    """
    bullets_payload = [
        {"id": b["id"], "text": b["text"], "tags": b["tags"]}
        for b in bullets
    ]

    prompt = f"""You are an expert CV consultant and ATS specialist.

Job Description (excerpt):
{jd_text[:2500]}

Detected role archetype: {archetype}

Below is a list of resume bullet points in JSON. For each bullet, return a relevance score from 0.0 to 1.0 based on:
- How directly the bullet addresses skills/experience in the JD
- Whether the bullet's tags match JD keywords
- Whether the bullet demonstrates impact relevant to this role type

Return ONLY a valid JSON array in this exact format:
[{{"id": "bullet-id", "relevance": 0.0}}]

No explanation. No markdown. Just the JSON array.

Bullets:
{json.dumps(bullets_payload, indent=2)}"""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}]
    )

    raw = response.content[0].text.strip()
    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    scores = json.loads(raw.strip())
    score_map = {s["id"]: s["relevance"] for s in scores}

    for b in bullets:
        b["relevance_score"] = score_map.get(b["id"], 0.0)

    return bullets


# ── Composite scorer ──────────────────────────────────────────────────────────
def compute_composite_score(bullet: dict) -> float:
    relevance   = bullet.get("relevance_score", 0.0)
    recency     = bullet.get("recency_weight", 0.5)
    conversion  = bullet.get("conversion_score") or 0.5  # null → neutral 0.5

    return (
        SCORING_WEIGHTS["relevance"]   * relevance  +
        SCORING_WEIGHTS["recency"]     * recency    +
        SCORING_WEIGHTS["conversion"]  * conversion
    )


# ── Constraint enforcer ───────────────────────────────────────────────────────
def apply_constraints(ranked: list[dict]) -> list[dict]:
    """
    Select up to MAX_BULLETS bullets enforcing:
    1. Max MAX_PER_PROJECT from the same source_project
    2. At least 1 bullet of each REQUIRED_METRIC_TYPE
    """
    selected = []
    project_counts: dict[str, int] = {}
    metric_types_covered: set[str] = set()
    required_remaining = set(REQUIRED_METRIC_TYPES)

    # Pass 1: greedily fill required metric types first
    for bullet in ranked:
        mt = bullet.get("metric_type", "")
        proj = bullet.get("source_project", "unknown")
        if mt in required_remaining:
            if project_counts.get(proj, 0) < MAX_PER_PROJECT:
                selected.append(bullet)
                project_counts[proj] = project_counts.get(proj, 0) + 1
                metric_types_covered.add(mt)
                required_remaining.discard(mt)
        if not required_remaining:
            break

    # Pass 2: fill remaining slots by composite score
    for bullet in ranked:
        if len(selected) >= MAX_BULLETS:
            break
        if bullet in selected:
            continue
        proj = bullet.get("source_project", "unknown")
        if project_counts.get(proj, 0) < MAX_PER_PROJECT:
            selected.append(bullet)
            project_counts[proj] = project_counts.get(proj, 0) + 1

    return selected[:MAX_BULLETS]


# ── Gap analyser ──────────────────────────────────────────────────────────────
def analyse_gaps(
    jd_text: str,
    selected_bullets: list[dict],
    client: anthropic.Anthropic
) -> str:
    """Ask Claude to identify JD requirements not covered by selected bullets."""
    selected_texts = "\n".join(f"- {b['text']}" for b in selected_bullets)

    prompt = f"""You are a career coach reviewing a tailored resume against a job description.

Job Description:
{jd_text[:2000]}

Selected resume bullets:
{selected_texts}

Identify up to 5 skills or requirements mentioned in the JD that are NOT addressed by the resume bullets.
For each gap, suggest in ONE sentence how the candidate could address it in an interview.

Format exactly like this (no headers, no markdown):
• [Skill/requirement]: [One-sentence interview tip]

Maximum 5 gaps. If fewer than 5, that's fine. Be concise."""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.content[0].text.strip()


# ── Main entry point ──────────────────────────────────────────────────────────
def pick_bullets(
    jd_text: str,
    story_bank_path: str = "config/story_bank.yaml",
    api_key: Optional[str] = None,
) -> dict:
    """
    Main function. Given a JD string, returns:
    {
        "archetype": str,
        "summary": str,
        "selected_bullets": [list of 15 bullet dicts],
        "gap_report": str,
        "bullets_not_selected": [list of remaining bullet ids]
    }
    """
    import os
    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=key)

    bank = load_story_bank(story_bank_path)
    all_bullets = bank["bullets"]
    summaries = bank["summaries"]

    # Step 1: Detect archetype
    archetype = detect_archetype(jd_text, client)

    # Step 2: Filter bullets relevant to detected archetype
    # Include bullets that match archetype OR are cross-project (arch-*)
    filtered = [
        b for b in all_bullets
        if archetype in b.get("archetype", []) or b["id"].startswith("arch-")
    ]

    # Step 3: Score with Claude
    scored = score_bullets_with_claude(jd_text, filtered, archetype, client)

    # Step 4: Sort by composite score
    ranked = sorted(scored, key=compute_composite_score, reverse=True)

    # Step 5: Apply constraints
    selected = apply_constraints(ranked)

    # Step 6: Analyse gaps
    gap_report = analyse_gaps(jd_text, selected, client)

    # Step 7: Identify bullets not selected
    selected_ids = {b["id"] for b in selected}
    not_selected = [b["id"] for b in ranked if b["id"] not in selected_ids]

    return {
        "archetype": archetype,
        "summary": summaries[archetype]["text"],
        "selected_bullets": selected,
        "gap_report": gap_report,
        "bullets_not_selected": not_selected,
    }


# ── CLI test runner ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) < 2:
        print("Usage: python bullet_picker.py \'<job description text>\'")
        sys.exit(1)

    jd = sys.argv[1]
    result = pick_bullets(jd)

    print(f"\n🎯 Archetype: {result['archetype'].upper()}")
    print(f"\n📝 Summary:\n{result['summary']}")
    print(f"\n✅ Selected {len(result['selected_bullets'])} bullets:")
    for i, b in enumerate(result["selected_bullets"], 1):
        print(f"  {i}. [{b['id']}] {b['text'][:90]}...")
    print(f"\n⚠️  Gap Report:\n{result['gap_report']}")
    print(f"\n❌ Not selected: {result['bullets_not_selected']}")
