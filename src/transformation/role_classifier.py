"""
Role Classifier for Job Search AutoPipe.

Uses keyword-weighted NLP scoring to determine whether a job posting
is genuinely a data engineering role vs. a mismatch (data entry, 
data analyst, support engineer, etc.).

This is a key differentiator for the portfolio — it demonstrates
understanding of data quality and intelligent filtering.
"""

import re
import logging
from collections import Counter
from typing import Tuple

logger = logging.getLogger(__name__)


# ── Default Signal Weights ─────────────────────────────────────────

DEFAULT_POSITIVE_SIGNALS = {
    "high_weight": {
        "weight": 15,
        "terms": [
            "airflow", "spark", "kafka", "dbt", "snowflake", "databricks",
            "data pipeline", "data pipelines", "etl", "elt", "data warehouse",
            "data lake", "data lakehouse", "data platform", "data mesh",
            "medallion", "bronze", "silver", "gold", "orchestration",
            "batch processing", "stream processing", "data modelling",
        ]
    },
    "medium_weight": {
        "weight": 8,
        "terms": [
            "python", "sql", "aws", "azure", "gcp", "docker", "kubernetes",
            "terraform", "postgresql", "redshift", "bigquery", "glue",
            "kinesis", "firehose", "lambda", "step functions", "mwaa",
            "s3", "parquet", "avro", "delta lake", "iceberg",
            "ci/cd", "github actions", "jenkins", "data quality",
            "great expectations", "soda", "dbt tests",
        ]
    },
    "low_weight": {
        "weight": 3,
        "terms": [
            "git", "linux", "api", "rest", "json", "yaml", "csv",
            "agile", "scrum", "jira", "confluence", "documentation",
            "monitoring", "logging", "observability", "alerting",
        ]
    },
}

DEFAULT_NEGATIVE_SIGNALS = {
    "high_penalty": {
        "weight": -20,
        "terms": [
            "data entry", "customer service", "receptionist", "admin support",
            "administrative", "filing", "typing speed", "switchboard",
        ]
    },
    "medium_penalty": {
        "weight": -10,
        "terms": [
            "helpdesk", "help desk", "support engineer", "manual testing",
            "data entry clerk", "call centre", "call center",
            "telesales", "telemarketing",
        ]
    },
    "light_penalty": {
        "weight": -5,
        "terms": [
            "no technical experience required",
            "no experience needed",
            "entry level customer",
        ]
    },
}

# Title-based signals (checked against job title only)
TITLE_POSITIVE = {
    "data engineer": 25,
    "data engineering": 25,
    "analytics engineer": 20,
    "data platform": 20,
    "pipeline engineer": 20,
    "etl developer": 15,
    "elt developer": 15,
    "data infrastructure": 15,
}

TITLE_NEGATIVE = {
    "data entry": -30,
    "data analyst": -10,     # Could be borderline, light penalty
    "data scientist": -8,    # Could be borderline
    "support": -15,
    "customer": -20,
    "receptionist": -30,
    "admin": -20,
    "clerk": -25,
    "sales": -20,
}


class RoleClassifier:
    """
    Classifies job postings as genuine data engineering roles.
    
    Scoring:
    - Base score: 50 (neutral)
    - Title signals: +/- 15-30 points
    - Description keyword signals: +/- 3-20 points per keyword
    - Final score clamped to 0-100
    - Threshold (default 60) determines pass/fail
    """

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.min_role_score = self.config.get("min_role_score", 60)

        # Load custom signals from config, or use defaults
        pos = self.config.get("positive_signals", {})
        neg = self.config.get("negative_signals", {})

        self.positive_signals = self._build_signals(pos, DEFAULT_POSITIVE_SIGNALS)
        self.negative_signals = self._build_signals(neg, DEFAULT_NEGATIVE_SIGNALS)

    def _build_signals(self, custom: dict, defaults: dict) -> dict:
        """Merge custom config signals with defaults."""
        if not custom:
            return defaults
        merged = {}
        for tier, data in defaults.items():
            custom_terms = custom.get(tier, [])
            if custom_terms:
                merged[tier] = {"weight": data["weight"], "terms": custom_terms}
            else:
                merged[tier] = data
        return merged

    def classify(self, title: str, description: str) -> Tuple[float, bool, dict]:
        """
        Classify a job posting.
        
        Returns:
            Tuple of (score, is_genuine, details_dict)
        """
        title_lower = title.lower().strip()
        desc_lower = description.lower().strip()
        full_text = f"{title_lower} {desc_lower}"

        score = 50.0  # Neutral starting point
        details = {
            "title_signals": [],
            "positive_hits": [],
            "negative_hits": [],
            "breakdown": {},
        }

        # ── Title Scoring ──────────────────────────────────
        for term, points in TITLE_POSITIVE.items():
            if term in title_lower:
                score += points
                details["title_signals"].append(f"+{points}: '{term}' in title")

        for term, points in TITLE_NEGATIVE.items():
            if term in title_lower:
                score += points  # points are already negative
                details["title_signals"].append(f"{points}: '{term}' in title")

        # ── Description Positive Signals ───────────────────
        for tier, data in self.positive_signals.items():
            weight = data["weight"]
            for term in data["terms"]:
                if term in desc_lower:
                    score += weight
                    details["positive_hits"].append(f"+{weight}: '{term}'")

        # ── Description Negative Signals ───────────────────
        for tier, data in self.negative_signals.items():
            weight = data["weight"]
            for term in data["terms"]:
                if term in desc_lower:
                    score += weight  # Already negative
                    details["negative_hits"].append(f"{weight}: '{term}'")

        # ── Clamp to 0-100 ─────────────────────────────────
        score = max(0.0, min(100.0, score))
        is_genuine = score >= self.min_role_score

        details["breakdown"] = {
            "base_score": 50,
            "title_adjustment": sum(
                int(s.split(":")[0]) for s in details["title_signals"]
            ),
            "positive_boost": sum(
                int(s.split(":")[0].replace("+", "")) for s in details["positive_hits"]
            ),
            "negative_penalty": sum(
                int(s.split(":")[0]) for s in details["negative_hits"]
            ),
            "final_score": score,
            "threshold": self.min_role_score,
            "passes": is_genuine,
        }

        return score, is_genuine, details
