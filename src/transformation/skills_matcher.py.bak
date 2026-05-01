"""
Skills Matcher for Job Search AutoPipe.

Compares a job description against the user's skills profile to produce
a match score. Identifies which skills match and which are missing.

This is the silver-layer intelligence that makes the pipeline useful —
it's the difference between "here are 50 jobs" and "here are 5 jobs
that actually match what you can do."
"""

import hashlib
import logging
import re
from typing import Tuple

logger = logging.getLogger(__name__)


# Proficiency weights
PROFICIENCY_WEIGHTS = {
    "expert": 1.0,
    "proficient": 0.7,
    "familiar": 0.4,
}


class SkillsMatcher:
    """
    Scores how well a job description matches the user's skills profile.
    
    Algorithm:
    1. Extract skill requirements from JD text
    2. Compare against user's skills (weighted by proficiency)
    3. Calculate match percentage
    4. Identify matched and missing skills
    5. Generate a cross-source deduplication hash
    """

    def __init__(self, skills_profile: dict):
        self.profile = skills_profile
        self.skill_index = self._build_skill_index()
        self.all_known_skills = self._build_known_skills_set()

    def _build_skill_index(self) -> dict:
        """
        Build a flat lookup: skill_name -> proficiency_weight.
        Handles the nested structure of skills_profile.yaml.
        """
        index = {}
        tech = self.profile.get("technical_skills", {})

        for category, levels in tech.items():
            if not isinstance(levels, dict):
                continue
            for level, skills in levels.items():
                weight = PROFICIENCY_WEIGHTS.get(level, 0.3)
                if isinstance(skills, list):
                    for skill in skills:
                        # Store lowercase for matching
                        index[skill.lower()] = {
                            "name": skill,
                            "category": category,
                            "level": level,
                            "weight": weight,
                        }
                        # Also index common abbreviations
                        for alias in self._get_aliases(skill):
                            index[alias.lower()] = index[skill.lower()]

        return index

    def _build_known_skills_set(self) -> set:
        """
        Build a comprehensive set of data engineering skills to look for in JDs.
        Includes skills beyond what the user knows (to identify gaps).
        """
        # Start with user's skills
        known = set(self.skill_index.keys())

        # Add common DE skills the user might not have
        additional = [
            "scala", "java", "go", "rust", "r",
            "hadoop", "hive", "presto", "trino", "druid",
            "fivetran", "stitch", "airbyte", "meltano",
            "looker", "tableau", "power bi", "superset",
            "dms", "cdc", "debezium",
            "mlflow", "feature store",
            "data governance", "data catalog", "data lineage",
            "nifi", "talend", "informatica",
            "elasticsearch", "opensearch",
            "redis", "memcached",
            "pulsar", "rabbitmq",
            "delta lake", "iceberg", "hudi",
            "unity catalog", "aws glue", "azure data factory",
        ]
        known.update(a.lower() for a in additional)
        return known

    def _get_aliases(self, skill: str) -> list:
        """Return common aliases/abbreviations for a skill."""
        aliases_map = {
            "Apache Airflow": ["airflow"],
            "Apache Kafka": ["kafka"],
            "dbt Core": ["dbt"],
            "PostgreSQL": ["postgres", "psql"],
            "Docker Compose": ["docker-compose"],
            "Great Expectations": ["great expectations", "ge", "gx"],
            "PySpark": ["pyspark", "spark"],
            "Amazon Web Services": ["aws"],
            "Google Cloud Platform": ["gcp"],
            "Microsoft Azure": ["azure"],
            "GitHub Actions": ["github actions", "gh actions"],
            "CI/CD": ["ci/cd", "cicd", "continuous integration"],
        }
        return aliases_map.get(skill, [])

    def match(self, title: str, description: str) -> Tuple[float, list, list]:
        """
        Score how well a job matches the user's skills.
        
        Returns:
            Tuple of (match_score, matched_skills, missing_skills)
            - match_score: 0-100 percentage
            - matched_skills: list of skill names the user has
            - missing_skills: list of required skills the user lacks
        """
        text = f"{title} {description}".lower()

        # Extract skills mentioned in the JD
        jd_skills = set()
        for skill_name in self.all_known_skills:
            # Use word boundary matching for short terms
            if len(skill_name) <= 3:
                pattern = rf"\b{re.escape(skill_name)}\b"
                if re.search(pattern, text):
                    jd_skills.add(skill_name)
            else:
                if skill_name in text:
                    jd_skills.add(skill_name)

        if not jd_skills:
            return 0.0, [], []

        # Score matches
        matched = []
        missing = []
        total_weight = 0.0
        matched_weight = 0.0

        for skill in jd_skills:
            if skill in self.skill_index:
                info = self.skill_index[skill]
                matched.append(info["name"])
                matched_weight += info["weight"]
                total_weight += 1.0
            else:
                missing.append(skill)
                total_weight += 1.0

        # Calculate score as weighted percentage
        if total_weight == 0:
            return 0.0, [], []

        score = (matched_weight / total_weight) * 100.0
        score = max(0.0, min(100.0, score))

        return score, sorted(set(matched)), sorted(set(missing))

    @staticmethod
    def compute_dedup_hash(title: str, company: str) -> str:
        """
        Generate a deduplication hash from title + company.
        Used to detect the same job posted across multiple sources.
        """
        normalized = f"{title.lower().strip()}|{company.lower().strip()}"
        return hashlib.sha256(normalized.encode()).hexdigest()

    def compute_overall_score(self, role_score: float, skills_score: float) -> float:
        """
        Compute a weighted overall score from role classification and skills match.
        
        Weights:
        - Role score: 40% (is it actually a DE role?)
        - Skills match: 60% (does it match your skills?)
        """
        return (role_score * 0.4) + (skills_score * 0.6)
