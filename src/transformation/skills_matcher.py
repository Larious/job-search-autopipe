"""
Skills Matcher for Job Search AutoPipe — v2 with 10-Dimensional Scoring.

Dimensions:
  1. role_match        (gate: must pass or score = 0)
  2. skills_alignment  (gate: must pass or score = 0)
  3. seniority_fit     (how well does the level match your target?)
  4. compensation      (does the salary match your range?)
  5. geographic        (remote/hybrid/location feasibility)
  6. company_stage     (startup vs enterprise preference)
  7. posting_quality   (clear JD = higher interview likelihood)
  8. stack_freshness   (modern vs legacy tech stack)
  9. scope_fit         (IC role vs management)
 10. timeline          (recently posted = more urgent/active)
"""

import hashlib
import logging
import re
from datetime import date
from typing import Tuple

logger = logging.getLogger(__name__)

PROFICIENCY_WEIGHTS = {
    "expert": 1.0,
    "proficient": 0.7,
    "familiar": 0.4,
}

# ── Seniority detection ────────────────────────────────────────────
SENIORITY_SIGNALS = {
    "junior":    ["junior", "graduate", "entry level", "entry-level", "associate",
                  "trainee", "apprentice", "0-1 year", "0-2 year", "1 year"],
    "mid":       ["data engineer", "mid-level", "mid level", "2-4 year",
                  "2+ year", "3+ year"],
    "senior":    ["senior", "sr.", "sr ", "lead", "staff", "principal",
                  "4+ year", "5+ year", "6+ year"],
    "manager":   ["head of", "director", "manager", "vp ", "vice president",
                  "engineering manager"],
}

# ── Modern vs legacy stack signals ────────────────────────────────
MODERN_STACK = [
    "dbt", "airflow", "spark", "kafka", "snowflake", "databricks",
    "delta lake", "iceberg", "kubernetes", "terraform", "great expectations",
    "airbyte", "fivetran", "dagster", "prefect", "flink",
]
LEGACY_STACK = [
    "informatica", "talend", "ssis", "ssrs", "ssas", "oracle data integrator",
    "odi", "datastage", "ab initio", "cobol", "mainframe", "db2",
    "microsoft access", "vba", "foxpro",
]

# ── Company stage signals ──────────────────────────────────────────
STARTUP_SIGNALS = [
    "startup", "start-up", "scale-up", "scaleup", "series a", "series b",
    "series c", "seed", "early stage", "fast-growing", "fast growing",
    "hypergrowth", "vc-backed", "venture",
]
ENTERPRISE_SIGNALS = [
    "ftse", "fortune 500", "global bank", "multinational", "plc",
    "established", "leading global", "market leader", "household name",
]

# ── Posting quality signals ────────────────────────────────────────
QUALITY_POSITIVE = [
    "salary", "£", "per annum", "equity", "pension", "annual leave",
    "flexible working", "hybrid", "remote", "responsibilities",
    "requirements", "what you'll do", "what we offer",
]
QUALITY_NEGATIVE = [
    "competitive salary", "market rate", "doe", "tbc", "to be confirmed",
    "apply now", "urgent", "immediate start",
]


class SkillsMatcher:
    """
    Scores how well a job matches the user's profile across 10 dimensions.
    Gate dimensions (role_match, skills_alignment) can zero out the entire score.
    """

    def __init__(self, skills_profile: dict):
        self.profile = skills_profile
        self.skill_index = self._build_skill_index()
        self.all_known_skills = self._build_known_skills_set()

        # User preferences (read from profile or use defaults)
        personal = self.profile.get("personal", {})
        self.target_seniority = personal.get("target_seniority", "mid").lower()
        self.min_salary = personal.get("min_salary", 35000)
        self.max_salary = personal.get("max_salary", 90000)
        self.prefer_remote = personal.get("prefer_remote", True)
        self.prefer_startup = personal.get("prefer_startup", None)  # None = neutral
        self.target_location = personal.get("location", "Glasgow").lower()

    def _build_skill_index(self) -> dict:
        """Build flat lookup from config.yaml -> skills_profile -> tools."""
        index = {}

        tools = self.profile.get("tools", {})
        for level, skills in tools.items():
            weight = PROFICIENCY_WEIGHTS.get(level, 0.3)
            if isinstance(skills, list):
                for skill in skills:
                    canonical = skill.lower().strip()
                    entry = {"name": skill, "level": level, "weight": weight}
                    index[canonical] = entry
                    for alias in self._get_aliases(skill):
                        index[alias.lower()] = entry

        # Fallback: technical_skills nested structure
        tech = self.profile.get("technical_skills", {})
        for category, levels in tech.items():
            if not isinstance(levels, dict):
                continue
            for level, skills in levels.items():
                weight = PROFICIENCY_WEIGHTS.get(level, 0.3)
                if isinstance(skills, list):
                    for skill in skills:
                        canonical = skill.lower().strip()
                        if canonical not in index:
                            entry = {"name": skill, "level": level, "weight": weight}
                            index[canonical] = entry
                            for alias in self._get_aliases(skill):
                                if alias.lower() not in index:
                                    index[alias.lower()] = entry

        logger.info(f"SkillsMatcher v2: built index with {len(index)} skill entries")
        return index

    def _build_known_skills_set(self) -> set:
        known = set(self.skill_index.keys())
        additional = [
            "scala", "java", "go", "rust", "r", "typescript",
            "hadoop", "hive", "presto", "trino", "druid", "flink",
            "fivetran", "stitch", "airbyte", "meltano", "matillion",
            "looker", "tableau", "power bi", "superset", "metabase",
            "debezium", "cdc", "kinesis", "pubsub",
            "aws glue", "azure data factory", "azure synapse",
            "google dataflow", "aws lake formation", "aws emr", "aws athena",
            "delta lake", "apache iceberg", "hudi", "iceberg",
            "prefect", "dagster", "luigi",
            "soda", "monte carlo", "great expectations",
            "unity catalog", "apache atlas", "collibra",
            "mlflow", "vertex ai", "sagemaker",
            "redis", "elasticsearch", "opensearch", "cassandra",
            "dynamodb", "cosmos db",
            "pulsar", "rabbitmq", "nifi", "talend", "informatica",
            "denodo", "dbt cloud", "dbt core",
        ]
        known.update(a.lower() for a in additional)
        return known

    def _get_aliases(self, skill: str) -> list:
        aliases_map = {
            "Apache Airflow": ["airflow", "apache airflow", "mwaa"],
            "Apache Kafka": ["kafka", "apache kafka", "confluent kafka"],
            "Apache Spark": ["spark", "apache spark"],
            "dbt Core": ["dbt", "dbt core"],
            "dbt": ["dbt core", "dbt cloud", "data build tool"],
            "PostgreSQL": ["postgres", "postgresql", "psql"],
            "PySpark": ["pyspark", "py spark"],
            "Great Expectations": ["great expectations", "ge", "gx"],
            "AWS": ["aws", "amazon web services", "amazon aws"],
            "GCP": ["gcp", "google cloud platform", "google cloud"],
            "Azure": ["azure", "microsoft azure"],
            "Docker": ["docker", "docker compose", "dockerfile", "containeris"],
            "Kubernetes": ["kubernetes", "k8s"],
            "Snowflake": ["snowflake"],
            "Databricks": ["databricks", "delta lake"],
            "Terraform": ["terraform", "tf", "infrastructure as code"],
            "Kafka": ["kafka", "apache kafka", "event streaming"],
            "Kafka Connect": ["kafka connect"],
            "Pandas": ["pandas"],
            "SQL": ["sql", "ansi sql", "t-sql", "pl/sql", "nosql"],
            "Python": ["python", "python3"],
        }
        return aliases_map.get(skill, [])

    # ── Core skills match ──────────────────────────────────────────

    def match(self, title: str, description: str) -> Tuple[float, list, list]:
        """Score skills match. Returns (score 0-100, matched, missing)."""
        text = f"{title} {description}".lower()
        jd_skills = set()

        for skill_name in self.all_known_skills:
            if len(skill_name) <= 3:
                if re.search(rf"\b{re.escape(skill_name)}\b", text):
                    jd_skills.add(skill_name)
            else:
                if skill_name in text:
                    jd_skills.add(skill_name)

        if not jd_skills:
            return 0.0, [], []

        matched, missing = [], []
        total_weight, matched_weight = 0.0, 0.0

        for skill in jd_skills:
            if skill in self.skill_index:
                info = self.skill_index[skill]
                matched.append(info["name"])
                matched_weight += info["weight"]
                total_weight += 1.0
            else:
                missing.append(skill)
                total_weight += 1.0

        if total_weight == 0:
            return 0.0, [], []

        score = (matched_weight / total_weight) * 100.0
        return max(0.0, min(100.0, score)), sorted(set(matched)), sorted(set(missing))

    # ── 10-Dimensional scorer ──────────────────────────────────────

    def score_10d(
        self,
        title: str,
        description: str,
        role_score: float,
        skills_score: float,
        salary_min: float = None,
        salary_max: float = None,
        posted_date: date = None,
    ) -> Tuple[float, dict]:
        """
        Compute a 10-dimensional score.
        Returns (overall_score 0-100, breakdown_dict).
        Gate dimensions: if role_match < 35 OR skills_alignment < 30 -> score = 0.
        """
        text = f"{title} {description}".lower()
        title_lower = title.lower()
        breakdown = {}

        # ── D1: Role match (gate) ──────────────────────────
        d1 = min(100.0, role_score)
        breakdown["role_match"] = round(d1, 1)

        # ── D2: Skills alignment (gate) ───────────────────
        d2 = min(100.0, skills_score)
        breakdown["skills_alignment"] = round(d2, 1)

        # Gate check
        if d1 < 35 or d2 < 30:
            breakdown["gate_failed"] = f"role={d1} skills={d2}"
            breakdown["overall"] = 0.0
            return 0.0, breakdown

        # ── D3: Seniority fit (0-100) ─────────────────────
        detected = "mid"
        for level, signals in SENIORITY_SIGNALS.items():
            if any(s in title_lower for s in signals):
                detected = level
                break
        seniority_map = {
            ("junior", "junior"): 100, ("junior", "mid"): 60,  ("junior", "senior"): 20,
            ("mid",    "junior"): 70,  ("mid",    "mid"):  100, ("mid",    "senior"): 50,
            ("senior", "junior"): 30,  ("senior", "mid"):  80,  ("senior", "senior"): 100,
            ("mid",    "manager"): 20, ("junior", "manager"): 10, ("senior", "manager"): 40,
        }
        d3 = seniority_map.get((self.target_seniority, detected), 60)
        breakdown["seniority_fit"] = d3
        breakdown["seniority_detected"] = detected

        # ── D4: Compensation match (0-100) ────────────────
        d4 = 50  # neutral when no salary data
        if salary_min or salary_max:
            job_min = salary_min or 0
            job_max = salary_max or job_min * 1.3
            overlap_low = max(job_min, self.min_salary)
            overlap_high = min(job_max, self.max_salary)
            if overlap_high >= overlap_low:
                range_span = self.max_salary - self.min_salary
                overlap = overlap_high - overlap_low
                d4 = min(100, int((overlap / range_span) * 100)) if range_span > 0 else 80
            elif job_min > self.max_salary:
                d4 = 90  # above your range = good
            else:
                d4 = 20  # below your minimum
        breakdown["compensation"] = d4

        # ── D5: Geographic / remote fit (0-100) ───────────
        remote_keywords = ["remote", "fully remote", "work from home", "wfh", "anywhere in uk"]
        hybrid_keywords = ["hybrid", "flexible working", "2 days", "3 days in office"]
        onsite_keywords = ["on-site", "onsite", "office based", "fully office"]
        if any(k in text for k in remote_keywords):
            d5 = 100 if self.prefer_remote else 60
        elif any(k in text for k in hybrid_keywords):
            d5 = 85
        elif any(k in text for k in onsite_keywords):
            d5 = 30 if self.prefer_remote else 70
        elif self.target_location in text or "glasgow" in text or "scotland" in text:
            d5 = 80
        elif "london" in text or "manchester" in text or "edinburgh" in text:
            d5 = 50
        else:
            d5 = 55  # neutral
        breakdown["geographic"] = d5

        # ── D6: Company stage fit (0-100) ─────────────────
        is_startup = any(s in text for s in STARTUP_SIGNALS)
        is_enterprise = any(s in text for s in ENTERPRISE_SIGNALS)
        if self.prefer_startup is True:
            d6 = 90 if is_startup else (50 if is_enterprise else 65)
        elif self.prefer_startup is False:
            d6 = 90 if is_enterprise else (50 if is_startup else 65)
        else:
            d6 = 70  # neutral
        breakdown["company_stage"] = d6

        # ── D7: Posting quality (0-100) ───────────────────
        quality_hits = sum(1 for k in QUALITY_POSITIVE if k in text)
        quality_misses = sum(1 for k in QUALITY_NEGATIVE if k in text)
        desc_len = len(description or "")
        length_score = min(40, desc_len // 25)  # up to 40pts for 1000+ char JD
        d7 = min(100, (quality_hits * 8) - (quality_misses * 5) + length_score + 20)
        d7 = max(0, d7)
        breakdown["posting_quality"] = d7

        # ── D8: Stack freshness (0-100) ───────────────────
        modern_hits = sum(1 for k in MODERN_STACK if k in text)
        legacy_hits = sum(1 for k in LEGACY_STACK if k in text)
        d8 = min(100, 50 + (modern_hits * 8) - (legacy_hits * 15))
        d8 = max(0, d8)
        breakdown["stack_freshness"] = d8

        # ── D9: Scope fit — IC vs management (0-100) ──────
        management_signals = ["manage a team", "line manage", "people manager",
                               "team lead", "direct reports", "headcount"]
        ic_signals = ["individual contributor", "hands-on", "hands on",
                      "technical depth", "no management"]
        is_mgmt = any(s in text for s in management_signals)
        is_ic = any(s in text for s in ic_signals)
        # Default preference: IC role
        d9 = 40 if is_mgmt else (90 if is_ic else 75)
        breakdown["scope_fit"] = d9

        # ── D10: Timeline — recency of posting (0-100) ────
        if posted_date:
            days_old = (date.today() - posted_date).days
            if days_old <= 1:   d10 = 100
            elif days_old <= 3: d10 = 90
            elif days_old <= 7: d10 = 75
            elif days_old <= 14: d10 = 55
            elif days_old <= 30: d10 = 35
            else:               d10 = 15
        else:
            d10 = 50
        breakdown["timeline"] = d10
        breakdown["days_old"] = (date.today() - posted_date).days if posted_date else None

        # ── Weighted final score ───────────────────────────
        weights = {
            "d1": 0.20,  # role match
            "d2": 0.25,  # skills alignment (highest weight)
            "d3": 0.10,  # seniority
            "d4": 0.10,  # compensation
            "d5": 0.10,  # geographic
            "d6": 0.05,  # company stage
            "d7": 0.05,  # posting quality
            "d8": 0.05,  # stack freshness
            "d9": 0.05,  # scope fit
            "d10": 0.05, # timeline
        }
        overall = (
            d1  * weights["d1"]  +
            d2  * weights["d2"]  +
            d3  * weights["d3"]  +
            d4  * weights["d4"]  +
            d5  * weights["d5"]  +
            d6  * weights["d6"]  +
            d7  * weights["d7"]  +
            d8  * weights["d8"]  +
            d9  * weights["d9"]  +
            d10 * weights["d10"]
        )
        overall = round(max(0.0, min(100.0, overall)), 2)
        breakdown["overall"] = overall
        breakdown["weights"] = weights

        return overall, breakdown

    # ── Legacy 2D scorer (kept for compatibility) ──────────────────

    def compute_overall_score(self, role_score: float, skills_score: float) -> float:
        """
        Legacy 2D scorer. Still called by DAG — now delegates to 10D scorer
        with defaults for unknown dimensions. Keeps DAG untouched.
        """
        overall, _ = self.score_10d(
            title="", description="",
            role_score=role_score, skills_score=skills_score,
        )
        # If gate failed via 10D, fall back to 2D so we don't lose border cases
        if overall == 0.0 and role_score >= 35 and skills_score >= 30:
            return (role_score * 0.4) + (skills_score * 0.6)
        return overall

    # ── Utilities ─────────────────────────────────────────────────

    @staticmethod
    def compute_dedup_hash(title: str, company: str) -> str:
        """Dedup hash — strips noise before hashing."""
        def clean(s: str) -> str:
            s = s.lower().strip()
            s = re.sub(r'[^a-z0-9\s]', '', s)
            s = re.sub(r'\s+', ' ', s).strip()
            for noise in [" contract", " permanent", " perm", " ltd", " limited",
                          " plc", " inc", " llc", " uk", " remote"]:
                s = s.replace(noise, "")
            return s.strip()
        normalized = f"{clean(title)}|{clean(company)}"
        return hashlib.sha256(normalized.encode()).hexdigest()
