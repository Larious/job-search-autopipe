"""
Configuration loader for Job Search AutoPipe.
Reads YAML config and provides typed access to settings.
"""

import os
import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


CONFIG_DIR = Path(__file__).parent.parent.parent / "config"


def load_config(config_path: Optional[str] = None) -> dict:
    """Load the main configuration file."""
    if config_path is None:
        config_path = os.getenv("AUTOPIPE_CONFIG", str(CONFIG_DIR / "config.yaml"))

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Config not found at {path}. "
            f"Copy config.example.yaml to config.yaml and fill in your values."
        )

    with open(path) as f:
        return yaml.safe_load(f)


def load_skills_profile(profile_path: Optional[str] = None) -> dict:
    """Load the skills profile for JD matching."""
    if profile_path is None:
        profile_path = str(CONFIG_DIR / "skills_profile.yaml")

    with open(profile_path) as f:
        return yaml.safe_load(f)


@dataclass
class APIConfig:
    """Configuration for a single job board API."""
    name: str
    base_url: str
    results_per_page: int = 50
    api_key: Optional[str] = None
    app_id: Optional[str] = None
    app_key: Optional[str] = None
    country: str = "gb"


@dataclass
class SearchConfig:
    """Search parameters."""
    primary_keywords: list = field(default_factory=list)
    secondary_keywords: list = field(default_factory=list)
    city: str = "Glasgow"
    radius_miles: int = 30
    include_remote: bool = True
    min_salary: int = 0
    max_salary: int = 200000
    posted_within_days: int = 1


@dataclass
class PipelineConfig:
    """Full pipeline configuration with typed access."""
    apis: dict = field(default_factory=dict)
    search: SearchConfig = field(default_factory=SearchConfig)
    database: dict = field(default_factory=dict)
    notifications: dict = field(default_factory=dict)
    classifier: dict = field(default_factory=dict)
    cover_letter: dict = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, config_path: Optional[str] = None) -> "PipelineConfig":
        raw = load_config(config_path)

        search_raw = raw.get("search", {})
        keywords = search_raw.get("keywords", {})
        location = search_raw.get("location", {})
        filters = search_raw.get("filters", {})

        search = SearchConfig(
            primary_keywords=keywords.get("primary", []),
            secondary_keywords=keywords.get("secondary", []),
            city=location.get("city", "Glasgow"),
            radius_miles=location.get("radius_miles", 30),
            include_remote=location.get("include_remote", True),
            min_salary=filters.get("min_salary", 0),
            max_salary=filters.get("max_salary", 200000),
            posted_within_days=filters.get("posted_within_days", 1),
        )

        return cls(
            apis=raw.get("apis", {}),
            search=search,
            database=raw.get("database", {}),
            notifications=raw.get("notifications", {}),
            classifier=raw.get("classifier", {}),
            cover_letter=raw.get("cover_letter", {}),
        )
