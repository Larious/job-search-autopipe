"""
Base client for job board API integration.
All job source clients inherit from this to ensure consistent data format.
"""

import hashlib
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RawJobPosting:
    """Standardised raw job posting from any source."""
    source: str
    source_job_id: str
    title: str
    company: str
    location: str
    description: str
    url: str
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    posted_date: Optional[str] = None
    contract_type: Optional[str] = None
    raw_json: Optional[dict] = None

    @property
    def content_hash(self) -> str:
        """SHA-256 hash for deduplication."""
        content = json.dumps(self.raw_json or asdict(self), sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()


class BaseJobClient(ABC):
    """
    Abstract base class for job board API clients.
    Each source (Adzuna, Reed, The Muse) implements this interface.
    """

    def __init__(self, config: dict):
        self.config = config
        self.source_name = self.__class__.__name__.replace("Client", "").lower()

    @abstractmethod
    def fetch_jobs(self, keywords: list, location: str,
                   radius_miles: int = 30, posted_within_days: int = 1,
                   max_results: int = 50) -> list[RawJobPosting]:
        """
        Fetch job listings from this source.
        Returns a list of RawJobPosting objects.
        """
        pass

    @abstractmethod
    def _parse_response(self, data: dict) -> list[RawJobPosting]:
        """Parse the API response into standardised RawJobPosting objects."""
        pass

    def _safe_int(self, value, default=None) -> Optional[int]:
        """Safely convert a value to int."""
        if value is None:
            return default
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return default

    def _safe_date(self, value) -> Optional[str]:
        """Safely parse a date string to ISO format."""
        if value is None:
            return None
        if isinstance(value, (date, datetime)):
            return value.isoformat()[:10]
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(str(value)[:19], fmt).date().isoformat()
            except ValueError:
                continue
        return None

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags from description text."""
        import re
        if not text:
            return ""
        clean = re.sub(r"<[^>]+>", " ", text)
        clean = re.sub(r"\s+", " ", clean).strip()
        return clean
