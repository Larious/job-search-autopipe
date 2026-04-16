"""
Adzuna API client for Job Search AutoPipe.
Adzuna provides free API access for UK job listings.
Sign up: https://developer.adzuna.com
"""

import logging
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus
import json

from .base_client import BaseJobClient, RawJobPosting

logger = logging.getLogger(__name__)


class AdzunaClient(BaseJobClient):
    """
    Fetches job listings from the Adzuna API.
    
    Adzuna is one of the largest UK job aggregators with a generous free tier.
    Free tier: 250 requests/month — more than enough for daily pipeline runs.
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.app_id = config.get("app_id", "")
        self.app_key = config.get("app_key", "")
        self.base_url = config.get("base_url", "https://api.adzuna.com/v1/api/jobs")
        self.country = config.get("country", "gb")

    def fetch_jobs(self, keywords: list, location: str = "Glasgow",
                   radius_miles: int = 30, posted_within_days: int = 1,
                   max_results: int = 50) -> list[RawJobPosting]:
        """
        Fetch jobs from Adzuna API.
        
        Args:
            keywords: Search terms (e.g., ["data engineer"])
            location: City or area name
            radius_miles: Search radius from location
            posted_within_days: Only return jobs posted within N days
            max_results: Maximum results to return
        """
        all_postings = []

        for keyword in keywords:
            try:
                postings = self._search(keyword, location, radius_miles,
                                         posted_within_days, max_results)
                all_postings.extend(postings)
                logger.info(f"Adzuna: fetched {len(postings)} jobs for '{keyword}'")
            except Exception as e:
                logger.error(f"Adzuna: error fetching '{keyword}': {e}")

        # Deduplicate by source_job_id within this batch
        seen = set()
        unique = []
        for p in all_postings:
            if p.source_job_id not in seen:
                seen.add(p.source_job_id)
                unique.append(p)

        logger.info(f"Adzuna: {len(unique)} unique postings after dedup")
        return unique

    def _search(self, keyword: str, location: str, radius_miles: int,
                posted_within_days: int, max_results: int) -> list[RawJobPosting]:
        """Execute a single search query."""
        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": min(max_results, 50),
            "what": keyword,
            "max_days_old": posted_within_days,
            "sort_by": "date",
            "content-type": "application/json",
        }
        if location:
            params["where"] = location
            params["distance"] = radius_miles

        url = f"{self.base_url}/{self.country}/search/1?{urlencode(params)}"
        req = Request(url, headers={"User-Agent": "JobSearchAutoPipe/1.0"})

        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())

        return self._parse_response(data)

    def _parse_response(self, data: dict) -> list[RawJobPosting]:
        """Parse Adzuna API response into standardised format."""
        postings = []
        results = data.get("results", [])

        for item in results:
            posting = RawJobPosting(
                source="adzuna",
                source_job_id=str(item.get("id", "")),
                title=item.get("title", ""),
                company=item.get("company", {}).get("display_name", "Unknown"),
                location=item.get("location", {}).get("display_name", ""),
                description=self._clean_html(item.get("description", "")),
                url=item.get("redirect_url", ""),
                salary_min=self._safe_int(item.get("salary_min")),
                salary_max=self._safe_int(item.get("salary_max")),
                posted_date=self._safe_date(item.get("created")),
                contract_type=item.get("contract_type"),
                raw_json=item,
            )
            postings.append(posting)

        return postings
