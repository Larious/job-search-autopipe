"""
Reed API client for Job Search AutoPipe.
Reed.co.uk is a major UK job board with a free developer API.
Sign up: https://www.reed.co.uk/developers
"""

import base64
import json
import logging
from urllib.request import Request, urlopen
from urllib.parse import urlencode

from .base_client import BaseJobClient, RawJobPosting

logger = logging.getLogger(__name__)


class ReedClient(BaseJobClient):
    """
    Fetches job listings from the Reed API.
    
    Reed uses Basic Auth with your API key as the username.
    Free tier provides generous limits for personal use.
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.api_key = config.get("api_key", "")
        self.base_url = config.get("base_url", "https://www.reed.co.uk/api/1.0")
        # Reed uses Basic Auth: api_key as username, empty password
        self._auth_header = base64.b64encode(f"{self.api_key}:".encode()).decode()

    def fetch_jobs(self, keywords: list, location: str = "Glasgow",
                   radius_miles: int = 30, posted_within_days: int = 1,
                   max_results: int = 100) -> list[RawJobPosting]:
        """Fetch jobs from Reed API."""
        all_postings = []

        for keyword in keywords:
            try:
                postings = self._search(keyword, location, radius_miles, max_results)
                all_postings.extend(postings)
                logger.info(f"Reed: fetched {len(postings)} jobs for '{keyword}'")
            except Exception as e:
                logger.error(f"Reed: error fetching '{keyword}': {e}")

        # Deduplicate
        seen = set()
        unique = []
        for p in all_postings:
            if p.source_job_id not in seen:
                seen.add(p.source_job_id)
                unique.append(p)

        logger.info(f"Reed: {len(unique)} unique postings after dedup")
        return unique

    def _search(self, keyword: str, location: str, radius_miles: int,
                max_results: int) -> list[RawJobPosting]:
        """Execute a single search query against Reed."""
        params = {
            "keywords": keyword,
            "locationName": location,
            "distancefromlocation": radius_miles,
            "resultsToTake": min(max_results, 100),
        }

        url = f"{self.base_url}/search?{urlencode(params)}"
        req = Request(url, headers={
            "Authorization": f"Basic {self._auth_header}",
            "User-Agent": "JobSearchAutoPipe/1.0",
        })

        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())

        return self._parse_response(data)

    def _fetch_details(self, job_id: str) -> dict:
        """Fetch full job details (Reed search results are summaries)."""
        url = f"{self.base_url}/jobs/{job_id}"
        req = Request(url, headers={
            "Authorization": f"Basic {self._auth_header}",
            "User-Agent": "JobSearchAutoPipe/1.0",
        })

        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())

    def _parse_response(self, data) -> list[RawJobPosting]:
        """Parse Reed API response."""
        postings = []

        # Reed returns a dict with 'results' key, or a list directly
        results = data if isinstance(data, list) else data.get("results", [])

        for item in results:
            posting = RawJobPosting(
                source="reed",
                source_job_id=str(item.get("jobId", "")),
                title=item.get("jobTitle", ""),
                company=item.get("employerName", "Unknown"),
                location=item.get("locationName", ""),
                description=self._clean_html(item.get("jobDescription", "")),
                url=item.get("jobUrl", ""),
                salary_min=self._safe_int(item.get("minimumSalary")),
                salary_max=self._safe_int(item.get("maximumSalary")),
                posted_date=self._safe_date(item.get("date")),
                contract_type="permanent" if not item.get("contractType") else item.get("contractType"),
                raw_json=item,
            )
            postings.append(posting)

        return postings
