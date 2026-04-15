"""
Slack notification utility for Job Search AutoPipe.
Sends pipeline alerts and daily job digests via webhooks.
"""

import json
import logging
from datetime import datetime
from typing import Optional
from urllib.request import Request, urlopen
from urllib.error import URLError

logger = logging.getLogger(__name__)


class SlackNotifier:
    """Sends formatted messages to Slack via webhook."""

    def __init__(self, webhook_url: str, channel: str = "#job-alerts"):
        self.webhook_url = webhook_url
        self.channel = channel

    def _send(self, payload: dict) -> bool:
        """Send a payload to Slack."""
        try:
            data = json.dumps(payload).encode("utf-8")
            req = Request(self.webhook_url, data=data, headers={"Content-Type": "application/json"})
            urlopen(req, timeout=10)
            logger.info("Slack notification sent successfully.")
            return True
        except URLError as e:
            logger.error(f"Failed to send Slack notification: {e}")
            return False

    def send_message(self, text: str):
        """Send a simple text message."""
        self._send({"channel": self.channel, "text": text})

    def send_pipeline_alert(self, phase: str, status: str,
                            records_in: int = 0, records_out: int = 0,
                            error: Optional[str] = None):
        """Send a pipeline status alert."""
        emoji = "✅" if status == "success" else "❌" if status == "failed" else "🔄"
        color = "#36a64f" if status == "success" else "#ff0000" if status == "failed" else "#ffaa00"

        blocks = {
            "channel": self.channel,
            "attachments": [{
                "color": color,
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": f"{emoji} Pipeline: {phase}"}
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Status:* {status.upper()}"},
                            {"type": "mrkdwn", "text": f"*Time:* {datetime.now().strftime('%H:%M:%S')}"},
                            {"type": "mrkdwn", "text": f"*Records In:* {records_in}"},
                            {"type": "mrkdwn", "text": f"*Records Out:* {records_out}"},
                        ]
                    },
                ]
            }]
        }

        if error:
            blocks["attachments"][0]["blocks"].append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Error:*\n```{error[:500]}```"}
            })

        self._send(blocks)

    def send_daily_digest(self, jobs: list, digest_date: Optional[str] = None):
        """
        Send the morning job digest to Slack.
        
        Args:
            jobs: List of dicts with keys: rank, title, company, location,
                  overall_score, skills_match_score, url, matched_skills
        """
        if digest_date is None:
            digest_date = datetime.now().strftime("%A, %d %B %Y")

        if not jobs:
            self._send({
                "channel": self.channel,
                "text": f"📭 *Job Digest — {digest_date}*\nNo new matching data engineering roles today. Pipeline ran successfully."
            })
            return

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"📬 Job Digest — {digest_date}"}
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"Found *{len(jobs)}* matching data engineering roles. Here are the top matches:"}
            },
            {"type": "divider"},
        ]

        for job in jobs[:10]:  # Top 10 in digest
            score_bar = "🟢" if job["overall_score"] >= 80 else "🟡" if job["overall_score"] >= 60 else "🟠"
            skills = ", ".join(job.get("matched_skills", [])[:5])

            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*#{job['rank']}* {score_bar} *{job['title']}*\n"
                        f"🏢 {job['company']} · 📍 {job['location']}\n"
                        f"Match: *{job['overall_score']:.0f}%* · Skills: {skills}\n"
                        f"<{job['url']}|View & Apply>"
                    )
                }
            })
            blocks.append({"type": "divider"})

        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": "Reply with a job number to flag it for application prep. E.g., `/flag 3`"
            }]
        })

        self._send({
            "channel": self.channel,
            "attachments": [{"color": "#4A90D9", "blocks": blocks}]
        })
        logger.info(f"Daily digest sent with {len(jobs)} jobs.")
