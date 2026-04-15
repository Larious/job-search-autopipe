"""
Telegram notification utility for Job Search AutoPipe.
Sends pipeline alerts and daily job digests via Telegram Bot API.

Setup:
  1. Message @BotFather on Telegram → /newbot → save the token
  2. Create a group chat and add your bot
  3. Get the chat_id:
     curl https://api.telegram.org/bot<TOKEN>/getUpdates
     Look for "chat": {"id": -100XXXXXXXXXX}
  4. Add token + chat_id to config.yaml
"""

import json
import logging
from datetime import datetime
from typing import Optional
from urllib.request import Request, urlopen
from urllib.error import URLError
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Sends formatted messages to Telegram via Bot API."""

    BASE_URL = "https://api.telegram.org/bot{token}"

    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = self.BASE_URL.format(token=bot_token)

    def _send(self, text: str, parse_mode: str = "Markdown") -> bool:
        """Send a message via Telegram Bot API."""
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }

        try:
            data = json.dumps(payload).encode("utf-8")
            req = Request(
                f"{self.api_url}/sendMessage",
                data=data,
                headers={"Content-Type": "application/json"},
            )
            with urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read().decode())
                if not result.get("ok"):
                    logger.error(f"Telegram API error: {result}")
                    return False
            logger.info("Telegram notification sent successfully.")
            return True
        except URLError as e:
            logger.error(f"Failed to send Telegram notification: {e}")
            return False

    def send_message(self, text: str):
        """Send a simple text message."""
        self._send(text)

    def send_pipeline_alert(self, phase: str, status: str,
                            records_in: int = 0, records_out: int = 0,
                            error: Optional[str] = None):
        """Send a pipeline status alert."""
        emoji = "✅" if status == "success" else "❌" if status == "failed" else "🔄"

        msg = (
            f"{emoji} *Pipeline: {phase}*\n"
            f"\n"
            f"Status: `{status.upper()}`\n"
            f"Time: {datetime.now().strftime('%H:%M:%S')}\n"
            f"Records in: {records_in}\n"
            f"Records out: {records_out}"
        )

        if error:
            # Truncate and escape for Markdown
            safe_error = error[:300].replace("`", "'")
            msg += f"\n\nError:\n```\n{safe_error}\n```"

        self._send(msg)

    def send_daily_digest(self, jobs: list, digest_date: Optional[str] = None):
        """
        Send the morning job digest to Telegram.

        Args:
            jobs: List of dicts with keys: rank, title, company, location,
                  overall_score, skills_match_score, url, matched_skills
        """
        if digest_date is None:
            digest_date = datetime.now().strftime("%A, %d %B %Y")

        if not jobs:
            self._send(f"📭 *Job Digest — {digest_date}*\n\nNo new matching data engineering roles today.")
            return

        # Telegram has a 4096 char limit per message, so we may need to split
        header = (
            f"📬 *Job Digest — {digest_date}*\n"
            f"Found *{len(jobs)}* matching roles\n"
            f"{'─' * 28}\n"
        )

        job_blocks = []
        for job in jobs[:10]:
            score_icon = "🟢" if job["overall_score"] >= 80 else "🟡" if job["overall_score"] >= 60 else "🟠"
            skills = ", ".join(job.get("matched_skills", [])[:4])

            block = (
                f"\n*#{job['rank']}* {score_icon} *{self._escape_md(job['title'])}*\n"
                f"🏢 {self._escape_md(job['company'])} · 📍 {self._escape_md(job['location'])}\n"
                f"Match: *{job['overall_score']:.0f}%* · {skills}\n"
                f"[View & Apply]({job['url']})"
            )
            job_blocks.append(block)

        # Build message, splitting if needed
        full_msg = header + "\n".join(job_blocks)
        full_msg += f"\n\n{'─' * 28}\nReply with a job number to flag it"

        if len(full_msg) <= 4000:
            self._send(full_msg)
        else:
            # Split into two messages
            mid = len(job_blocks) // 2
            msg1 = header + "\n".join(job_blocks[:mid])
            msg2 = "\n".join(job_blocks[mid:]) + f"\n\n{'─' * 28}\nReply with a job number to flag it"
            self._send(msg1)
            self._send(msg2)

        logger.info(f"Daily digest sent with {len(jobs)} jobs via Telegram.")

    @staticmethod
    def _escape_md(text: str) -> str:
        """Escape special Markdown characters in user-generated text."""
        if not text:
            return ""
        for char in ("_", "*", "`", "["):
            text = text.replace(char, f"\\{char}")
        return text
