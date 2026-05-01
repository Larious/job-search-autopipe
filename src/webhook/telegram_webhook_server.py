"""
Telegram Webhook Server for Job Search AutoPipe.

Receives incoming messages from Telegram via webhook and executes
pipeline commands — turning your Telegram chat into a control plane.

Commands:
  /digest          — Show today's digest
  /flag <id>       — Flag a job for application prep
  /cover <id>      — Generate a tailored cover letter
  /stats           — Pipeline statistics
  /status          — Check pipeline health
  /help            — Show available commands

Architecture:
  Telegram → Webhook (POST /webhook) → Flask handler → Pipeline action
  
  This replaces polling (getUpdates) with push — Telegram delivers
  messages to your server in real time via HTTPS POST.

Setup:
  1. Expose this server publicly (ngrok for dev, reverse proxy for prod)
  2. Register the webhook with Telegram:
     curl -X POST "https://api.telegram.org/bot<TOKEN>/setWebhook" \\
       -H "Content-Type: application/json" \\
       -d '{"url": "https://your-domain.com/webhook"}'
  3. Verify: curl "https://api.telegram.org/bot<TOKEN>/getWebhookInfo"
"""

import json
import logging
import os
import sys
import hmac
import hashlib
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.utils.config_loader import PipelineConfig, load_skills_profile
from src.utils.database import Database
from src.utils.telegram_notifier import TelegramNotifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("telegram_webhook")


class WebhookHandler(BaseHTTPRequestHandler):
    """
    HTTP handler for Telegram webhook updates.

    Telegram sends a JSON POST to /webhook for every message
    sent to the bot. We parse the command and execute the
    corresponding pipeline action.
    """

    # These are set by the server factory
    config: PipelineConfig = None
    db: Database = None
    notifier: TelegramNotifier = None
    bot_token: str = ""
    allowed_chat_ids: set = set()

    def do_POST(self):
        """Handle incoming webhook POST from Telegram."""
        if self.path != "/webhook":
            self._respond(404, "Not found")
            return

        # Read body
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length == 0 or content_length > 65536:
            self._respond(400, "Bad request")
            return

        body = self.rfile.read(content_length)

        try:
            update = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            self._respond(400, "Invalid JSON")
            return

        # Respond 200 immediately (Telegram retries on non-200)
        self._respond(200, "OK")

        # Process asynchronously-ish (in same thread, but after response)
        try:
            self._process_update(update)
        except Exception as e:
            logger.error(f"Error processing update: {e}", exc_info=True)

    def do_GET(self):
        """Health check endpoint."""
        if self.path == "/health":
            self._respond(200, json.dumps({
                "status": "healthy",
                "service": "job-search-autopipe-webhook",
                "timestamp": datetime.now().isoformat(),
            }))
        else:
            self._respond(404, "Not found")

    def _respond(self, code: int, body: str):
        """Send HTTP response."""
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def _process_update(self, update: dict):
        """Route an incoming Telegram update to the right handler."""
        message = update.get("message") or update.get("callback_query", {}).get("message")
        if not message:
            return

        chat_id = str(message.get("chat", {}).get("id", ""))
        text = (update.get("message", {}).get("text", "") or
                update.get("callback_query", {}).get("data", ""))

        # Security: only respond to authorised chat(s)
        if self.allowed_chat_ids and chat_id not in self.allowed_chat_ids:
            logger.warning(f"Unauthorised chat_id: {chat_id}")
            return

        if not text:
            return

        text = text.strip()
        logger.info(f"Received command: {text} from chat {chat_id}")

        # Route commands
        if text.startswith("/digest"):
            self._cmd_digest(chat_id)
        elif text.startswith("/flag"):
            self._cmd_flag(chat_id, text)
        elif text.startswith("/cover"):
            self._cmd_cover(chat_id, text)
        elif text.startswith("/update"):
            self._cmd_update(chat_id, text)
        elif text.startswith("/funnel"):
            self._cmd_funnel(chat_id)
        elif text.startswith("/analytics"):
            self._cmd_analytics(chat_id)
        elif text.startswith("/stats"):
            self._cmd_stats(chat_id)
        elif text.startswith("/status"):
            self._cmd_status(chat_id)
        elif text.startswith("/help") or text.startswith("/start"):
            self._cmd_help(chat_id)
        elif text.isdigit():
            # Shorthand: just a number = flag that job
            self._cmd_flag(chat_id, f"/flag {text}")
        else:
            self._send_to(chat_id,
                "I didn't recognise that command. "
                "Type /help to see what I can do."
            )

    # ── Command Handlers ───────────────────────────────────

    def _cmd_help(self, chat_id: str):
        """Show available commands."""
        self._send_to(chat_id, (
            "🔧 *Job Search AutoPipe — Commands*\n"
            "\n"
            "*Discovery*\n"
            "/digest — Today's job matches\n"
            "/flag `<id>` — Flag a job for application\n"
            "/cover `<id>` — Generate cover letter\n"
            "\n"
            "*Outcome tracking*\n"
            "/update `<id>` `<status>` — Update application status\n"
            "  Statuses: `applied`, `interviewing`, `rejected`, `ghosted`, `offer`\n"
            "/funnel — Application funnel breakdown\n"
            "/analytics — Score vs outcome insights\n"
            "\n"
            "*Pipeline*\n"
            "/stats — Pipeline statistics\n"
            "/status — Pipeline health check\n"
            "/help — This message\n"
            "\n"
            "💡 Reply with a number after digest to flag that job."
        ))

    def _cmd_digest(self, chat_id: str):
        """Show today's digest."""
        candidates = self.db.get_digest_candidates(limit=15)

        if not candidates:
            self._send_to(chat_id, "📭 No new matching jobs today. Pipeline ran successfully.")
            return

        header = (
            f"📬 *Digest — {datetime.now().strftime('%d %b %Y')}*\n"
            f"{'─' * 28}\n"
        )

        blocks = []
        for i, job in enumerate(candidates, 1):
            matched = job.get("matched_skills", [])
            if isinstance(matched, str):
                matched = json.loads(matched)

            icon = "🟢" if job["overall_score"] >= 80 else "🟡" if job["overall_score"] >= 60 else "🟠"
            skills = ", ".join(matched[:3]) if matched else "—"

            blocks.append(
                f"*{i}.* {icon} {TelegramNotifier._escape_md(job['title'])}\n"
                f"   🏢 {TelegramNotifier._escape_md(job['company'])}\n"
                f"   📍 {TelegramNotifier._escape_md(job['location'])}\n"
                f"   Score: *{job['overall_score']:.0f}%* · {skills}\n"
                f"   ID: `{job['id']}` · [Apply]({job['url']})"
            )

        msg = header + "\n\n".join(blocks)
        msg += f"\n\n{'─' * 28}\nReply with `/flag <ID>` to start an application"

        # Split if too long
        if len(msg) <= 4000:
            self._send_to(chat_id, msg)
        else:
            mid = len(blocks) // 2
            self._send_to(chat_id, header + "\n\n".join(blocks[:mid]))
            self._send_to(chat_id, "\n\n".join(blocks[mid:])
                          + f"\n\n{'─' * 28}\nReply with `/flag <ID>` to start an application")

    def _cmd_flag(self, chat_id: str, text: str):
        """Flag a job for application."""
        parts = text.split()
        if len(parts) < 2 or not parts[1].isdigit():
            self._send_to(chat_id, "Usage: `/flag <silver_id>`\nGet IDs from /digest")
            return

        silver_id = int(parts[1])

        # Verify job exists
        with self.db.cursor() as cur:
            cur.execute("SELECT title, company FROM silver.classified_jobs WHERE id = %s;", (silver_id,))
            job = cur.fetchone()

        if not job:
            self._send_to(chat_id, f"❌ No job found with ID `{silver_id}`")
            return

        tracker_id = self.db.flag_for_application(silver_id)

        if tracker_id:
            self._send_to(chat_id, (
                f"✅ *Flagged for application*\n\n"
                f"📋 {TelegramNotifier._escape_md(job['title'])}\n"
                f"🏢 {TelegramNotifier._escape_md(job['company'])}\n\n"
                f"Next: `/cover {silver_id}` to generate a tailored cover letter"
            ))
        else:
            self._send_to(chat_id, f"⚠️ Job `{silver_id}` was already flagged.")

    def _cmd_cover(self, chat_id: str, text: str):
        """Generate a cover letter for a flagged job."""
        parts = text.split()
        if len(parts) < 2 or not parts[1].isdigit():
            self._send_to(chat_id, "Usage: `/cover <silver_id>`")
            return

        silver_id = int(parts[1])

        # Get job details
        with self.db.cursor() as cur:
            cur.execute("SELECT * FROM silver.classified_jobs WHERE id = %s;", (silver_id,))
            job = cur.fetchone()

        if not job:
            self._send_to(chat_id, f"❌ No job found with ID `{silver_id}`")
            return

        self._send_to(chat_id, f"✍️ Generating cover letter for *{TelegramNotifier._escape_md(job['title'])}*...")

        try:
            from src.generation.cover_letter_generator import CoverLetterGenerator
            profile = load_skills_profile()
            generator = CoverLetterGenerator(self.config.cover_letter, profile)

            matched = job.get("matched_skills", [])
            missing = job.get("missing_skills", [])
            if isinstance(matched, str):
                matched = json.loads(matched)
            if isinstance(missing, str):
                missing = json.loads(missing)

            body = generator.generate(dict(job), matched, missing)
            full_letter = generator.format_full_letter(body, dict(job))

            # Save to tracker
            with self.db.cursor() as cur:
                cur.execute(
                    "SELECT id FROM gold.application_tracker WHERE silver_id = %s;",
                    (silver_id,)
                )
                tracker = cur.fetchone()
                if tracker:
                    self.db.update_application(tracker["id"], cover_letter=full_letter)

            # Send the letter (may need splitting)
            header = f"📝 *Cover letter for {TelegramNotifier._escape_md(job['company'])}*\n{'─' * 28}\n\n"
            letter_msg = header + body

            if len(letter_msg) <= 4000:
                self._send_to(chat_id, letter_msg)
            else:
                self._send_to(chat_id, header + body[:3500] + "...")
                self._send_to(chat_id, "..." + body[3500:])

            self._send_to(chat_id, "✅ Cover letter saved to application tracker.")

        except Exception as e:
            logger.error(f"Cover letter generation failed: {e}", exc_info=True)
            self._send_to(chat_id, f"❌ Generation failed: {str(e)[:200]}")

    def _cmd_update(self, chat_id: str, text: str):
        """Update application status with outcome tracking."""
        # Parse: /update <silver_id> <status> [reason]
        parts = text.split(maxsplit=3)
        if len(parts) < 3:
            self._send_to(chat_id, (
                "Usage: `/update <id> <status>`\n\n"
                "Statuses:\n"
                "  `applied` — Submitted application\n"
                "  `interviewing` — Got an interview\n"
                "  `rejected` — Received rejection\n"
                "  `ghosted` — No response after 14+ days\n"
                "  `offer` — Received an offer\n\n"
                "Examples:\n"
                "  `/update 42 applied`\n"
                "  `/update 42 rejected experience`\n"
                "  `/update 42 offer`"
            ))
            return

        silver_id = parts[1]
        new_status = parts[2].lower()
        reason = parts[3] if len(parts) > 3 else None

        if not silver_id.isdigit():
            self._send_to(chat_id, "❌ ID must be a number. Usage: `/update <id> <status>`")
            return

        valid_statuses = {"applied", "interviewing", "rejected", "ghosted", "offer"}
        if new_status not in valid_statuses:
            self._send_to(chat_id, f"❌ Invalid status `{new_status}`.\nValid: {', '.join(sorted(valid_statuses))}")
            return

        silver_id = int(silver_id)

        with self.db.cursor() as cur:
            # Find the tracker entry
            cur.execute("""
                SELECT a.id, a.status, s.title, s.company
                FROM gold.application_tracker a
                JOIN silver.classified_jobs s ON a.silver_id = s.id
                WHERE a.silver_id = %s;
            """, (silver_id,))
            row = cur.fetchone()

        if not row:
            self._send_to(chat_id, f"❌ No tracked application for ID `{silver_id}`. Flag it first with `/flag {silver_id}`")
            return

        # Build update fields based on status
        update_fields = {"status": new_status}
        now_sql = "NOW()"

        timestamp_map = {
            "applied": "applied_at",
            "interviewing": "interview_at",
            "rejected": "rejected_at",
            "offer": "offer_at",
        }

        with self.db.cursor() as cur:
            set_parts = ["status = %s", "updated_at = NOW()"]
            values = [new_status]

            if new_status in timestamp_map:
                col = timestamp_map[new_status]
                set_parts.append(f"{col} = NOW()")

            if new_status == "applied" and row["status"] in ("new", "reviewing"):
                set_parts.append("applied_at = NOW()")
                set_parts.append("follow_up_date = (NOW() + INTERVAL '7 days')::date")
                set_parts.append("follow_up_sent = FALSE")

            if new_status in ("rejected", "ghosted") and reason:
                set_parts.append("rejection_reason = %s")
                values.append(reason[:200])

            if new_status == "interviewing":
                set_parts.append("interview_rounds = interview_rounds + 1")

            # Set response_at on first non-ghosted response
            if new_status in ("interviewing", "rejected", "offer"):
                set_parts.append("response_at = COALESCE(response_at, NOW())")

            values.append(row["id"])
            sql = f"UPDATE gold.application_tracker SET {', '.join(set_parts)} WHERE id = %s;"
            cur.execute(sql, values)

        # Confirmation message
        status_emoji = {
            "applied": "📤", "interviewing": "🎯",
            "rejected": "❌", "ghosted": "👻", "offer": "🎉",
        }
        emoji = status_emoji.get(new_status, "📋")

        msg = (
            f"{emoji} *Status updated*\n\n"
            f"📋 {TelegramNotifier._escape_md(row['title'])}\n"
            f"🏢 {TelegramNotifier._escape_md(row['company'])}\n"
            f"  {row['status']} → *{new_status}*"
        )
        if reason:
            msg += f"\n  Reason: {reason}"

        self._send_to(chat_id, msg)

    def _cmd_funnel(self, chat_id: str):
        """Show application funnel breakdown."""
        with self.db.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status != 'new') AS total_applied,
                    COUNT(*) FILTER (WHERE response_at IS NOT NULL) AS got_response,
                    COUNT(*) FILTER (WHERE interview_at IS NOT NULL) AS got_interview,
                    COUNT(*) FILTER (WHERE offer_at IS NOT NULL) AS got_offer,
                    COUNT(*) FILTER (
                        WHERE status = 'applied'
                        AND response_at IS NULL
                        AND applied_at < NOW() - INTERVAL '14 days'
                    ) AS ghosted,
                    COUNT(*) FILTER (WHERE status = 'rejected') AS rejected,
                    ROUND(AVG(
                        CASE WHEN days_to_response IS NOT NULL
                        THEN days_to_response END
                    )::numeric, 1) AS avg_days_to_response
                FROM gold.application_tracker;
            """)
            f = cur.fetchone()

            # Weekly trend
            cur.execute("""
                SELECT
                    DATE_TRUNC('week', applied_at)::DATE AS week,
                    COUNT(*) AS applied,
                    COUNT(*) FILTER (WHERE response_at IS NOT NULL) AS responses
                FROM gold.application_tracker
                WHERE applied_at IS NOT NULL
                GROUP BY 1
                ORDER BY 1 DESC
                LIMIT 6;
            """)
            weeks = cur.fetchall()

        total = f["total_applied"] or 0
        if total == 0:
            self._send_to(chat_id, "📊 No applications tracked yet. Use `/flag` then `/update` to start tracking.")
            return

        response_rate = (f["got_response"] / total * 100) if total else 0
        interview_rate = (f["got_interview"] / total * 100) if total else 0
        offer_rate = (f["got_offer"] / total * 100) if total else 0

        # Visual funnel bars
        def bar(count, total, width=12):
            filled = round(count / max(total, 1) * width)
            return "█" * filled + "░" * (width - filled)

        msg = (
            f"📊 *Application Funnel*\n"
            f"{'─' * 28}\n\n"
            f"📤 Applied:      {bar(total, total)} *{total}*\n"
            f"💬 Got response: {bar(f['got_response'], total)} *{f['got_response']}* ({response_rate:.0f}%)\n"
            f"🎯 Interview:    {bar(f['got_interview'], total)} *{f['got_interview']}* ({interview_rate:.0f}%)\n"
            f"🎉 Offer:        {bar(f['got_offer'], total)} *{f['got_offer']}* ({offer_rate:.0f}%)\n"
            f"\n"
            f"👻 Ghosted: {f['ghosted']}  ·  ❌ Rejected: {f['rejected']}\n"
            f"⏱ Avg response time: {f['avg_days_to_response'] or '—'} days"
        )

        if weeks:
            msg += f"\n\n📅 *Weekly trend*\n"
            for w in weeks:
                msg += f"  {w['week']}: {w['applied']} applied, {w['responses']} responses\n"

        self._send_to(chat_id, msg)

    def _cmd_analytics(self, chat_id: str):
        """Show score vs outcome analytics — what's actually working."""
        with self.db.cursor() as cur:
            # Score bucket conversion rates
            cur.execute("""
                SELECT
                    CASE
                        WHEN s.overall_score >= 80 THEN '80-100 (excellent)'
                        WHEN s.overall_score >= 60 THEN '60-79 (good)'
                        WHEN s.overall_score >= 40 THEN '40-59 (fair)'
                        ELSE '0-39 (low)'
                    END AS score_range,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE a.response_at IS NOT NULL) AS responses,
                    COUNT(*) FILTER (WHERE a.interview_at IS NOT NULL) AS interviews,
                    ROUND(
                        COUNT(*) FILTER (WHERE a.response_at IS NOT NULL)::numeric
                        / NULLIF(COUNT(*), 0) * 100, 0
                    ) AS response_pct
                FROM gold.application_tracker a
                JOIN silver.classified_jobs s ON a.silver_id = s.id
                WHERE a.status != 'new'
                GROUP BY 1
                ORDER BY 1 DESC;
            """)
            buckets = cur.fetchall()

            # Top rejection reasons
            cur.execute("""
                SELECT rejection_reason, COUNT(*) as cnt
                FROM gold.application_tracker
                WHERE rejection_reason IS NOT NULL
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 5;
            """)
            reasons = cur.fetchall()

            # Source effectiveness
            cur.execute("""
                SELECT
                    s.source,
                    COUNT(*) AS applied,
                    COUNT(*) FILTER (WHERE a.interview_at IS NOT NULL) AS interviews,
                    ROUND(
                        COUNT(*) FILTER (WHERE a.interview_at IS NOT NULL)::numeric
                        / NULLIF(COUNT(*), 0) * 100, 0
                    ) AS interview_pct
                FROM gold.application_tracker a
                JOIN silver.classified_jobs s ON a.silver_id = s.id
                WHERE a.status != 'new'
                GROUP BY 1
                ORDER BY interview_pct DESC;
            """)
            sources = cur.fetchall()

            # Referral vs cold
            cur.execute("""
                SELECT
                    CASE WHEN referral THEN 'Referral' ELSE 'Cold apply' END AS method,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE interview_at IS NOT NULL) AS interviews,
                    ROUND(
                        COUNT(*) FILTER (WHERE interview_at IS NOT NULL)::numeric
                        / NULLIF(COUNT(*), 0) * 100, 0
                    ) AS interview_pct
                FROM gold.application_tracker
                WHERE status != 'new'
                GROUP BY 1;
            """)
            referrals = cur.fetchall()

        if not buckets:
            self._send_to(chat_id, "📈 Not enough data yet. Track more applications with `/update` first.")
            return

        msg = f"📈 *Score vs Outcome Analysis*\n{'─' * 28}\n\n"

        msg += "*Match score → response rate:*\n"
        for b in buckets:
            msg += f"  {b['score_range']}: {b['total']} applied → {b['responses']} responses (*{b['response_pct']}%*), {b['interviews']} interviews\n"

        if sources:
            msg += f"\n*By source:*\n"
            for s in sources:
                msg += f"  {s['source']}: {s['applied']} applied → {s['interview_pct']}% interview rate\n"

        if referrals:
            msg += f"\n*Referral vs cold:*\n"
            for r in referrals:
                msg += f"  {r['method']}: {r['total']} applied → *{r['interview_pct']}%* interview rate\n"

        if reasons:
            msg += f"\n*Top rejection reasons:*\n"
            for r in reasons:
                msg += f"  {r['rejection_reason']}: {r['cnt']}\n"

        self._send_to(chat_id, msg)

    def _cmd_stats(self, chat_id: str):
        """Show pipeline statistics."""
        with self.db.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM bronze.raw_job_postings;")
            bronze_count = cur.fetchone()["cnt"]

            cur.execute("""
                SELECT COUNT(*) as total,
                       COUNT(*) FILTER (WHERE is_genuine_de_role) as genuine,
                       ROUND(AVG(overall_score)::numeric, 1) as avg_score
                FROM silver.classified_jobs;
            """)
            silver = cur.fetchone()

            cur.execute("""
                SELECT status, COUNT(*) as cnt
                FROM gold.application_tracker
                GROUP BY status ORDER BY cnt DESC;
            """)
            apps = cur.fetchall()

            cur.execute("""
                SELECT phase, status, records_out, completed_at
                FROM meta.pipeline_runs
                ORDER BY started_at DESC LIMIT 3;
            """)
            runs = cur.fetchall()

        app_lines = "\n".join(f"  {a['status']}: {a['cnt']}" for a in apps) if apps else "  None yet"

        run_lines = "\n".join(
            f"  {r['phase']}: {r['status']} ({r['records_out']} out)"
            for r in runs
        ) if runs else "  No runs yet"

        self._send_to(chat_id, (
            f"📊 *Pipeline Statistics*\n"
            f"{'─' * 28}\n\n"
            f"🥉 *Bronze*: {bronze_count} raw postings\n\n"
            f"🥈 *Silver*: {silver['total']} classified\n"
            f"  Genuine DE: {silver['genuine']}\n"
            f"  Avg score: {silver['avg_score'] or 0}%\n\n"
            f"🥇 *Applications*:\n{app_lines}\n\n"
            f"🔄 *Recent runs*:\n{run_lines}"
        ))

    def _cmd_status(self, chat_id: str):
        """Quick pipeline health check."""
        from src.quality.expectations import QualityValidator
        validator = QualityValidator(self.db)

        try:
            all_passed, results = validator.run_full_suite()
            icon = "✅" if all_passed else "⚠️"

            lines = [f"{icon} *Pipeline Health*\n"]
            for report in results:
                layer_icon = "✅" if report["passed"] else "❌"
                lines.append(f"\n{layer_icon} *{report['layer'].title()} layer*")
                for check in report["checks"]:
                    c_icon = "✓" if check["passed"] else "✗"
                    lines.append(f"  {c_icon} {check['name']}")

            self._send_to(chat_id, "\n".join(lines))
        except Exception as e:
            self._send_to(chat_id, f"❌ Health check failed: {str(e)[:200]}")

    # ── Helpers ────────────────────────────────────────────

    def _send_to(self, chat_id: str, text: str):
        """Send a message to a specific chat."""
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        try:
            data = json.dumps(payload).encode("utf-8")
            from urllib.request import Request, urlopen
            req = Request(
                f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
                data=data,
                headers={"Content-Type": "application/json"},
            )
            urlopen(req, timeout=15)
        except Exception as e:
            logger.error(f"Failed to send message: {e}")

    def log_message(self, format, *args):
        """Override to use our logger instead of stderr."""
        logger.info(f"{self.address_string()} - {format % args}")


def register_webhook(bot_token: str, webhook_url: str) -> bool:
    """
    Register the webhook URL with Telegram.
    Call this once when setting up, or on server startup.
    """
    from urllib.request import Request, urlopen
    payload = json.dumps({
        "url": webhook_url,
        "allowed_updates": ["message"],
        "drop_pending_updates": True,
    }).encode("utf-8")

    req = Request(
        f"https://api.telegram.org/bot{bot_token}/setWebhook",
        data=payload,
        headers={"Content-Type": "application/json"},
    )

    try:
        with urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
            if result.get("ok"):
                logger.info(f"Webhook registered: {webhook_url}")
                return True
            else:
                logger.error(f"Webhook registration failed: {result}")
                return False
    except Exception as e:
        logger.error(f"Webhook registration error: {e}")
        return False


def run_server(host: str = "0.0.0.0", port: int = 8443):
    """
    Start the webhook server.

    Usage:
        python -m src.webhook.telegram_webhook_server

    For development, use ngrok to expose locally:
        ngrok http 8443
        Then register: python -m src.webhook.telegram_webhook_server --register https://xxxx.ngrok.io/webhook
    """
    config = PipelineConfig.from_yaml()
    db = Database(config.database)

    tg_cfg = config.notifications.get("telegram", {})
    bot_token = tg_cfg.get("bot_token", "")
    chat_id = tg_cfg.get("chat_id", "")

    if not bot_token:
        logger.error("No Telegram bot_token in config. Cannot start webhook server.")
        sys.exit(1)

    notifier = TelegramNotifier(bot_token, chat_id)

    # Inject dependencies into handler class
    WebhookHandler.config = config
    WebhookHandler.db = db
    WebhookHandler.notifier = notifier
    WebhookHandler.bot_token = bot_token
    WebhookHandler.allowed_chat_ids = {chat_id} if chat_id else set()

    # Check for --register flag
    if len(sys.argv) > 2 and sys.argv[1] == "--register":
        webhook_url = sys.argv[2]
        if not webhook_url.endswith("/webhook"):
            webhook_url = webhook_url.rstrip("/") + "/webhook"
        register_webhook(bot_token, webhook_url)

    server = HTTPServer((host, port), WebhookHandler)
    logger.info(f"Webhook server running on {host}:{port}")
    logger.info(f"Authorised chat_id: {chat_id}")
    logger.info("Waiting for Telegram updates...")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down webhook server.")
        server.server_close()


if __name__ == "__main__":
    run_server()
