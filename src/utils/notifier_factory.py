"""
Notification factory for Job Search AutoPipe.

Returns the appropriate notifier(s) based on the preferred_channel
setting in config.yaml. Supports Slack, Telegram, or both.
"""

import logging
from typing import Protocol, Optional

logger = logging.getLogger(__name__)


class Notifier(Protocol):
    """Interface that all notifiers implement."""
    def send_message(self, text: str) -> None: ...
    def send_pipeline_alert(self, phase: str, status: str,
                            records_in: int, records_out: int,
                            error: Optional[str]) -> None: ...
    def send_daily_digest(self, jobs: list, digest_date: Optional[str]) -> None: ...


class MultiNotifier:
    """Sends to multiple channels at once."""

    def __init__(self, notifiers: list):
        self.notifiers = notifiers

    def send_message(self, text: str):
        for n in self.notifiers:
            try:
                n.send_message(text)
            except Exception as e:
                logger.error(f"Notifier {n.__class__.__name__} failed: {e}")

    def send_pipeline_alert(self, phase: str, status: str,
                            records_in: int = 0, records_out: int = 0,
                            error: Optional[str] = None):
        for n in self.notifiers:
            try:
                n.send_pipeline_alert(phase, status, records_in, records_out, error)
            except Exception as e:
                logger.error(f"Notifier {n.__class__.__name__} failed: {e}")

    def send_daily_digest(self, jobs: list, digest_date: Optional[str] = None):
        for n in self.notifiers:
            try:
                n.send_daily_digest(jobs, digest_date)
            except Exception as e:
                logger.error(f"Notifier {n.__class__.__name__} failed: {e}")


def create_notifier(notifications_config: dict) -> Optional[MultiNotifier]:
    """
    Factory function that builds the right notifier(s) from config.

    Config example:
        notifications:
          preferred_channel: "telegram"  # or "slack" or "both"
          slack:
            webhook_url: "https://hooks.slack.com/..."
            channel: "#job-alerts"
          telegram:
            bot_token: "123456:ABC..."
            chat_id: "-100123456789"
    """
    preferred = notifications_config.get("preferred_channel", "slack")
    notifiers = []

    channels_to_init = []
    if preferred == "both":
        channels_to_init = ["slack", "telegram"]
    elif preferred in ("slack", "telegram"):
        channels_to_init = [preferred]
    else:
        logger.warning(f"Unknown preferred_channel '{preferred}', defaulting to slack")
        channels_to_init = ["slack"]

    for channel in channels_to_init:
        if channel == "slack":
            slack_cfg = notifications_config.get("slack", {})
            webhook = slack_cfg.get("webhook_url", "")
            if webhook and not webhook.startswith("YOUR_"):
                from .slack_notifier import SlackNotifier
                notifiers.append(SlackNotifier(webhook, slack_cfg.get("channel", "#job-alerts")))
                logger.info("Slack notifier initialized")
            else:
                logger.warning("Slack configured but no valid webhook_url found")

        elif channel == "telegram":
            tg_cfg = notifications_config.get("telegram", {})
            token = tg_cfg.get("bot_token", "")
            chat_id = tg_cfg.get("chat_id", "")
            if token and chat_id and not token.startswith("YOUR_"):
                from .telegram_notifier import TelegramNotifier
                notifiers.append(TelegramNotifier(token, chat_id))
                logger.info("Telegram notifier initialized")
            else:
                logger.warning("Telegram configured but missing bot_token or chat_id")

    if not notifiers:
        logger.warning("No notification channels configured. Digest will only be logged.")
        return None

    return MultiNotifier(notifiers)
