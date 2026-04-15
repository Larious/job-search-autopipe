#!/usr/bin/env bash
# ============================================================
# Telegram Webhook Setup
# ============================================================
# Registers the webhook URL with Telegram so incoming messages
# are pushed to your server instead of polled.
#
# For development:
#   1. Install ngrok: https://ngrok.com
#   2. Run: ngrok http 8443
#   3. Run: ./scripts/setup_webhook.sh https://xxxx.ngrok.io
#
# For production:
#   ./scripts/setup_webhook.sh https://your-domain.com
# ============================================================

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: ./scripts/setup_webhook.sh <PUBLIC_URL>"
    echo ""
    echo "Examples:"
    echo "  ./scripts/setup_webhook.sh https://abc123.ngrok.io"
    echo "  ./scripts/setup_webhook.sh https://autopipe.yourdomain.com"
    exit 1
fi

PUBLIC_URL="${1%/}/webhook"

# Read bot token from config
BOT_TOKEN=$(python3 -c "
import yaml
with open('config/config.yaml') as f:
    c = yaml.safe_load(f)
print(c['notifications']['telegram']['bot_token'])
" 2>/dev/null)

if [ -z "$BOT_TOKEN" ] || [ "$BOT_TOKEN" = "YOUR_TELEGRAM_BOT_TOKEN" ]; then
    echo "❌ No valid bot_token found in config/config.yaml"
    echo "   Set notifications.telegram.bot_token first."
    exit 1
fi

echo "📡 Registering webhook..."
echo "   URL: $PUBLIC_URL"
echo ""

RESULT=$(curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/setWebhook" \
    -H "Content-Type: application/json" \
    -d "{\"url\": \"${PUBLIC_URL}\", \"allowed_updates\": [\"message\"], \"drop_pending_updates\": true}")

OK=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('ok', False))" 2>/dev/null)

if [ "$OK" = "True" ]; then
    echo "✅ Webhook registered successfully!"
    echo ""

    # Verify
    echo "📋 Webhook info:"
    curl -s "https://api.telegram.org/bot${BOT_TOKEN}/getWebhookInfo" | python3 -m json.tool 2>/dev/null
    echo ""
    echo "🚀 Start the webhook server:"
    echo "   docker-compose up -d telegram-webhook"
    echo ""
    echo "🧪 Test by sending /help to your bot on Telegram"
else
    echo "❌ Registration failed:"
    echo "$RESULT" | python3 -m json.tool 2>/dev/null || echo "$RESULT"
fi
