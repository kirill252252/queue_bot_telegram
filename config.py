import os
from dotenv import load_dotenv

load_dotenv()

# ── Telegram бот ──────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# ── База данных ───────────────────────────────────────────────────────────────
_database_url = os.getenv("DATABASE_URL", "")
if _database_url:
    POSTGRES_DSN = _database_url.replace("postgres://", "postgresql://", 1)
    DB_TYPE = "postgres"
else:
    POSTGRES_DSN = os.getenv("POSTGRES_DSN", "")
    DB_TYPE = os.getenv("DB_TYPE", "sqlite")

# ── Webhook / Polling ─────────────────────────────────────────────────────────
BOT_MODE = os.getenv("BOT_MODE", "polling")
WEBHOOK_HOST = os.getenv("RAILWAY_PUBLIC_DOMAIN", os.getenv("WEBHOOK_HOST", ""))
if WEBHOOK_HOST and not WEBHOOK_HOST.startswith("https://"):
    WEBHOOK_HOST = f"https://{WEBHOOK_HOST}"
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_PORT = int(os.getenv("PORT", os.getenv("WEBHOOK_PORT", "8443")))

# ── Веб-панель ────────────────────────────────────────────────────────────────
WEB_PANEL_ENABLED  = os.getenv("WEB_PANEL_ENABLED", "false").lower() == "true"
WEB_PANEL_PORT     = int(os.getenv("WEB_PANEL_PORT", "8080"))
WEB_PANEL_PASSWORD = os.getenv("WEB_PANEL_PASSWORD", "changeme")

# ── Groq AI (OCR расписания и парсинг изменений) ──────────────────────────────
# Получить ключ: https://console.groq.com/keys
# Добавить в .env: GROQ_API_KEY=gsk_ваш_ключ
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

# Модели можно переопределить в .env
# Vision (изображения расписания):
GROQ_VISION_MODEL = os.getenv("GROQ_VISION_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")
# Text (парсинг текстовых сообщений об изменениях):
GROQ_TEXT_MODEL   = os.getenv("GROQ_TEXT_MODEL", "llama-3.3-70b-versatile")

if not GROQ_API_KEY:
    import logging as _logging
    _logging.getLogger(__name__).warning(
        "GROQ_API_KEY не задан — AI-функции расписания отключены. "
        "Получите ключ на https://console.groq.com/keys и добавьте в .env"
    )

# ── VK мониторинг ─────────────────────────────────────────────────────────────
# Токен VK API для мониторинга стены группы (опционально)
# Получить: https://vk.com/dev/access_token (user token или service token)
VK_TOKEN = os.getenv("VK_TOKEN", "")

# ── Уведомления ───────────────────────────────────────────────────────────────
# За сколько мест до первого предупреждать участника
NOTIFY_APPROACHING = int(os.getenv("NOTIFY_APPROACHING", "3"))

# ── Администрирование ─────────────────────────────────────────────────────────
# User ID владельца бота — только он может назначать бот-админов
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID", "0"))

# ── Часовой пояс ─────────────────────────────────────────────────────────────
TZ_OFFSET = int(os.getenv("TZ_OFFSET", "0"))
