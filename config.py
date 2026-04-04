import os
from dotenv import load_dotenv

load_dotenv()

# токен берём из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

_database_url = os.getenv("DATABASE_URL", "")
if _database_url:
    POSTGRES_DSN = _database_url.replace("postgres://", "postgresql://", 1)
    DB_TYPE = "postgres"
else:
    POSTGRES_DSN = os.getenv("POSTGRES_DSN", "")
    DB_TYPE = os.getenv("DB_TYPE", "sqlite")

BOT_MODE = os.getenv("BOT_MODE", "polling")
WEBHOOK_HOST = os.getenv("RAILWAY_PUBLIC_DOMAIN", os.getenv("WEBHOOK_HOST", ""))
if WEBHOOK_HOST and not WEBHOOK_HOST.startswith("https://"):
    WEBHOOK_HOST = f"https://{WEBHOOK_HOST}"
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_PORT = int(os.getenv("PORT", os.getenv("WEBHOOK_PORT", "8443")))

WEB_PANEL_ENABLED = os.getenv("WEB_PANEL_ENABLED", "false").lower() == "true"
WEB_PANEL_PORT = int(os.getenv("WEB_PANEL_PORT", "8080"))
WEB_PANEL_PASSWORD = os.getenv("WEB_PANEL_PASSWORD", "changeme")

# за сколько мест до первого предупреждать
NOTIFY_APPROACHING = int(os.getenv("NOTIFY_APPROACHING", "3"))
