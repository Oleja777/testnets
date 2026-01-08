import os


def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).lower() in {"1", "true", "yes", "on"}


POLYMARKET_BASE_URL = os.getenv("POLYMARKET_BASE_URL", "https://clob.polymarket.com")
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "60"))
ANOMALY_VOLUME_THRESHOLD = float(os.getenv("ANOMALY_VOLUME_THRESHOLD", "3.0"))
ANOMALY_TRADES_THRESHOLD = float(os.getenv("ANOMALY_TRADES_THRESHOLD", "3.0"))
SIGNAL_CSV_PATH = os.getenv("SIGNAL_CSV_PATH", "polymarket_insider/data/signals.csv")
TOP_ADDRESS_COUNT = int(os.getenv("TOP_ADDRESS_COUNT", "5"))

TELEGRAM_ENABLED = _env_bool("TELEGRAM_ENABLED", "false")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
