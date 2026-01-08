import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from . import config
from .tracker import MarketSignal, MarketTracker, PolymarketClient, SignalStore, TrackerService


class TelegramNotifier:
    def __init__(self) -> None:
        self.enabled = config.TELEGRAM_ENABLED
        self.token = config.TELEGRAM_BOT_TOKEN
        self.chat_id = config.TELEGRAM_CHAT_ID

    def send(self, signal: MarketSignal) -> None:
        if not self.enabled or not self.token or not self.chat_id:
            return
        message = (
            f"🚨 Polymarket anomaly\n"
            f"Market: {signal.market_name}\n"
            f"Time: {signal.timestamp.isoformat()}\n"
            f"Vol 15m: {signal.volume_15m:.4f}\n"
            f"Trades 15m: {signal.trades_15m}\n"
            f"Price Δ: {signal.price_change_pct:.2f}%\n"
            f"Top addresses: {', '.join(addr for addr, _ in signal.top_addresses)}"
        )
        payload = {"chat_id": self.chat_id, "text": message}
        url = f"https://api.telegram.org/bot{self.token}/sendMessage?{urlencode(payload)}"
        req = Request(url, headers={"User-Agent": "polymarket-insider-tracker"})
        with urlopen(req, timeout=20):
            return


store = SignalStore(config.SIGNAL_CSV_PATH)
client = PolymarketClient(config.POLYMARKET_BASE_URL)
tracker = MarketTracker(client, store)
notifier = TelegramNotifier()


def _notify(signals: List[MarketSignal]) -> None:
    for signal in signals:
        notifier.send(signal)


service = TrackerService(tracker, on_signals=_notify)


def _serialize_signals(signals: List[MarketSignal]) -> List[Dict[str, Any]]:
    data: List[Dict[str, Any]] = []
    for signal in signals:
        data.append(
            {
                "market_id": signal.market_id,
                "market": signal.market_name,
                "timestamp": signal.timestamp.isoformat(),
                "vol_15m": signal.volume_15m,
                "trades_15m": signal.trades_15m,
                "price_change_pct": signal.price_change_pct,
                "top_addresses": [
                    {"address": addr, "count": count}
                    for addr, count in signal.top_addresses
                ],
                "anomaly_volume_ratio": signal.anomaly_volume_ratio,
                "anomaly_trades_ratio": signal.anomaly_trades_ratio,
            }
        )
    return data


def _load_index_html() -> bytes:
    template_path = Path(__file__).parent / "templates" / "index.html"
    return template_path.read_bytes()


class RequestHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload: Any, status_code: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(self, body: bytes, content_type: str = "text/html") -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_bytes(_load_index_html())
            return
        if parsed.path == "/api/signals":
            signals = _serialize_signals(store.list_signals())
            self._send_json(signals)
            return
        if parsed.path == "/api/health":
            self._send_json({"status": "ok", "timestamp": datetime.now(tz=timezone.utc).isoformat()})
            return
        self.send_response(404)
        self.end_headers()


def run() -> None:
    if os.getenv("SKIP_TRACKER") != "1":
        service.start()
    server = ThreadingHTTPServer(("0.0.0.0", int(os.getenv("PORT", "8000"))), RequestHandler)
    server.serve_forever()


if __name__ == "__main__":
    run()
