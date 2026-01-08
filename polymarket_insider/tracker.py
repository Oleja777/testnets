import csv
import json
import threading
import time
from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from . import config


@dataclass
class TradeRecord:
    timestamp: datetime
    price: float
    size: float
    maker: Optional[str]
    taker: Optional[str]


@dataclass
class MarketSignal:
    market_id: str
    market_name: str
    timestamp: datetime
    volume_15m: float
    trades_15m: int
    price_change_pct: float
    top_addresses: List[Tuple[str, int]]
    anomaly_volume_ratio: float
    anomaly_trades_ratio: float

    def to_row(self) -> Dict[str, Any]:
        return {
            "market_id": self.market_id,
            "market": self.market_name,
            "timestamp": self.timestamp.isoformat(),
            "vol_15m": f"{self.volume_15m:.4f}",
            "trades_15m": self.trades_15m,
            "price_change_pct": f"{self.price_change_pct:.2f}",
            "top_addresses": ";".join(
                f"{address}:{count}" for address, count in self.top_addresses
            ),
            "anomaly_volume_ratio": f"{self.anomaly_volume_ratio:.2f}",
            "anomaly_trades_ratio": f"{self.anomaly_trades_ratio:.2f}",
        }


class PolymarketClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        query = f"?{urlencode(params)}" if params else ""
        url = f"{self.base_url}{path}{query}"
        req = Request(url, headers={"User-Agent": "polymarket-insider-tracker"})
        with urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))

    def get_active_markets(self) -> List[Dict[str, Any]]:
        data = self._get("/markets", {"active": "true"})
        return data.get("markets", data.get("data", data))

    def get_recent_trades(self, market_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        data = self._get("/trades", {"market": market_id, "limit": str(limit)})
        return data.get("trades", data.get("data", data))


class SignalStore:
    def __init__(self, csv_path: str) -> None:
        self.csv_path = Path(csv_path)
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.signals: Deque[MarketSignal] = deque(maxlen=5000)
        self._load_existing()

    def _load_existing(self) -> None:
        if not self.csv_path.exists():
            return
        with self.csv_path.open("r", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                try:
                    timestamp = datetime.fromisoformat(row["timestamp"])
                except (KeyError, ValueError):
                    continue
                top_addresses = []
                for entry in row.get("top_addresses", "").split(";"):
                    if not entry:
                        continue
                    if ":" not in entry:
                        continue
                    address, count = entry.split(":", 1)
                    try:
                        top_addresses.append((address, int(count)))
                    except ValueError:
                        continue
                signal = MarketSignal(
                    market_id=row.get("market_id", ""),
                    market_name=row.get("market", ""),
                    timestamp=timestamp,
                    volume_15m=float(row.get("vol_15m", 0)),
                    trades_15m=int(float(row.get("trades_15m", 0))),
                    price_change_pct=float(row.get("price_change_pct", 0)),
                    top_addresses=top_addresses,
                    anomaly_volume_ratio=float(row.get("anomaly_volume_ratio", 0)),
                    anomaly_trades_ratio=float(row.get("anomaly_trades_ratio", 0)),
                )
                self.signals.append(signal)

    def append(self, signal: MarketSignal) -> None:
        file_exists = self.csv_path.exists()
        with self.csv_path.open("a", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=list(signal.to_row().keys()),
            )
            if not file_exists:
                writer.writeheader()
            writer.writerow(signal.to_row())
        self.signals.append(signal)

    def list_signals(self) -> List[MarketSignal]:
        return list(self.signals)


class MarketTracker:
    def __init__(self, client: PolymarketClient, store: SignalStore) -> None:
        self.client = client
        self.store = store
        self.market_trades: Dict[str, Deque[TradeRecord]] = {}
        self.last_seen_ids: Dict[str, set] = {}
        self._lock = threading.Lock()

    def _parse_timestamp(self, raw: Any) -> Optional[datetime]:
        if raw is None:
            return None
        if isinstance(raw, (int, float)):
            ts = float(raw)
            if ts > 1e12:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        if isinstance(raw, str):
            try:
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    def _extract_float(self, trade: Dict[str, Any], keys: Iterable[str]) -> Optional[float]:
        for key in keys:
            value = trade.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    def _normalize_trade(self, trade: Dict[str, Any]) -> Optional[TradeRecord]:
        timestamp = self._parse_timestamp(trade.get("timestamp") or trade.get("time"))
        if not timestamp:
            return None
        price = self._extract_float(trade, ["price", "avg_price", "rate"])
        size = self._extract_float(trade, ["size", "amount", "quantity"])
        if price is None or size is None:
            return None
        maker = trade.get("maker") or trade.get("maker_address")
        taker = trade.get("taker") or trade.get("taker_address")
        return TradeRecord(timestamp=timestamp, price=price, size=size, maker=maker, taker=taker)

    def _purge_old(self, trades: Deque[TradeRecord]) -> None:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=24)
        while trades and trades[0].timestamp < cutoff:
            trades.popleft()

    def _compute_signal(
        self, market_id: str, market_name: str, trades: Deque[TradeRecord]
    ) -> Optional[MarketSignal]:
        now = datetime.now(tz=timezone.utc)
        window_15m = now - timedelta(minutes=15)
        last_24h = [trade for trade in trades if trade.timestamp >= now - timedelta(hours=24)]
        last_15m = [trade for trade in trades if trade.timestamp >= window_15m]
        if not last_15m:
            return None

        volume_15m = sum(trade.size for trade in last_15m)
        trades_15m = len(last_15m)
        total_volume_24h = sum(trade.size for trade in last_24h)
        total_trades_24h = len(last_24h)
        avg_volume_15m = total_volume_24h / 96 if total_volume_24h > 0 else 0
        avg_trades_15m = total_trades_24h / 96 if total_trades_24h > 0 else 0
        anomaly_volume_ratio = volume_15m / avg_volume_15m if avg_volume_15m else 0
        anomaly_trades_ratio = trades_15m / avg_trades_15m if avg_trades_15m else 0

        if (
            anomaly_volume_ratio < config.ANOMALY_VOLUME_THRESHOLD
            and anomaly_trades_ratio < config.ANOMALY_TRADES_THRESHOLD
        ):
            return None

        first_price = last_15m[0].price
        last_price = last_15m[-1].price
        price_change_pct = ((last_price - first_price) / first_price * 100) if first_price else 0

        address_counter: Counter[str] = Counter()
        for trade in last_15m:
            if trade.maker:
                address_counter[trade.maker] += 1
            if trade.taker:
                address_counter[trade.taker] += 1
        top_addresses = address_counter.most_common(config.TOP_ADDRESS_COUNT)

        return MarketSignal(
            market_id=market_id,
            market_name=market_name,
            timestamp=now,
            volume_15m=volume_15m,
            trades_15m=trades_15m,
            price_change_pct=price_change_pct,
            top_addresses=top_addresses,
            anomaly_volume_ratio=anomaly_volume_ratio,
            anomaly_trades_ratio=anomaly_trades_ratio,
        )

    def poll_once(self) -> List[MarketSignal]:
        signals: List[MarketSignal] = []
        markets = self.client.get_active_markets()
        for market in markets:
            market_id = str(market.get("id") or market.get("market_id"))
            if not market_id:
                continue
            market_name = (
                market.get("question")
                or market.get("name")
                or market.get("market_title")
                or market_id
            )
            trades_raw = self.client.get_recent_trades(market_id)
            trades = []
            for trade_raw in trades_raw:
                trade_id = trade_raw.get("id") or trade_raw.get("trade_id")
                if trade_id is not None:
                    seen = self.last_seen_ids.setdefault(market_id, set())
                    if trade_id in seen:
                        continue
                    seen.add(trade_id)
                trade = self._normalize_trade(trade_raw)
                if trade:
                    trades.append(trade)
            if not trades:
                continue
            trades.sort(key=lambda t: t.timestamp)
            with self._lock:
                trade_deque = self.market_trades.setdefault(market_id, deque())
                trade_deque.extend(trades)
                self._purge_old(trade_deque)
                signal = self._compute_signal(market_id, market_name, trade_deque)
            if signal:
                self.store.append(signal)
                signals.append(signal)
        return signals


class TrackerService:
    def __init__(self, tracker: MarketTracker, on_signals=None) -> None:
        self.tracker = tracker
        self.on_signals = on_signals
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                signals = self.tracker.poll_once()
                if self.on_signals and signals:
                    self.on_signals(signals)
            except Exception:
                time.sleep(config.POLL_INTERVAL_SEC)
                continue
            time.sleep(config.POLL_INTERVAL_SEC)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
