"""持久化的低频分红数据服务。

``DividendService`` 将 AKShare/CNINFO 请求限制在显式刷新操作中；实时监测
只从 SQLite 读取缓存，因此慢数据或其运行时依赖不会进入行情快路径。
"""

import logging
import sqlite3
from threading import RLock
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from app.market.dividend import DividendProvider
from app.market.dividend_repository import DividendRepository

logger = logging.getLogger(__name__)

_ETF_PREFIXES = ("5", "1", "58", "16")
_BENCHMARK_KEY = "__etf_benchmark__"


class DividendService:
    """SQLite-backed dividend cache with explicit, failure-isolated refreshes."""

    STOCK_TTL = timedelta(hours=24)
    ETF_TTL = timedelta(hours=2)

    def __init__(self, data_dir: str | Path = "data", provider=None):
        self._db_path = Path(data_dir) / "market_cache.db"
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._provider = provider or DividendProvider()
        self._lock = RLock()
        self._init_db()
        self.repository = DividendRepository(self._db_path)
        logger.info("Dividend cache loaded: %s", self._db_path)

    def get_yield(self, symbol: str, price: float) -> float:
        """Return a cached yield only; never performs network I/O."""
        with self._lock:
            if price <= 0:
                return 0.0
            if self._is_etf(symbol):
                cached, fresh = self._get_cached(_BENCHMARK_KEY, self.ETF_TTL)
                if cached is None:
                    logger.warning("ETF dividend cache missing; %s uses 0", symbol)
                    return 0.0
                if not fresh:
                    logger.warning("ETF dividend cache is stale; retaining last value for %s", symbol)
                return cached
            dividend, fresh = self._get_cached(symbol, self.STOCK_TTL)
            if dividend is None:
                logger.warning("Dividend cache missing; %s uses 0", symbol)
                return 0.0
            if not fresh:
                logger.warning("Dividend cache is stale; retaining last value for %s", symbol)
            return round(dividend / price * 100, 2) if dividend > 0 else 0.0

    def get_yields(self, symbols: Iterable[str], price_map: dict[str, float]) -> dict[str, float]:
        """Batch read cached yields for one monitor snapshot."""
        return {symbol: self.get_yield(symbol, float(price_map.get(symbol, 0) or 0))
                for symbol in symbols}

    def refresh(self, symbols: Iterable[str]) -> dict[str, int]:
        """Refresh slow data explicitly. Individual provider failures are isolated."""
        symbols = list(dict.fromkeys(str(symbol) for symbol in symbols))
        refreshed = failed = 0
        with self._lock:
            if any(self._is_etf(symbol) for symbol in symbols):
                try:
                    value = self._provider.get_etf_benchmark_yield()
                    self._raise_provider_error()
                    self._put(_BENCHMARK_KEY, value, "akshare")
                    refreshed += 1
                except Exception as exc:
                    failed += 1
                    logger.warning("ETF dividend benchmark refresh failed; retaining cache: %s", exc)
            for symbol in symbols:
                if self._is_etf(symbol):
                    continue
                try:
                    value = self._provider.get_stock_dividend(symbol)
                    self._raise_provider_error()
                    self._put(symbol, value, "cninfo")
                    refreshed += 1
                except Exception as exc:
                    failed += 1
                    logger.warning("Dividend refresh failed for %s; retaining cache: %s", symbol, exc)
        logger.info("Dividend refresh completed: refreshed=%s failed=%s", refreshed, failed)
        return {"refreshed": refreshed, "failed": failed}

    def _init_db(self) -> None:
        with sqlite3.connect(self._db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dividend_cache (
                    symbol TEXT PRIMARY KEY,
                    dividend_per_share REAL NOT NULL,
                    source TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

    def _get_cached(self, symbol: str, ttl: timedelta) -> tuple[float | None, bool]:
        with sqlite3.connect(self._db_path) as conn:
            row = conn.execute(
                "SELECT dividend_per_share, updated_at FROM dividend_cache WHERE symbol=?", (symbol,)
            ).fetchone()
        if not row:
            return None, False
        try:
            updated_at = datetime.fromisoformat(row[1])
        except (TypeError, ValueError):
            return None, False
        return float(row[0]), datetime.now() - updated_at <= ttl

    def _put(self, symbol: str, value: float, source: str) -> None:
        with self._lock, sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """INSERT INTO dividend_cache(symbol, dividend_per_share, source, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(symbol) DO UPDATE SET dividend_per_share=excluded.dividend_per_share,
                       source=excluded.source, updated_at=excluded.updated_at""",
                (symbol, float(value or 0), source, datetime.now().isoformat()),
            )

    def _raise_provider_error(self) -> None:
        error = getattr(self._provider, "last_error", None)
        if error:
            raise RuntimeError(error)

    @staticmethod
    def _is_etf(symbol: str) -> bool:
        return str(symbol).startswith(_ETF_PREFIXES)
