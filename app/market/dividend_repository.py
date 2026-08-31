"""Local persistence boundary for live and historical dividend data."""

import sqlite3
from datetime import date, datetime
from pathlib import Path
from threading import RLock


class DividendRepository:
    """SQLite repository; reads never perform network I/O."""

    def __init__(self, db_path: str | Path):
        self._db_path = Path(db_path)
        self._lock = RLock()
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self._db_path)

    def _init_db(self) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS dividend_history (
                symbol TEXT NOT NULL, ex_date TEXT NOT NULL, dividend_per_share REAL NOT NULL,
                source TEXT NOT NULL, updated_at TEXT NOT NULL,
                PRIMARY KEY(symbol, ex_date))""")
            conn.execute("""CREATE TABLE IF NOT EXISTS etf_benchmark_history (
                ex_date TEXT PRIMARY KEY, annual_dividend REAL NOT NULL,
                benchmark_price REAL, source TEXT NOT NULL, updated_at TEXT NOT NULL)""")

    def store_history(self, symbol: str, rows: list[tuple[date | str, float]], source: str = "manual") -> None:
        now = datetime.now().isoformat()
        values = [(symbol, self._date_string(ex_date), float(dividend), source, now)
                  for ex_date, dividend in rows]
        if not values:
            return
        with self._lock, self._connect() as conn:
            conn.executemany("""INSERT INTO dividend_history VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(symbol, ex_date) DO UPDATE SET dividend_per_share=excluded.dividend_per_share,
                source=excluded.source, updated_at=excluded.updated_at""", values)

    def store_etf_benchmark_history(self, rows: list[tuple[date | str, float, float | None]], source: str = "manual") -> None:
        now = datetime.now().isoformat()
        values = [(self._date_string(ex_date), float(dividend), price, source, now)
                  for ex_date, dividend, price in rows]
        if not values:
            return
        with self._lock, self._connect() as conn:
            conn.executemany("""INSERT INTO etf_benchmark_history VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(ex_date) DO UPDATE SET annual_dividend=excluded.annual_dividend,
                benchmark_price=excluded.benchmark_price, source=excluded.source, updated_at=excluded.updated_at""", values)

    def backtest_data(self, symbols: list[str]) -> dict:
        with self._lock:
            data = {"etf_dividends": [], "etf_prices": {}}
            stock_symbols = [symbol for symbol in symbols if not self._is_etf(symbol)]
            for symbol in stock_symbols:
                with self._connect() as conn:
                    rows = conn.execute("SELECT ex_date, dividend_per_share FROM dividend_history WHERE symbol=? ORDER BY ex_date", (symbol,)).fetchall()
                data[symbol] = [(datetime.fromisoformat(ex_date).date(), float(dividend)) for ex_date, dividend in rows]
            with self._connect() as conn:
                rows = conn.execute("SELECT ex_date, annual_dividend, benchmark_price FROM etf_benchmark_history ORDER BY ex_date").fetchall()
            data["etf_dividends"] = [(datetime.fromisoformat(ex_date).date(), float(dividend)) for ex_date, dividend, _ in rows]
            data["etf_prices"] = {ex_date: float(price) for ex_date, _, price in rows if price and price > 0}
            return data

    @staticmethod
    def _date_string(value: date | str) -> str:
        return value.isoformat() if hasattr(value, "isoformat") else str(value)[:10]

    @staticmethod
    def _is_etf(symbol: str) -> bool:
        return str(symbol).startswith(("5", "1", "58", "16"))
