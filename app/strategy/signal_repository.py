"""SQLite repository for signal logs and reviews."""

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock


class SignalRepository:
    def __init__(self, db_path: str | Path):
        self._db_path = Path(db_path)
        self._lock = RLock()
        self._init_db()

    @contextmanager
    def _conn(self):
        with self._lock:
            conn = sqlite3.connect(str(self._db_path))
            try:
                yield conn
            finally:
                conn.close()

    def _init_db(self):
        with self._conn() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS signal_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
                strategy_id TEXT NOT NULL, strategy_name TEXT, symbol TEXT NOT NULL,
                action TEXT NOT NULL, price REAL, reason TEXT, strength REAL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS signal_review (
                signal_id INTEGER, review_date TEXT, signal_price REAL, review_price REAL,
                pnl_pct REAL, correct INTEGER, PRIMARY KEY (signal_id, review_date))""")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_log_today ON signal_log(timestamp, strategy_id, symbol, action)")
            conn.commit()

    def today_keys(self, today: str | None = None) -> set[tuple[str, str, str]]:
        today = today or datetime.now().strftime("%Y-%m-%d")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT strategy_id, symbol, action FROM signal_log WHERE timestamp >= ?", (today,)
            ).fetchall()
        return {(row[0], row[1], row[2]) for row in rows}

    def recent_signal_ticks(self, days: int = 366) -> dict[tuple[str, str, str], int]:
        """Return the latest calendar-day ordinal for each signal key."""
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT strategy_id, symbol, action, MAX(timestamp) FROM signal_log
                   WHERE timestamp >= ? GROUP BY strategy_id, symbol, action""", (cutoff,)
            ).fetchall()
        result = {}
        for strategy_id, symbol, action, timestamp in rows:
            try:
                result[(strategy_id, symbol, action)] = datetime.fromisoformat(timestamp).date().toordinal()
            except (TypeError, ValueError):
                continue
        return result

    def log_batch(self, records: list[dict]) -> None:
        if not records:
            return
        now = datetime.now().isoformat()
        rows = [(record.get("timestamp", now), record["strategy_id"], record["strategy_name"],
                 record["symbol"], record["action"], record.get("price", 0),
                 record.get("reason", ""), record.get("strength", 0)) for record in records]
        with self._conn() as conn:
            conn.executemany("""INSERT INTO signal_log
                (timestamp, strategy_id, strategy_name, symbol, action, price, reason, strength)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", rows)
            conn.commit()

    def list_recent(self, limit: int = 50) -> list[dict]:
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM signal_log ORDER BY timestamp DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in rows]

    def review_signals(self, price_map: dict[str, float], days_list: list[int] | None = None) -> list[dict]:
        days_list = days_list or [3, 7, 21]
        today = datetime.now().strftime("%Y-%m-%d")
        results = []
        with self._conn() as conn:
            for days in days_list:
                target_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
                rows = conn.execute(
                    """SELECT s.id, s.symbol, s.action, s.price, s.strategy_name, s.timestamp, s.reason
                       FROM signal_log s LEFT JOIN signal_review r
                       ON s.id = r.signal_id AND r.review_date = ?
                       WHERE date(s.timestamp) = ? AND r.signal_id IS NULL ORDER BY s.timestamp""",
                    (today, target_date),
                ).fetchall()
                for sig_id, symbol, action, signal_price, strategy_name, timestamp, reason in rows:
                    current_price = price_map.get(symbol, 0)
                    if current_price <= 0 or signal_price <= 0:
                        continue
                    if action == "buy":
                        pnl_pct = round((current_price / signal_price - 1) * 100, 2)
                        correct = int(current_price > signal_price)
                    else:
                        pnl_pct = round((signal_price / current_price - 1) * 100, 2)
                        correct = int(current_price < signal_price)
                    conn.execute("INSERT OR REPLACE INTO signal_review VALUES (?, ?, ?, ?, ?, ?)",
                                 (sig_id, today, signal_price, current_price, pnl_pct, correct))
                    results.append({
                        "signal": {"id": sig_id, "symbol": symbol, "action": action,
                                   "price": signal_price, "strategy_name": strategy_name,
                                   "timestamp": timestamp, "reason": reason},
                        "review": {"days": days, "pnl_pct": pnl_pct, "correct": bool(correct),
                                   "review_price": current_price},
                    })
            conn.commit()
        return results

    def signal_stats(self) -> dict:
        with self._conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM signal_log").fetchone()[0]
            reviewed, correct = conn.execute("SELECT COUNT(*), SUM(correct) FROM signal_review").fetchone()
        return {"total_signals": total, "reviewed": reviewed, "correct": correct or 0,
                "accuracy": round((correct or 0) / reviewed * 100, 1) if reviewed else 0}
