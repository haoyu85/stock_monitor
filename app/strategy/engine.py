"""
策略引擎模块。

负责策略的加载、存储、管理和安全执行。
使用受限环境执行策略代码，避免安全风险。
"""

import hashlib
import logging
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Optional

import pandas as pd

from app.strategy.llm import _safe_builtins
from app.strategy.signal_repository import SignalRepository

logger = logging.getLogger(__name__)


class StrategyRecord:
    """策略记录数据类。"""

    def __init__(
        self,
        strategy_id: str,
        name: str,
        description: str,
        code: str,
        enabled: bool = True,
        created_at: str | None = None,
        updated_at: str | None = None,
    ):
        self.id = strategy_id
        self.name = name
        self.description = description
        self.code = code
        self.enabled = enabled
        self.created_at = created_at or datetime.now().isoformat()
        self.updated_at = updated_at or datetime.now().isoformat()

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "enabled": self.enabled,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class StrategyExecutionSnapshot:
    """Compiled policy inputs pinned for one MonitorLoop execution."""

    record: StrategyRecord
    symbol_filter: object | None
    cooldown: tuple[int, int]


class StrategyEngine:
    """策略引擎。

    管理策略的全生命周期：创建、加载、启用/禁用、删除、执行。

    Usage:
        engine = StrategyEngine(config)
        record = engine.create("均线交叉", "当5日线上穿20日线时买入", code)
        result = engine.execute(record.id, context, data)
    """

    def __init__(self, config):
        """
        Args:
            config: 应用配置实例。
        """
        self._config = config
        self._storage_dir = Path(config.system.data_dir) / config.strategy.storage_dir
        self._storage_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = self._storage_dir / "strategies.db"
        self._init_db()
        self.signals = SignalRepository(self._db_path)
        self._cache: dict[str, tuple[str, object, object | None, object | None]] = {}
        # id -> (code_hash, strategy, symbol_filter, cooldown)
        self._record_cache: dict[str, StrategyRecord] = {}  # id -> record
        self._lock = RLock()

    @contextmanager
    def _get_conn(self):
        """获取数据库连接（上下文管理器，确保关闭）。"""
        conn = sqlite3.connect(str(self._db_path))
        try:
            yield conn
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # CRUD 操作
    # ------------------------------------------------------------------

    def create(
        self,
        name: str,
        description: str,
        code: str,
        enabled: bool = True,
    ) -> StrategyRecord:
        """创建并持久化一条策略。

        Args:
            name: 策略名称。
            description: 自然语言描述。
            code: Python 策略函数源代码。
            enabled: 是否立即启用。

        Returns:
            StrategyRecord。
        """
        with self._lock:
            return self._create_unlocked(name, description, code, enabled)

    def _create_unlocked(self, name: str, description: str, code: str, enabled: bool) -> StrategyRecord:
        strategy_id = uuid.uuid4().hex[:12]
        now = datetime.now().isoformat()

        # 保存源代码文件
        code_file = self._storage_dir / f"{strategy_id}.py"
        code_file.write_text(code, encoding="utf-8")

        # 保存到数据库
        with self._get_conn() as conn:
            conn.execute(
                """INSERT INTO strategies (id, name, description, code_path, enabled, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (strategy_id, name, description, str(code_file), int(enabled), now, now),
            )
            conn.commit()

        record = StrategyRecord(strategy_id, name, description, code, enabled, now, now)
        self._record_cache[strategy_id] = record
        logger.info(f"策略已创建: {name} (id={strategy_id})")
        return record

    def update(
        self,
        strategy_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        code: str | None = None,
        enabled: bool | None = None,
    ) -> Optional[StrategyRecord]:
        """Update a strategy in place, preserving its ID and signal history."""
        with self._lock:
            existing = self.get(strategy_id)
            if existing is None:
                return None
            next_name = existing.name if name is None else name
            next_description = existing.description if description is None else description
            next_enabled = existing.enabled if enabled is None else enabled
            next_code = existing.code if code is None else code
            code_path = self._storage_dir / f"{strategy_id}.py"
            # Keep built-in strategies in their existing files unless their code is edited.
            with self._get_conn() as conn:
                row = conn.execute("SELECT code_path FROM strategies WHERE id=?", (strategy_id,)).fetchone()
                if row:
                    code_path = Path(row[0])
                code_path.write_text(next_code, encoding="utf-8")
                now = datetime.now().isoformat()
                conn.execute(
                    """UPDATE strategies SET name=?, description=?, code_path=?, enabled=?, updated_at=?
                       WHERE id=?""",
                    (next_name, next_description, str(code_path), int(next_enabled), now, strategy_id),
                )
                conn.commit()
            self._cache.pop(strategy_id, None)
            record = StrategyRecord(strategy_id, next_name, next_description, next_code,
                                    next_enabled, existing.created_at, now)
            self._record_cache[strategy_id] = record
            logger.info("策略已更新: %s (id=%s)", next_name, strategy_id)
            return record

    def get(self, strategy_id: str) -> Optional[StrategyRecord]:
        """获取指定策略。"""
        with self._lock:
            return self._get_unlocked(strategy_id)

    def _get_unlocked(self, strategy_id: str) -> Optional[StrategyRecord]:
        if strategy_id in self._record_cache:
            return self._record_cache[strategy_id]

        with self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM strategies WHERE id = ?", (strategy_id,)
            ).fetchone()

        if row is None:
            return None

        code_path = Path(row["code_path"])
        code = code_path.read_text(encoding="utf-8") if code_path.exists() else ""

        record = StrategyRecord(
            strategy_id=row["id"],
            name=row["name"],
            description=row["description"],
            code=code,
            enabled=bool(row["enabled"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
        self._record_cache[strategy_id] = record
        return record

    def list_all(self, enabled_only: bool = False) -> list[StrategyRecord]:
        """列出所有策略。"""
        with self._lock, self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            if enabled_only:
                rows = conn.execute(
                    "SELECT * FROM strategies WHERE enabled = 1 ORDER BY updated_at DESC"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM strategies ORDER BY updated_at DESC"
                ).fetchall()

            records = []
            for row in rows:
                code_path = Path(row["code_path"])
                code = code_path.read_text(encoding="utf-8") if code_path.exists() else ""
                records.append(StrategyRecord(
                    strategy_id=row["id"],
                    name=row["name"],
                    description=row["description"],
                    code=code,
                    enabled=bool(row["enabled"]),
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                ))
            return records

    def set_enabled(self, strategy_id: str, enabled: bool) -> bool:
        """启用或禁用策略。"""
        with self._lock, self._get_conn() as conn:
            conn.execute(
                "UPDATE strategies SET enabled = ?, updated_at = ? WHERE id = ?",
                (int(enabled), datetime.now().isoformat(), strategy_id),
            )
            affected = conn.total_changes
            conn.commit()
            self._cache.pop(strategy_id, None)
            self._record_cache.pop(strategy_id, None)
            return affected > 0

    def delete(self, strategy_id: str) -> bool:
        """删除策略。"""
        with self._lock, self._get_conn() as conn:
            row = conn.execute(
                "SELECT code_path FROM strategies WHERE id = ?", (strategy_id,)
            ).fetchone()
            conn.execute("DELETE FROM strategies WHERE id = ?", (strategy_id,))
            conn.commit()

            if row:
                code_path = Path(row[0])
                if code_path.exists():
                    code_path.unlink()

            self._cache.pop(strategy_id, None)
            self._record_cache.pop(strategy_id, None)
            logger.info(f"策略已删除: {strategy_id}")
            return True

    # ------------------------------------------------------------------
    # 策略执行
    # ------------------------------------------------------------------

    def execute(
        self,
        strategy_id: str,
        context: dict,
        data: pd.DataFrame,
    ) -> dict:
        """安全执行指定策略。

        Args:
            strategy_id: 策略ID。
            context: 上下文字典（持仓、资金等）。
            data: 行情 DataFrame。

        Returns:
            策略执行结果 dict，格式 {"action": ..., "reason": ..., "strength": ...}。
        """
        record = self.get(strategy_id)
        return self.execute_record(record, context, data)

    def execute_record(self, record: StrategyRecord | None, context: dict, data: pd.DataFrame) -> dict:
        """Execute a monitor-pinned record without reloading mutable CRUD state."""
        if record is None:
            return {"action": "hold", "reason": "策略不存在", "strength": 0.0}

        if not record.enabled:
            return {"action": "hold", "reason": "策略已禁用", "strength": 0.0}

        try:
            func = self._load_func(record)
            result = func(context, data)
            return self._normalize_result(result)
        except Exception as e:
            logger.error(f"策略执行异常 [{record.name}]: {e}", exc_info=True)
            return {
                "action": "hold",
                "reason": f"执行异常: {e}",
                "strength": 0.0,
            }

    def review_signals(self, price_map: dict[str, float], days_list: list[int] | None = None) -> list[dict]:
        """回看过往信号的盈亏。

        对 days_list 中每个天数的历史信号，对比当日价格计算是否盈利。
        buy 信号: 当日价 > 信号价 = 正确; sell 信号: 当日价 < 信号价 = 正确

        Returns:
            [{"signal": dict, "review": {"days": N, "pnl_pct": X, "correct": bool}}, ...]
        """
        return self._signal_repository().review_signals(price_map, days_list)

    def signal_stats(self) -> dict:
        """获取信号全局统计。"""
        return self._signal_repository().signal_stats()

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        """初始化 SQLite 数据库。"""
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS strategies (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT DEFAULT '',
                    code_path TEXT NOT NULL,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signal_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    strategy_name TEXT,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    price REAL,
                    reason TEXT,
                    strength REAL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signal_review (
                    signal_id INTEGER,
                    review_date TEXT,
                    signal_price REAL,
                    review_price REAL,
                    pnl_pct REAL,
                    correct INTEGER,
                    PRIMARY KEY (signal_id, review_date)
                )
            """)
            conn.commit()

        self._seed_builtin_strategies()

    def _seed_builtin_strategies(self) -> None:
        """首次启动时将内置策略注册到数据库（幂等，已存在则跳过）。"""
        builtins = {
            "ma_crossover.py":      ("均线交叉",   "参数化金叉死叉：fast=50/slow=200长线，fast=10/slow=30短线"),
            "volume_breakout.py":   ("放量突破",   "价格上穿MA20 + 放量1.5倍买入，下穿MA20卖出"),
            "pullback_entry.py":   ("缩量回踩",   "价格回踩MA20 + 缩量<70%买入，偏离>5%+放量卖出"),
            "rsi_oversold.py":     ("RSI超卖反弹", "RSI从30以下回升买入，从70以上回落卖出"),
            "bollinger_squeeze.py":("布林收缩突破", "布林带宽压缩至60日最低，价格上穿中轨买入"),
            "nine_turns.py":       ("神奇九转",   "连续9天收盘价低于/高于4天前，第9天反转信号"),
            "trailing_stop.py":    ("ATR移动止盈", "ATR(14)×2 替代固定回撤，高波动宽、低波动窄"),
            "dividend_grid.py":    ("红利网格",   "真实股息率+布林收口+月线方向，三因子共振分级买入"),
        }
        import uuid, datetime
        storage = self._storage_dir
        now = datetime.datetime.now().isoformat()

        for filename, (name, desc) in builtins.items():
            filepath = storage / filename
            if not filepath.exists():
                continue
            with self._get_conn() as conn:
                exists = conn.execute(
                    "SELECT COUNT(*) FROM strategies WHERE code_path LIKE ?",
                    (f"%{filename}",),
                ).fetchone()[0]
                if exists:
                    continue
                sid = uuid.uuid4().hex[:12]
                conn.execute(
                    "INSERT INTO strategies (id, name, description, code_path, enabled, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, 1, ?, ?)",
                    (sid, name, desc, str(filepath), now, now),
                )
                conn.commit()
                logger.info(f"内置策略已注册: {name} ({sid})")

    def _load_func(self, record: StrategyRecord):
        """加载并编译策略函数（带缓存）。"""
        with self._lock:
            return self._load_func_unlocked(record)

    def _load_func_unlocked(self, record: StrategyRecord):
        code_hash = hashlib.md5(record.code.encode()).hexdigest()

        if record.id in self._cache:
            cached_hash, cached_func, cached_filter, cached_cooldown = self._cache[record.id]
            if cached_hash == code_hash:
                return cached_func

        import numpy as np
        local_ns: dict = {}
        exec(record.code, {"pd": pd, "np": np, "__builtins__": _safe_builtins()}, local_ns)
        func = local_ns["strategy"]
        filter_func = local_ns.get("symbols")  # 可选的标的过滤函数
        cooldown_func = local_ns.get("cooldown")  # 可选的回测冷却函数
        self._cache[record.id] = (code_hash, func, filter_func, cooldown_func)
        return func

    def get_symbol_filter(self, strategy_id: str):
        """获取策略的标的过滤函数（如果有的话）。

        Returns:
            callable(all_symbols, meta) -> list[str]，或 None。
        """
        with self._lock:
            record = self._record_cache.get(strategy_id) or self._get_unlocked(strategy_id)
            if record is None:
                return None
            self._load_func_unlocked(record)  # ensure cached
            return self._cache[record.id][2]

    def get_cooldown(self, strategy_id: str) -> tuple[int, int]:
        """获取策略的回测冷却周期 (buy_days, sell_days)。0 表示仅当日去重。"""
        with self._lock:
            record = self._record_cache.get(strategy_id) or self._get_unlocked(strategy_id)
            if record is None:
                return (0, 0)
            self._load_func_unlocked(record)
            return self._cooldown_from_cached(record)

    def execution_snapshot(self) -> list[StrategyExecutionSnapshot]:
        """Return records and policy hooks captured under the engine lock."""
        with self._lock:
            snapshots = []
            for record in self.list_all(enabled_only=True):
                self._load_func_unlocked(record)
                snapshots.append(StrategyExecutionSnapshot(
                    record=record,
                    symbol_filter=self._cache[record.id][2],
                    cooldown=self._cooldown_from_cached(record),
                ))
            return snapshots

    def _cooldown_from_cached(self, record: StrategyRecord) -> tuple[int, int]:
        cooldown_func = self._cache[record.id][3]
        if cooldown_func:
            try:
                value = cooldown_func()
                return int(value[0]), int(value[1])
            except Exception:
                logger.warning("策略冷却函数异常: %s", record.name, exc_info=True)
        return (0, 0)

    @staticmethod
    def _normalize_result(result: dict) -> dict:
        """标准化执行结果。"""
        action = str(result.get("action", "hold")).lower()
        if action not in ("buy", "sell", "hold"):
            action = "hold"
        return {
            "action": action,
            "reason": str(result.get("reason", "")),
            "strength": max(0.0, min(1.0, float(result.get("strength", 0.0)))),
        }

    def log_signal(
        self,
        strategy_id: str,
        strategy_name: str,
        symbol: str,
        action: str,
        price: float,
        reason: str,
        strength: float,
    ) -> None:
        """Compatibility wrapper for a one-record signal write."""
        self.log_signals_batch([{
            "strategy_id": strategy_id, "strategy_name": strategy_name, "symbol": symbol,
            "action": action, "price": price, "reason": reason, "strength": strength,
        }])

    def log_signals_batch(self, records: list[dict]) -> None:
        """Persist monitor signals in one SQLite transaction."""
        self._signal_repository().log_batch(records)

    def load_today_signal_keys(self) -> set[tuple[str, str, str]]:
        return self._signal_repository().today_keys()

    def load_recent_signal_ticks(self) -> dict[tuple[str, str, str], int]:
        return self._signal_repository().recent_signal_ticks()

    def list_recent_signals(self, limit: int = 50) -> list[dict]:
        return self._signal_repository().list_recent(limit)

    def _signal_repository(self) -> SignalRepository:
        """Keep legacy test/custom DB-path overrides compatible with the repository."""
        if self.signals._db_path != self._db_path:
            self.signals = SignalRepository(self._db_path)
        return self.signals

    def has_signal_today(self, strategy_id: str, symbol: str, action: str) -> bool:
        """检查今天同一标的+策略+方向是否已产生过信号。"""
        return (strategy_id, symbol, action) in self.load_today_signal_keys()
