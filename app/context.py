"""Application-wide lightweight service container."""

import logging
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from app.market.data import MarketData
from app.market.dividend_service import DividendService
from app.notify.manager import NotificationManager
from app.strategy.engine import StrategyEngine
from app.symbols.groups import GroupStore
from app.symbols.manager import SymbolManager
from app.trade.positions import PositionStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MonitorSnapshot:
    """The immutable service view consumed by one monitor execution."""

    symbols: tuple[str, ...]
    symbol_meta: dict[str, dict]
    strategies: tuple[object, ...]
    positions: tuple[dict, ...]


class AppContext:
    """Own one shared set of mutable production services for Web and Monitor."""

    def __init__(self, config):
        self.config = config
        # Symbol and notification Web mutations both persist into config.yaml.
        # They must share a file-level lock, not merely separate service locks.
        self._config_write_lock = RLock()
        data_dir = Path(config.system.data_dir)
        self.market = MarketData(config.market)
        self.symbols = SymbolManager(config.symbols, self.market, self._config_write_lock)
        self.strategies = StrategyEngine(config)
        self.positions = PositionStore(str(data_dir / "positions.json"))
        self.notifications = NotificationManager(config, self._config_write_lock)
        self.dividends = DividendService(data_dir)
        self.groups = GroupStore(str(data_dir / "symbol_groups.json"))
        self.groups.ensure_defaults()
        self._snapshot_lock = RLock()
        logger.info("Shared AppContext initialized")

    def take_monitor_snapshot(self) -> MonitorSnapshot:
        """Capture stable copies of every mutable input used by ``run_once``.

        Individual services own their own locks.  This coordinating lock prevents
        two monitor snapshots from interleaving; the returned values are defensive
        copies and must be treated as read-only for the remainder of a run.
        """
        with self._snapshot_lock:
            symbol_snapshot = self.symbols.snapshot()
            return MonitorSnapshot(
                symbols=symbol_snapshot.symbols,
                symbol_meta=symbol_snapshot.meta,
                strategies=tuple(self.strategies.execution_snapshot()),
                positions=tuple(self.positions.list_all()),
            )
