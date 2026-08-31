"""Shared execution policy used by live monitoring and backtests."""

from dataclasses import dataclass, field


@dataclass
class StrategyExecutionState:
    today_keys: set[tuple[str, str, str]] = field(default_factory=set)
    recent_signals: dict[tuple[str, str, str], int] = field(default_factory=dict)

    def next_day(self) -> None:
        self.today_keys.clear()


class StrategyPolicy:
    def __init__(self, engine, config):
        self._engine = engine
        self._config = config

    def applicable_symbols(self, strategy_id: str, symbols: list[str], meta: dict | None = None,
                           selector=None) -> list[str]:
        selector = selector if selector is not None else self._engine.get_symbol_filter(strategy_id)
        if not selector:
            return list(symbols)
        selected = selector(symbols, meta or {symbol: {"name": symbol} for symbol in symbols})
        return [symbol for symbol in selected if symbol in symbols]

    def accepts_strength(self, result: dict) -> bool:
        return result.get("action") != "hold" and result.get("strength", 0) >= self._config.scheduler.min_signal_strength

    def can_emit(self, state: StrategyExecutionState, strategy_id: str, symbol: str, action: str,
                 tick: int = 0, cooldown: tuple[int, int] | None = None) -> bool:
        key = (strategy_id, symbol, action)
        if key in state.today_keys:
            return False
        buy_days, sell_days = cooldown if cooldown is not None else self._engine.get_cooldown(strategy_id)
        cooldown = buy_days if action == "buy" else sell_days
        return tick - state.recent_signals.get(key, -999999) >= cooldown

    @staticmethod
    def mark_emitted(state: StrategyExecutionState, strategy_id: str, symbol: str, action: str, tick: int = 0) -> None:
        key = (strategy_id, symbol, action)
        state.today_keys.add(key)
        state.recent_signals[key] = tick

    def apply_market_regime(self, result: dict, regime: dict | None) -> dict | None:
        """Apply the existing buy-side market timing semantics without changing strategy logic."""
        if result.get("action") != "buy" or not self._config.market_timing.enabled:
            return result
        regime = regime or {}
        if not regime.get("above_ma_bear", True):
            return None
        adjusted = dict(result)
        if not regime.get("above_ma_weak", True):
            discount = self._config.market_timing.strength_discount
            adjusted["strength"] *= discount
            adjusted["reason"] = f"{adjusted.get('reason', '')} [弱市x{discount}]"
            if not self.accepts_strength(adjusted):
                return None
        return adjusted
