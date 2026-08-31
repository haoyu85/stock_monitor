from datetime import date

from app.backtest.engine import BacktestEngine
from app.config import AppConfig
from app.market.dividend_repository import DividendRepository
from app.strategy.engine import StrategyEngine


class NoNetworkMarket:
    """A deliberately minimal market double: dividend preload needs no market I/O."""


def test_backtest_historical_dividends_are_loaded_from_repository_only(tmp_path):
    config = AppConfig()
    config.system.data_dir = str(tmp_path)
    repository = DividendRepository(tmp_path / "market_cache.db")
    repository.store_history("600000", [(date(2026, 1, 15), 1.5)], source="fixture")
    repository.store_etf_benchmark_history([(date(2026, 1, 15), 0.3, 3.0)], source="fixture")
    engine = BacktestEngine(config, StrategyEngine(config), NoNetworkMarket(), repository)

    data = engine._prefetch_dividends(["600000", "510050"])

    assert data["600000"] == [(date(2026, 1, 15), 1.5)]
    assert data["etf_dividends"] == [(date(2026, 1, 15), 0.3)]
    assert engine._dividend_at_date(data, "600000", date(2026, 2, 1), 10) == 15.0
