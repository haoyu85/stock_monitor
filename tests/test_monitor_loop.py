from types import SimpleNamespace

import pandas as pd

from app.config import AppConfig
from app.context import AppContext
from app.scheduler.loop import MonitorLoop
from app.strategy.engine import StrategyExecutionSnapshot


def _history():
    index = pd.date_range("2026-01-01", periods=30, freq="D")
    return pd.DataFrame({"open": 10, "high": 11, "low": 9, "close": 10, "volume": 1000}, index=index)


def test_monitor_fast_path_handles_75_symbols_without_dividend_requests(tmp_path, monkeypatch):
    config = AppConfig()
    config.system.data_dir = str(tmp_path)
    symbols = [f"60{i:04d}" for i in range(75)]
    config.symbols.groups = {"股票": symbols}
    context = AppContext(config)
    loop = MonitorLoop(context=context)
    strategies = [SimpleNamespace(id=f"s{i}", name=f"strategy-{i}", enabled=True) for i in range(8)]
    quotes = pd.DataFrame({
        "symbol": symbols, "name": symbols, "price": [10.0] * len(symbols),
        "open": [10.0] * len(symbols), "high": [10.0] * len(symbols),
        "low": [10.0] * len(symbols), "volume": [1000.0] * len(symbols),
    })
    monkeypatch.setattr(context.market, "preload_kline_cache", lambda values: None)
    monkeypatch.setattr(context.market, "get_realtime_quotes", lambda values: quotes)
    monkeypatch.setattr(context.market, "get_history", lambda *args, **kwargs: _history())
    monkeypatch.setattr(context.strategies, "execution_snapshot", lambda: [
        StrategyExecutionSnapshot(record=strategy, symbol_filter=None, cooldown=(0, 0))
        for strategy in strategies
    ])
    monkeypatch.setattr(context.strategies, "execute_record", lambda *args: {"action": "hold", "reason": "", "strength": 0})

    def unexpected_refresh(*args, **kwargs):
        raise AssertionError("DividendProvider must not be called by run_once")

    monkeypatch.setattr(context.dividends._provider, "get_stock_dividend", unexpected_refresh)
    monkeypatch.setattr(context.dividends._provider, "get_etf_benchmark_yield", unexpected_refresh)
    summary = loop.run_once()
    assert summary["status"] == "ok"
    assert summary["symbols"] == 75
    assert summary["strategies"] == 8


def test_monitor_applies_strategy_symbol_filter_and_cooldown(tmp_path, monkeypatch):
    config = AppConfig()
    config.system.data_dir = str(tmp_path)
    config.symbols.groups = {"股票": ["600001", "600002"]}
    context = AppContext(config)
    loop = MonitorLoop(context=context)
    strategy = SimpleNamespace(id="s1", name="filtered", enabled=True)
    quotes = pd.DataFrame({"symbol": ["600001", "600002"], "name": ["a", "b"], "price": [10.0, 10.0], "open": [10, 10], "high": [11, 11], "low": [9, 9], "volume": [1, 1]})
    monkeypatch.setattr(context.market, "preload_kline_cache", lambda values: None)
    monkeypatch.setattr(context.market, "get_realtime_quotes", lambda values: quotes)
    monkeypatch.setattr(context.market, "get_history", lambda *args, **kwargs: _history())
    monkeypatch.setattr(context.strategies, "execution_snapshot", lambda: [
        StrategyExecutionSnapshot(
            record=strategy,
            symbol_filter=lambda all_symbols, meta: ["600001"],
            cooldown=(2, 0),
        )
    ])
    monkeypatch.setattr(context.strategies, "execute_record", lambda *args: {"action": "buy", "reason": "test", "strength": 1})
    monkeypatch.setattr(loop, "_notify_batch", lambda *args: None)

    assert loop.run_once()["signals"] == 1
    assert loop.run_once()["signals"] == 0


def test_daemon_registers_monitor_and_closing_jobs(tmp_path, monkeypatch):
    config = AppConfig()
    config.system.data_dir = str(tmp_path)
    context = AppContext(config)
    loop = MonitorLoop(context=context)
    registered = []

    class Scheduler:
        running = True

        def add_job(self, func, trigger, **kwargs):
            registered.append(kwargs["id"])

        def start(self):
            pass

        def shutdown(self, wait=False):
            pass

        def pause(self):
            pass

        def resume(self):
            pass

    monkeypatch.setattr("app.scheduler.loop.BackgroundScheduler", lambda **kwargs: Scheduler())
    monkeypatch.setattr("app.scheduler.loop._shutdown_flag", True)
    loop.run_daemon()
    assert {"monitor_tick", "dividend_refresh", "closing_summary"}.issubset(registered)


def test_monitor_uses_start_of_run_snapshots(tmp_path, monkeypatch):
    config = AppConfig()
    config.system.data_dir = str(tmp_path)
    config.symbols.groups = {"股票": ["600001"]}
    context = AppContext(config)
    context.symbols._save_config_groups = lambda: None  # isolate mutation from repository config.yaml
    context.positions.add("600001", 100, 10, "2026-01-01")
    loop = MonitorLoop(context=context)
    strategy = SimpleNamespace(id="s1", name="snapshot", enabled=True)
    monkeypatch.setattr(context.strategies, "execution_snapshot", lambda: [
        StrategyExecutionSnapshot(record=strategy, symbol_filter=None, cooldown=(0, 0))
    ])
    monkeypatch.setattr(context.market, "preload_kline_cache", lambda values: None)

    def quotes(values):
        # These Web-like writes happen after Monitor has taken its snapshot.
        context.symbols.add_symbol("600002")
        context.positions.add("600002", 200, 10, "2026-01-01")
        return pd.DataFrame({"symbol": ["600001"], "name": ["a"], "price": [10.0],
                             "open": [10.0], "high": [11.0], "low": [9.0], "volume": [1.0]})

    seen = []
    monkeypatch.setattr(context.market, "get_realtime_quotes", quotes)
    monkeypatch.setattr(context.market, "get_history", lambda *args, **kwargs: _history())
    monkeypatch.setattr(context.strategies, "execute_record", lambda record, live, data: seen.append(dict(live)) or {"action": "hold", "reason": "", "strength": 0})

    assert loop.run_once()["symbols"] == 1
    assert seen[0]["positions"] == {"600001": 100}


def test_scheduled_dividend_refresh_is_failure_isolated(tmp_path, monkeypatch):
    config = AppConfig()
    config.system.data_dir = str(tmp_path)
    config.symbols.groups = {"股票": ["600001"]}
    context = AppContext(config)
    loop = MonitorLoop(context=context)
    context.dividends._put("600001", 1.0, "test")
    monkeypatch.setattr(context.dividends, "refresh", lambda symbols: (_ for _ in ()).throw(RuntimeError("offline")))

    loop._refresh_dividend_cache()
    # Scheduler exception is contained and previous cache data remains usable.
    assert context.dividends.get_yield("600001", 10) == 10.0
