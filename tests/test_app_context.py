from app.config import AppConfig
from app.context import AppContext
from app.scheduler.loop import MonitorLoop


def test_monitor_uses_context_services(tmp_path):
    config = AppConfig()
    config.system.data_dir = str(tmp_path)
    context = AppContext(config)
    loop = MonitorLoop(context=context)

    assert loop._market is context.market
    assert loop._symbols is context.symbols
    assert loop._engine is context.strategies
    assert loop._positions is context.positions
    assert loop._notify_mgr is context.notifications
    assert loop._dividends is context.dividends


def test_position_snapshot_is_real_position_context(tmp_path):
    config = AppConfig()
    config.system.data_dir = str(tmp_path)
    context = AppContext(config)
    context.positions.add("510050", 1000, 2.5, "2026-01-01")
    loop = MonitorLoop(context=context)

    live = loop._build_live_strategy_context(context.positions.list_all(), {"510050": 3.0})
    assert live["positions"] == {"510050": 1000}
    assert live["holdings"] == {"510050": 3000.0}
    assert live["cash"] is None


def test_context_monitor_snapshot_is_defensive(tmp_path):
    config = AppConfig()
    config.system.data_dir = str(tmp_path)
    config.symbols.groups = {"股票": ["600001"]}
    context = AppContext(config)
    context.symbols._save_config_groups = lambda: None  # test must not write user's config.yaml
    context.positions.add("600001", 100, 10, "2026-01-01")

    snapshot = context.take_monitor_snapshot()
    context.symbols.add_symbol("600002")
    context.positions.add("600002", 200, 10, "2026-01-01")

    assert snapshot.symbols == ("600001",)
    assert snapshot.positions == ({"symbol": "600001", "shares": 100, "cost": 10, "date": "2026-01-01"},)
