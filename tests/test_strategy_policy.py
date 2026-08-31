from app.config import AppConfig
from app.strategy.engine import StrategyEngine


def test_strategy_update_preserves_id_and_signal_history(tmp_path):
    config = AppConfig()
    config.system.data_dir = str(tmp_path)
    engine = StrategyEngine(config)
    code = "def strategy(context, data):\n    return {'action': 'hold', 'reason': '', 'strength': 0}\n"
    record = engine.create("before", "old", code)
    engine.log_signal(record.id, record.name, "600000", "buy", 10, "old", 1)

    updated = engine.update(record.id, name="after", description="new", code=code)
    assert updated.id == record.id
    assert updated.name == "after"
    assert engine.list_recent_signals()[0]["strategy_id"] == record.id
