import pytest

from app.config import AppConfig
from app.context import AppContext
from app.scheduler.loop import MonitorLoop
from app.web.server import create_app


@pytest.fixture
def client(tmp_path):
    config = AppConfig()
    config.system.data_dir = str(tmp_path)
    app = create_app(context=AppContext(config))
    return app.test_client()


@pytest.mark.parametrize("path", ["/api/status", "/api/signals", "/api/positions", "/api/account"])
def test_legacy_sensitive_apis_require_login(client, path):
    assert client.get(path).status_code == 302


def test_web_and_monitor_share_position_store(tmp_path):
    config = AppConfig()
    config.system.data_dir = str(tmp_path)
    context = AppContext(config)
    app = create_app(context=context)
    with app.test_client() as client:
        client.post("/login", data={"password": config.web.password})
        response = client.post("/api/positions", json={"symbol": "510050", "shares": 100, "cost": 2.5})
    assert response.status_code == 200
    assert context.positions.get("510050")["shares"] == 100


def test_web_symbol_change_is_visible_to_next_monitor_snapshot(tmp_path):
    config = AppConfig()
    config.system.data_dir = str(tmp_path)
    context = AppContext(config)
    context.symbols._save_config_groups = lambda: None  # avoid changing repository config in this test
    loop = MonitorLoop(context=context)
    app = create_app(context=context, monitor_loop=loop)
    with app.test_client() as client:
        client.post("/login", data={"password": config.web.password})
        response = client.post("/api/instruments/add", json={"symbol": "600000"})
    assert response.status_code == 200
    assert "600000" in loop._symbols.get_active_symbols()
