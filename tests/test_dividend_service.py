from datetime import datetime

from app.market.dividend_service import DividendService


class FakeDividendProvider:
    def __init__(self):
        self.stock_calls = 0
        self.etf_calls = 0

    def get_stock_dividend(self, symbol):
        self.stock_calls += 1
        return 1.2

    def get_etf_benchmark_yield(self):
        self.etf_calls += 1
        return 3.5


def test_cached_yield_does_not_request_provider(tmp_path):
    provider = FakeDividendProvider()
    service = DividendService(tmp_path, provider=provider)
    service.refresh(["600000"])
    assert service.get_yield("600000", 10) == 12.0
    assert provider.stock_calls == 1
    assert service.get_yield("600000", 10) == 12.0
    assert provider.stock_calls == 1


def test_missing_cache_returns_zero_without_provider_call(tmp_path):
    provider = FakeDividendProvider()
    service = DividendService(tmp_path, provider=provider)
    assert service.get_yield("600000", 10) == 0
    assert provider.stock_calls == 0


def test_refresh_failure_is_isolated(tmp_path):
    class BrokenProvider(FakeDividendProvider):
        def get_stock_dividend(self, symbol):
            raise RuntimeError("CNINFO unavailable")

    service = DividendService(tmp_path, provider=BrokenProvider())
    assert service.refresh(["600000"]) == {"refreshed": 0, "failed": 1}
    assert service.get_yield("600000", 10) == 0


def test_refresh_failure_retains_existing_cache(tmp_path):
    provider = FakeDividendProvider()
    service = DividendService(tmp_path, provider=provider)
    service.refresh(["600000"])

    def broken(symbol):
        raise RuntimeError("CNINFO unavailable")

    provider.get_stock_dividend = broken
    assert service.refresh(["600000"]) == {"refreshed": 0, "failed": 1}
    assert service.get_yield("600000", 10) == 12.0
