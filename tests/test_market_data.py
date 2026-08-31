from app.market.data import MarketData


def test_sina_quote_with_zero_previous_close_is_kept_without_division_error():
    fields = ["测试", "10.0", "0", "10.2", "10.3", "9.9", "", "", "100", "1000"]
    fields.extend([""] * (33 - len(fields)))
    text = 'var hq_str_sh600000="' + ",".join(fields) + '";'

    rows = MarketData._parse_sina_response(text, ["sh600000"])

    assert rows == [{
        "symbol": "600000", "name": "测试", "price": 10.2,
        "open": 10.0, "pre_close": 0.0, "high": 10.3, "low": 9.9,
        "volume": 100.0, "amount": 1000.0, "pct_change": 0.0,
        "change": 10.2,
    }]
