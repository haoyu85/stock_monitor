"""
回测引擎。

按交易日逐日推进，对历史数据执行策略并模拟交易，
输出权益曲线和绩效指标。
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from app.backtest.metrics import compute_metrics
from app.market.dividend_repository import DividendRepository
from app.strategy.policy import StrategyExecutionState, StrategyPolicy

logger = logging.getLogger(__name__)


class BacktestBroker:
    """回测专用模拟账户。"""

    def __init__(self, initial_capital: float, position_ratio: float = 0.1,
                 etf_lot: int = 100, stock_lot: int = 100):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions: dict[str, dict] = {}
        self.trades: list[dict] = []
        self.position_ratio = position_ratio
        self.etf_lot = etf_lot
        self.stock_lot = stock_lot

    def equity(self, price_map: dict[str, float]) -> float:
        mv = sum(self.positions[s]["shares"] * price_map.get(s, 0)
                 for s in self.positions)
        return self.cash + mv

    def calc_quantity(self, symbol: str, price: float) -> int:
        """按当前权益的 position_ratio% 计算买入股数，向下取整到 100 股。"""
        if price <= 0:
            return 0
        equity = self.cash  # 保守：用现金而非总权益，避免高估值时过度买入
        target = equity * self.position_ratio
        lot = self.etf_lot if symbol.startswith(("5", "1", "58", "16")) else self.stock_lot
        qty = int(target / price / lot) * lot
        return max(lot, qty)

    def buy(self, symbol: str, price: float, quantity: int, date: str, reason: str = "") -> bool:
        cost = price * quantity + 5
        if cost > self.cash or quantity <= 0:
            return False
        self.cash -= cost
        pos = self.positions.get(symbol, {"shares": 0, "avg_cost": 0.0})
        total = pos["shares"] * pos["avg_cost"] + quantity * price
        pos["shares"] += quantity
        pos["avg_cost"] = total / pos["shares"] if pos["shares"] > 0 else price
        self.positions[symbol] = pos
        self.trades.append({"symbol": symbol, "action": "buy", "price": price,
                            "quantity": quantity, "date": date,
                            "position_after": pos["shares"], "reason": reason})
        return True

    def sell(self, symbol: str, price: float, quantity: int, date: str, reason: str = "") -> bool:
        pos = self.positions.get(symbol)
        if not pos or pos["shares"] < quantity or quantity <= 0:
            return False
        revenue = price * quantity - 5
        self.cash += revenue
        pos["shares"] -= quantity
        remaining = pos["shares"]
        if remaining <= 0:
            del self.positions[symbol]
        else:
            self.positions[symbol] = pos
        self.trades.append({"symbol": symbol, "action": "sell", "price": price,
                            "quantity": quantity, "date": date,
                            "position_after": remaining, "reason": reason})
        return True

class BacktestEngine:
    """逐日回测引擎。"""

    def __init__(self, config, strategy_engine, market_data, dividend_repository: DividendRepository | None = None):
        self._config = config
        self._engine = strategy_engine
        self._market = market_data
        self._policy = StrategyPolicy(strategy_engine, config)
        self._dividend_repository = dividend_repository or DividendRepository(
            Path(config.system.data_dir) / "market_cache.db"
        )

    def run(
        self,
        strategy_id: str,
        symbols: list[str],
        start_date: str,
        end_date: str,
        initial_capital: float = 100000.0,
        skip_filter: bool = False,
        position_ratio: float = 0.1,
    ) -> dict:
        """执行回测。"""
        # 估算需要的 K 线数量：交易日数 + 缓冲
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            days_needed = max(250, int((end_dt - start_dt).days * 5 / 7) + 250)
        except ValueError:
            days_needed = 250
        strategy = self._engine.get(strategy_id)
        if strategy is None:
            return {"error": f"策略不存在: {strategy_id}"}

        self._market.ensure_history(symbols, days_needed)

        broker = BacktestBroker(initial_capital, position_ratio=position_ratio)


        # 分红数据预取（回测期间股息率 = 最新分红 / 历史时点价格）
        self._div_data = self._prefetch_dividends(symbols)

        # 大盘择时数据
        mt = self._config.market_timing
        benchmark_regime = {}
        if mt.enabled:
            benchmark_regime = self._compute_benchmark_regime(mt.benchmark)

        # 权益曲线从指定起始日开始
        equity_curve = [{"date": start_date, "value": initial_capital}]
        state = StrategyExecutionState()

        trading_dates = self._build_trading_calendar(symbols, start_date, end_date)
        logger.info(f"回测开始: {strategy.name} × {len(symbols)} 只, "
                    f"{start_date} → {end_date}, {len(trading_dates)} 个交易日")

        # 预加载并预索引 K 线：避免每天重复做 pandas 日期比较
        all_histories: dict[str, pd.DataFrame] = {}
        sym_close: dict[str, list] = {}
        sym_pos: dict[str, dict] = {}  # symbol -> {trading_day_index: row_position}
        for sym in symbols:
            h = self._market.get_history(sym, period="days", freq="daily")
            if h.empty or "close" not in h.columns:
                continue
            all_histories[sym] = h
            close_vals = h["close"].values
            sym_close[sym] = close_vals
            # 为每个交易日预计算该标的的数据位置
            pos_map = {}
            hi = 0
            for ti, tdt in enumerate(trading_dates):
                while hi < len(h) and h.index[hi] <= tdt:
                    hi += 1
                if hi > 0:
                    pos_map[ti] = hi
            sym_pos[sym] = pos_map

        for ti, dt in enumerate(trading_dates):
            date_str = dt.strftime("%Y-%m-%d")
            state.next_day()

            if not skip_filter and self._engine.get_symbol_filter(strategy_id):
                active = self._policy.applicable_symbols(strategy_id, symbols)
                if not active:
                    return {"error": f"策略 \"{strategy.name}\" 的标的过滤未匹配任何选中标的（策略限制：仅 ETF 等特定类型）"}
            else:
                active = list(symbols)

            # 快速价格映射（O(1) 查数组）
            price_map = {}
            for sym in all_histories:
                p = sym_pos[sym].get(ti)
                if p is not None:
                    price_map[sym] = float(sym_close[sym][p - 1])

            context = {
                "cash": broker.cash,
                "positions": {s: p["shares"] for s, p in broker.positions.items()},
                "position_count": len(broker.positions),
                "div_yield": 0.0,  # 每个标的单独设置
            }

            for sym in active:
                if sym not in sym_pos:
                    continue
                p = sym_pos[sym].get(ti, 0)
                if p < 21:
                    continue
                data = all_histories[sym].iloc[:p]  # 整数索引，无日期比较

                # 按历史时点计算股息率
                price = price_map.get(sym, 0)
                context["div_yield"] = self._dividend_at_date(self._div_data, sym, dt, price) if price > 0 else 0.0
                context["is_etf"] = sym.startswith(("5", "1", "58", "16"))

                try:
                    result = self._engine.execute(strategy.id, context, data)

                    if not self._policy.accepts_strength(result):
                        continue

                    if not self._policy.can_emit(state, strategy.id, sym, result["action"], ti):
                        continue

                    price = price_map.get(sym, 0)
                    if price <= 0:
                        continue

                    result = self._policy.apply_market_regime(result, benchmark_regime.get(date_str, {}) if benchmark_regime else None)
                    if result is None:
                        continue
                    self._policy.mark_emitted(state, strategy.id, sym, result["action"], ti)

                    if result["action"] == "buy":
                        qty = broker.calc_quantity(sym, price)
                        broker.buy(sym, price, qty, date_str, result.get("reason", ""))

                    elif result["action"] == "sell":
                        pos = broker.positions.get(sym)
                        if pos:
                            qty = broker.calc_quantity(sym, price)
                            broker.sell(sym, price, min(qty, pos["shares"]), date_str, result.get("reason", ""))

                except Exception as e:
                    logger.debug(f"回测策略异常 {sym} @ {date_str}: {e}")

            equity_curve.append({
                "date": date_str,
                "value": round(broker.equity(price_map), 2),
            })

        metrics = compute_metrics(equity_curve, broker.trades, initial_capital)
        benchmark_return = self._benchmark_return(symbols, start_date, end_date)
        metrics["benchmark_return"] = round(benchmark_return * 100, 2)
        metrics["excess_return"] = round(metrics["total_return"] - metrics["benchmark_return"], 2)

        logger.info(f"回测完成: 总收益={metrics['total_return']}%, "
                    f"最大回撤={metrics['max_drawdown']}%, "
                    f"夏普={metrics['sharpe_ratio']}, 交易={metrics['total_trades']}次")

        # 期末持仓
        final_positions = []
        for sym, pos in broker.positions.items():
            p = price_map.get(sym, 0)
            mv = pos["shares"] * p
            pnl = (p - pos["avg_cost"]) * pos["shares"] if pos["avg_cost"] > 0 else 0
            final_positions.append({
                "symbol": sym, "shares": pos["shares"], "avg_cost": round(pos["avg_cost"], 3),
                "price": round(p, 3), "market_value": round(mv, 2),
                "pnl": round(pnl, 2),
            })

        return {"metrics": metrics, "equity": equity_curve, "trades": broker.trades,
                "final_positions": final_positions}

    def run_combined(
        self,
        strategy_ids: list[str],
        symbols: list[str],
        start_date: str,
        end_date: str,
        initial_capital: float = 100000.0,
        skip_filter: bool = False,
        position_ratio: float = 0.1,
    ) -> dict:
        """多策略组合回测，共享同一资金池，信号按强度优先级竞争资金。"""
        self._market.ensure_history(symbols, 250)
        broker = BacktestBroker(initial_capital, position_ratio=position_ratio)

        strategies = [s for s in (self._engine.get(sid) for sid in strategy_ids) if s]
        if not strategies:
            return {"error": "无有效策略"}

        mt = self._config.market_timing
        benchmark_regime = self._compute_benchmark_regime(mt.benchmark) if mt.enabled else {}

        equity_curve = [{"date": start_date, "value": initial_capital}]
        state = StrategyExecutionState()
        trading_dates = self._build_trading_calendar(symbols, start_date, end_date)

        all_histories, sym_close, sym_pos = {}, {}, {}
        for sym in symbols:
            h = self._market.get_history(sym, period="days", freq="daily")
            if h.empty or "close" not in h.columns:
                continue
            all_histories[sym] = h
            close_vals = h["close"].values
            sym_close[sym] = close_vals
            pos_map = {}
            hi = 0
            for ti, tdt in enumerate(trading_dates):
                while hi < len(h) and h.index[hi] <= tdt:
                    hi += 1
                if hi > 0:
                    pos_map[ti] = hi
            sym_pos[sym] = pos_map

        logger.info(f"组合回测: {[s.name for s in strategies]}, {len(trading_dates)}天")

        self._div_data = self._prefetch_dividends(symbols)

        for ti, dt in enumerate(trading_dates):
            date_str = dt.strftime("%Y-%m-%d")
            state.next_day()
            price_map = {}
            for sym in all_histories:
                p = sym_pos[sym].get(ti)
                if p is not None:
                    price_map[sym] = float(sym_close[sym][p - 1])

            context = {
                "cash": broker.cash,
                "positions": {s: p["shares"] for s, p in broker.positions.items()},
                "position_count": len(broker.positions),
                "div_yield": 0.0,
            }

            pending = []
            for strategy in strategies:
                active = list(symbols) if skip_filter else self._policy.applicable_symbols(strategy.id, symbols)
                for sym in active:
                    if sym not in sym_pos:
                        continue
                    p = sym_pos[sym].get(ti, 0)
                    if p < 21:
                        continue
                    data = all_histories[sym].iloc[:p]

                    # 按历史时点计算股息率
                    price = price_map.get(sym, 0)
                    context["div_yield"] = self._dividend_at_date(self._div_data, sym, dt, price) if price > 0 else 0.0
                    context["is_etf"] = sym.startswith(("5", "1", "58", "16"))

                    try:
                        result = self._engine.execute(strategy.id, context, data)
                        if not self._policy.accepts_strength(result):
                            continue
                        price = price_map.get(sym, 0)
                        if price <= 0:
                            continue
                        pending.append((result["strength"], strategy, sym, result, price))
                    except Exception:
                        pass

            pending.sort(key=lambda x: x[0], reverse=True)
            for _, strategy, sym, result, price in pending:
                if not self._policy.can_emit(state, strategy.id, sym, result["action"], ti):
                    continue
                result = self._policy.apply_market_regime(result, benchmark_regime.get(date_str, {}) if benchmark_regime else None)
                if result is None:
                    continue
                self._policy.mark_emitted(state, strategy.id, sym, result["action"], ti)
                if result["action"] == "buy":
                    qty = broker.calc_quantity(sym, price)
                    broker.buy(sym, price, qty, date_str, result.get("reason", ""))
                elif result["action"] == "sell":
                    pos = broker.positions.get(sym)
                    if pos:
                        qty = broker.calc_quantity(sym, price)
                        broker.sell(sym, price, min(qty, pos["shares"]), date_str, result.get("reason", ""))

            equity_curve.append({"date": date_str, "value": round(broker.equity(price_map), 2)})

        metrics = compute_metrics(equity_curve, broker.trades, initial_capital)
        bm = self._benchmark_return(symbols, start_date, end_date)
        metrics["benchmark_return"] = round(bm * 100, 2)
        metrics["excess_return"] = round(metrics["total_return"] - metrics["benchmark_return"], 2)

        final_positions = []
        for sym, pos in broker.positions.items():
            p = price_map.get(sym, 0)
            mv = pos["shares"] * p
            pnl = (p - pos["avg_cost"]) * pos["shares"] if pos["avg_cost"] > 0 else 0
            final_positions.append({
                "symbol": sym, "shares": pos["shares"], "avg_cost": round(pos["avg_cost"], 3),
                "price": round(p, 3), "market_value": round(mv, 2),
                "pnl": round(pnl, 2),
            })

        return {"metrics": metrics, "equity": equity_curve, "trades": broker.trades,
                "final_positions": final_positions}

    def _force_kline_fetch(self, symbols: list[str], datalen: int) -> None:
        """Compatibility shim; uses MarketData's supported public API."""
        self._market.ensure_history(symbols, datalen)

    def _prefetch_dividends(self, symbols: list[str]) -> dict:
        """Load historical dividends from the local repository only.

        Backtesting intentionally has no AKShare/CNINFO path: absent repository
        data simply yields zero dividend yield for the affected strategy run.
        """
        return self._dividend_repository.backtest_data(symbols)

    @staticmethod
    def _nearest_price(prices: dict[str, float], target) -> float:
        """在价格字典中查找目标日期当天或之前最近一个交易日的价格。"""
        from datetime import date, timedelta
        d = target if isinstance(target, date) else target.date()
        for offset in range(7):
            check = d - timedelta(days=offset)
            p = prices.get(check.isoformat(), 0)
            if p > 0:
                return p
        return 0.0

    @staticmethod
    def _dividend_at_date(div_data: dict, sym: str, dt, price: float) -> float:
        """计算某只股票在指定日期的股息率(%)。

        个股：滚动 12 个月累计分红 / 当日价格 × 100
        ETF：510880 滚动 12 个月分红 / 510880 当日价格 × 100
        """
        from datetime import timedelta
        d = dt.date() if hasattr(dt, "date") else dt
        cutoff = d - timedelta(days=365)

        is_etf = sym.startswith(("5", "1", "58", "16"))
        if is_etf:
            # 用 510880 的历史价格和分红计算基准股息率
            timeline = div_data.get("etf_dividends", [])
            prices = div_data.get("etf_prices", {})
            # 取目标日期当天或之前最近一个交易日的价格
            etf_price = BacktestEngine._nearest_price(prices, d)
            if not timeline or etf_price <= 0:
                return 0.0

            total_div = 0.0
            for ex_date, annual_div in timeline:
                if cutoff < ex_date <= d:
                    total_div += annual_div
            return round(total_div / etf_price * 100, 2) if total_div > 0 else 0.0

        # 个股
        timeline = div_data.get(sym, [])
        if not timeline or price <= 0:
            return 0.0

        total_dps = 0.0
        for ex_date, div in timeline:
            if cutoff < ex_date <= d:
                total_dps += div

        return round(total_dps / price * 100, 2) if total_dps > 0 else 0.0

    def _compute_benchmark_regime(self, benchmark: str) -> dict[str, dict]:
        """预计算基准指数每日的大盘环境。

        Returns:
            {date_str: {"above_ma_bear": bool, "above_ma_weak": bool}, ...}
        """
        mt = self._config.market_timing
        result = {}
        try:
            hist = self._market.get_history(benchmark, period="days", freq="daily")
        except Exception as exc:
            logger.warning("回测基准数据不可用，跳过大盘择时: %s", exc)
            return result
        if hist.empty or "close" not in hist.columns:
            return result
        close = hist["close"]
        ma_weak = close.rolling(mt.ma_weak).mean()
        ma_bear = close.rolling(mt.ma_bear).mean()
        for d in hist.index:
            if hasattr(d, "to_pydatetime"):
                d = d.to_pydatetime()
            if d is None:
                continue
            date_str = d.strftime("%Y-%m-%d")
            if pd.isna(ma_weak[d]) or pd.isna(ma_bear[d]):
                result[date_str] = {"above_ma_bear": True, "above_ma_weak": True}
            else:
                p = float(close[d])
                result[date_str] = {
                    "above_ma_bear": p > float(ma_bear[d]),
                    "above_ma_weak": p > float(ma_weak[d]),
                }
        return result

    def _build_trading_calendar(
        self, symbols: list[str], start: str, end: str
    ) -> list[datetime]:
        """以第一只标的的 K 线日期为交易日历基准。A 股所有标的共享同一交易日历。"""
        dates_set = set()
        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = datetime.strptime(end, "%Y-%m-%d")

        hist = self._market.get_history(symbols[0], period="days", freq="daily")
        if not hist.empty:
            for d in hist.index:
                if hasattr(d, "to_pydatetime"):
                    d = d.to_pydatetime()
                if d is not None and start_dt <= d <= end_dt:
                    dates_set.add(d)

        if not dates_set:
            d = start_dt
            while d <= end_dt:
                if d.weekday() < 5:
                    dates_set.add(d)
                d += timedelta(days=1)

        return sorted(dates_set)

    def _benchmark_return(
        self, symbols: list[str], start: str, end: str
    ) -> float:
        returns = []
        for sym in symbols:
            hist = self._market.get_history(sym, period="days", freq="daily")
            if hist.empty or "close" not in hist.columns:
                continue
            close = hist["close"]
            start_slice = close[close.index <= start]
            end_slice = close[close.index <= end]
            if start_slice.empty or end_slice.empty:
                continue
            r = float(end_slice.iloc[-1] / start_slice.iloc[-1] - 1)
            returns.append(r)
        return sum(returns) / len(returns) if returns else 0
