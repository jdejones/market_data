# pyright: reportMissingImports=false
"""
Simple Backtrader backtest helpers.

Implements a daily timeframe SMA crossover strategy:
- Buy when 10-day SMA crosses above 20-day SMA
- Exit (sell/close) when 10-day SMA crosses below 20-day SMA

Designed to work with this repository's daily OHLCV DataFrames, which typically
have a datetime index (named 'Date') and columns: Open, High, Low, Close, Volume.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd

try:
    import backtrader as bt
except Exception as e:  # pragma: no cover
    raise ImportError(
        "Missing dependency 'backtrader'. Install with: pip install backtrader"
    ) from e


class PandasDailyOHLCV(bt.feeds.PandasData):
    """
    Backtrader feed mapping for the repo's daily OHLCV DataFrames.

    Expects:
    - datetime: index (datetime=None)
    - columns: Open, High, Low, Close, Volume
    """

    lines = ("rvol",)

    params = (
        ("datetime", None),
        ("open", "Open"),
        ("high", "High"),
        ("low", "Low"),
        ("close", "Close"),
        ("volume", "Volume"),
        ("rvol", "RVol"),
        ("openinterest", None),
    )


class SmaCrossDaily(bt.Strategy):
    """
    Buy when fast SMA crosses above slow SMA; close when it crosses below.
    """

    params = dict(
        fast=10,
        slow=20,
        stake=1,  # used by FixedSize sizer if enabled
        use_stoploss=True,
        printlog=False,
    )

    def log(self, txt: str) -> None:
        if not self.p.printlog:
            return
        dt = self.datas[0].datetime.date(0)
        print(f"{dt.isoformat()} {txt}")

    def __init__(self) -> None:
        sma_fast = bt.indicators.SMA(self.data.close, period=int(self.p.fast))
        sma_slow = bt.indicators.SMA(self.data.close, period=int(self.p.slow))
        self.crossover = bt.indicators.CrossOver(sma_fast, sma_slow)

        self.rvol = self.data.rvol
        self.order = None
        self.stop_order = None

    def notify_order(self, order) -> None:
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    f"BUY  price={order.executed.price:.2f} size={order.executed.size}"
                )
                # Place an initial stop loss at the low of the entry day (bar).
                # Note: with Backtrader's default execution (market orders fill next bar),
                # "entry day" here refers to the bar in which the buy is executed.
                if bool(self.p.use_stoploss):
                    entry_low = float(self.data.low[0])
                    if entry_low > 0:
                        if self.stop_order is not None:
                            try:
                                self.cancel(self.stop_order)
                            except Exception:
                                pass
                        self.stop_order = self.sell(
                            exectype=bt.Order.Stop,
                            price=entry_low,
                            size=order.executed.size,
                        )
                        self.log(f"STOP placed at {entry_low:.2f}")
            else:
                self.log(
                    f"SELL price={order.executed.price:.2f} size={order.executed.size}"
                )
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f"ORDER {order.getstatusname()}")

        # Clear references once the main order completes/fails
        if order is self.order:
            self.order = None
        if order is self.stop_order and order.status in [order.Completed, order.Canceled, order.Rejected]:
            self.stop_order = None

    def next(self) -> None:
        if self.order:
            return

        if not self.position:
            # Entry: SMA cross up + relative volume filter
            if (self.crossover > 0) and (self.rvol[0] > 1.5):
                self.order = self.buy()
        else:
            if self.crossover < 0:
                # If we're exiting due to signal, cancel the protective stop first.
                if self.stop_order is not None:
                    try:
                        self.cancel(self.stop_order)
                    except Exception:
                        pass
                    self.stop_order = None
                self.order = self.close()


def _extract_df(data: Any) -> pd.DataFrame:
    """
    Accepts either:
    - pd.DataFrame
    - SymbolData (from market_data/Symbol_Data.py) or any object with .df
    """

    if isinstance(data, pd.DataFrame):
        return data
    df = getattr(data, "df", None)
    if isinstance(df, pd.DataFrame):
        return df
    raise TypeError(
        "Expected a pandas DataFrame or an object with a .df pandas DataFrame."
    )


def _normalize_ohlcv_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make the DataFrame compatible with Backtrader PandasData:
    - ensure datetime index
    - ensure required columns exist (case-insensitive mapping)
    - sort ascending, de-duplicate index, drop NaNs in required columns
    """

    if df is None or len(df) == 0:
        raise ValueError("OHLCV DataFrame is empty.")

    out = df.copy()

    # If the index isn't datetime but a Date-like column exists, use it.
    if not pd.api.types.is_datetime64_any_dtype(out.index):
        for candidate in ("Date", "date", "Datetime", "datetime", "Timestamp", "timestamp"):
            if candidate in out.columns:
                out[candidate] = pd.to_datetime(out[candidate], errors="coerce")
                out = out.set_index(candidate)
                break

    if not pd.api.types.is_datetime64_any_dtype(out.index):
        raise ValueError(
            "DataFrame index must be datetime (or include a Date/Timestamp column)."
        )

    # Backtrader expects naive datetimes.
    if getattr(out.index, "tz", None) is not None:
        out.index = out.index.tz_convert(None)

    # Case-insensitive column mapping
    required = ["Open", "High", "Low", "Close", "Volume"]
    col_map: dict[str, str] = {}
    lower_cols = {c.lower(): c for c in out.columns}
    for req in required:
        if req in out.columns:
            continue
        alt = lower_cols.get(req.lower())
        if alt:
            col_map[alt] = req
    if col_map:
        out = out.rename(columns=col_map)

    missing = [c for c in required if c not in out.columns]
    if missing:
        raise KeyError(
            f"Missing required OHLCV columns {missing}. "
            f"Available columns: {list(out.columns)}"
        )

    # Ensure RVol exists for the strategy filter.
    if "RVol" not in out.columns:
        avgv20 = out["Volume"].rolling(window=20).mean()
        out["RVol"] = (out["Volume"] / avgv20).replace([pd.NA, float("inf")], 0).fillna(0)

    out = out.sort_index()
    out = out[~out.index.duplicated(keep="last")]
    out = out.dropna(subset=required)
    return out


@dataclass(slots=True)
class BacktestResult:
    symbol: str
    start_value: float
    end_value: float
    pnl: float
    pnl_pct: float
    sharpe: Optional[float]
    max_drawdown_pct: Optional[float]
    trades: Optional[dict]


def run_sma_crossover_backtest(
    data: Any,
    *,
    symbol: Optional[str] = None,
    cash: float = 10_000.0,
    commission: float = 0.0,
    stake: int = 1,
    fromdate: Optional[pd.Timestamp] = None,
    todate: Optional[pd.Timestamp] = None,
    plot: bool = False,
    printlog: bool = False,
    ) -> BacktestResult:
    """
    Run a simple SMA(10/20) crossover backtest with Backtrader.

    Args:
        data: pd.DataFrame or SymbolData-like object with `.df`
        symbol: optional symbol label for reporting
        cash: starting cash
        commission: per-trade commission (e.g. 0.001 = 0.1%)
        stake: shares per buy (FixedSize sizer)
        fromdate/todate: optional date bounds (inclusive-ish; Backtrader uses datetime range)
        plot: if True, show Backtrader plot (requires matplotlib)
        printlog: if True, prints buy/sell fills
    """

    df = _normalize_ohlcv_df(_extract_df(data))

    name = (
        symbol
        or getattr(data, "symbol", None)
        or getattr(data, "ticker", None)
        or "DATA"
    )

    cerebro = bt.Cerebro()
    cerebro.broker.setcash(float(cash))
    cerebro.broker.setcommission(commission=float(commission))

    # Size positions by fixed shares.
    cerebro.addsizer(bt.sizers.FixedSize, stake=int(stake))

    # Data feed (daily)
    feed = PandasDailyOHLCV(
        dataname=df,
        fromdate=pd.to_datetime(fromdate).to_pydatetime() if fromdate is not None else None,
        todate=pd.to_datetime(todate).to_pydatetime() if todate is not None else None,
    )
    cerebro.adddata(feed, name=str(name))

    # Strategy
    cerebro.addstrategy(SmaCrossDaily, fast=10, slow=20, printlog=bool(printlog))

    # Analyzers (optional but helpful)
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe", timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trades")

    start_value = float(cerebro.broker.getvalue())
    results = cerebro.run()
    strat = results[0]
    end_value = float(cerebro.broker.getvalue())

    sharpe = None
    try:
        sharpe = strat.analyzers.sharpe.get_analysis().get("sharperatio", None)
    except Exception:
        sharpe = None

    max_dd = None
    try:
        max_dd = strat.analyzers.drawdown.get_analysis().get("max", {}).get("drawdown", None)
    except Exception:
        max_dd = None

    trades = None
    try:
        trades = strat.analyzers.trades.get_analysis()
    except Exception:
        trades = None

    if plot:
        cerebro.plot(style="candlestick")

    pnl = end_value - start_value
    pnl_pct = (pnl / start_value) * 100 if start_value else 0.0

    return BacktestResult(
        symbol=str(name),
        start_value=start_value,
        end_value=end_value,
        pnl=pnl,
        pnl_pct=pnl_pct,
        sharpe=sharpe,
        max_drawdown_pct=max_dd,
        trades=trades,
    )


if __name__ == "__main__":  # pragma: no cover
    # Example usage (adapt to your environment):
    #
    # from market_data.price_data_import import api_import
    # data = api_import(["AAPL"], from_date="2024-01-01")
    # result = run_sma_crossover_backtest(data["AAPL"], symbol="AAPL", cash=10000, stake=10, commission=0.001)
    # print(result)
    #
    # If you already have SymbolData objects:
    # result = run_sma_crossover_backtest(symbols["AAPL"], cash=10000, stake=10)
    # print(result)
    pass
