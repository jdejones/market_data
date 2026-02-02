"""Helpers for running strategies with the backtesting library."""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, Type

import pandas as pd
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from backtesting.test import SMA


REQUIRED_OHLCV_COLUMNS = ("Open", "High", "Low", "Close", "Volume")


def normalize_ohlcv(
    df: pd.DataFrame, column_map: Optional[Dict[str, str]] = None
) -> pd.DataFrame:
    """Return a DataFrame with the OHLCV columns required by backtesting.

    Args:
        df: Source data.
        column_map: Optional mapping of existing column names to the required
            OHLCV column names. Example: {"open": "Open", "close": "Close"}.
    """
    if column_map:
        df = df.rename(columns=column_map)
    else:
        lower_map = {col.lower(): col for col in df.columns}
        rename: Dict[str, str] = {}
        for col in ("open", "high", "low", "close", "volume"):
            if col in lower_map:
                rename[lower_map[col]] = col.title()
        df = df.rename(columns=rename)

    missing = [col for col in REQUIRED_OHLCV_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            "Missing required OHLCV columns after normalization: "
            f"{', '.join(missing)}"
        )

    return df[list(REQUIRED_OHLCV_COLUMNS)].copy()


class SmaCross(Strategy):
    """Simple SMA crossover example strategy."""

    n1 = 10
    n2 = 20

    def init(self) -> None:
        close = self.data.Close
        self.sma1 = self.I(SMA, close, self.n1)
        self.sma2 = self.I(SMA, close, self.n2)

    def next(self) -> None:
        if crossover(self.sma1, self.sma2):
            self.buy()
        elif crossover(self.sma2, self.sma1):
            self.sell()


def run_backtest(
    df: pd.DataFrame,
    strategy: Type[Strategy] = SmaCross,
    cash: float = 10_000,
    commission: float = 0.0,
    column_map: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> Tuple[Backtest, pd.Series]:
    """Run a backtest and return the Backtest instance plus stats.

    Args:
        df: Input OHLCV data.
        strategy: Backtesting Strategy class.
        cash: Starting cash.
        commission: Commission percentage (0.002 = 0.2%).
        column_map: Optional mapping for OHLCV columns.
        **kwargs: Extra Backtest keyword arguments (e.g., exclusive_orders).
    """
    data = normalize_ohlcv(df, column_map=column_map)
    backtest = Backtest(data, strategy, cash=cash, commission=commission, **kwargs)
    stats = backtest.run()
    return backtest, stats
