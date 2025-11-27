import sys
sys.path.insert(0, r"C:\Users\jdejo\Market_Data_Processing")

import datetime as dt
from threading import Lock, Thread
from typing import Dict, List, Optional

import pandas as pd
import requests
from market_data.api_keys import polygon_api_key
from polygon.websocket import WebSocketClient
from polygon.websocket.models import Feed, Market, WebSocketMessage

import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go

message_count = 0
# =============================================================================
# Configuration / shared state
# =============================================================================

# Default starting symbol (can be changed at runtime from the dashboard)
SYMBOL = ""

# Currently watched symbol (updated via Dash UI)
current_symbol: str = SYMBOL
symbol_lock = Lock()

# Global websocket client so Dash can subscribe to new symbols on demand
ws_client: Optional[WebSocketClient] = None

# Track which symbols have already been backfilled this session
backfilled_symbols: set[str] = set()
backfill_lock = Lock()

# Market hours in US/Eastern
MARKET_OPEN = dt.time(9, 30)
MARKET_CLOSE = dt.time(16, 0)

# Default history window (in seconds) to keep in memory for the dashboard
HISTORY_SECONDS = int(6.5 * 60 * 60)
# Mutable history window that can be changed from the dashboard
history_seconds = HISTORY_SECONDS


# =============================================================================
# In‑memory state for per‑second aggregates
# =============================================================================

# Each key is a Python datetime (naive, treated as US/Eastern) rounded to the
# nearest second. Each value is a small dict of aggregates for that second.
#
# {
#   ts: {
#       "value_1_1000": float,
#       "value_1001_10000": float,
#       "value_10000_plus": float,
#       "trades": int,
#   },
#   ...
# }
per_second_data: Dict[dt.datetime, Dict[str, float]] = {}
data_lock = Lock()


def _is_market_hours(ts_eastern: dt.datetime) -> bool:
    """Return True if the timestamp (US/Eastern) is within regular market hours."""
    t = ts_eastern.time()
    return (t >= MARKET_OPEN) and (t <= MARKET_CLOSE)


def _prune_old_data(now_ts: dt.datetime) -> None:
    """Remove entries older than HISTORY_SECONDS relative to now_ts."""
    cutoff = now_ts - dt.timedelta(seconds=history_seconds)
    old_keys = [k for k in per_second_data.keys() if k < cutoff]
    for k in old_keys:
        per_second_data.pop(k, None)


def _update_aggregates(ts_eastern: dt.datetime, trade_value: float) -> None:
    """
    Update per‑second aggregates for a trade that occurred at ts_eastern
    with a given trade_value (price * size).
    """
    # Round to the nearest second (drop microseconds)
    ts_second = ts_eastern.replace(microsecond=0, tzinfo=None)

    with data_lock:
        metrics = per_second_data.get(
            ts_second,
            {
                "value_1_1000": 0.0,
                "value_1001_10000": 0.0,
                "value_10001_100000": 0.0,
                "value_100000_plus": 0.0,
                "trades": 0,
            },
        )

        # Bucket the trade value
        if 1 <= trade_value <= 1_000:
            metrics["value_1_1000"] += trade_value
        elif 1_001 <= trade_value <= 10_000:
            metrics["value_1001_10000"] += trade_value
        elif 10_001 <= trade_value <= 100_000:
            metrics["value_10001_100000"] += trade_value
        elif trade_value > 100_000:
            metrics["value_100000_plus"] += trade_value

        metrics["trades"] += 1
        per_second_data[ts_second] = metrics

        # Prune old history
        _prune_old_data(ts_second)


def _backfill_trades_for_symbol(symbol: str, first_ts_eastern: pd.Timestamp) -> None:
    """
    Backfill trades for `symbol` from the start of today's market session
    (MARKET_OPEN) up to just before `first_ts_eastern`, and feed them through
    the same per-second aggregation logic used for live trades.
    """
    # Ensure we are working with a tz-aware Eastern timestamp
    if first_ts_eastern.tzinfo is None:
        first_ts_eastern = first_ts_eastern.tz_localize("US/Eastern")
    else:
        first_ts_eastern = first_ts_eastern.tz_convert("US/Eastern")

    day = first_ts_eastern.date()
    start_dt = dt.datetime.combine(day, MARKET_OPEN)
    start_ts_eastern = pd.Timestamp(start_dt, tz="US/Eastern")

    print(
        f"Backfill starting for {symbol}: "
        f"{start_ts_eastern} -> {first_ts_eastern} (US/Eastern)"
    )

    # If the first tick is at or before the market open, nothing to backfill
    if first_ts_eastern <= start_ts_eastern:
        return

    # Convert to UTC nanoseconds for Polygon's /v3/trades endpoint
    start_utc = start_ts_eastern.tz_convert("UTC")
    end_utc = first_ts_eastern.tz_convert("UTC")
    start_ns = int(start_utc.timestamp() * 1_000_000_000)
    end_ns = int(end_utc.timestamp() * 1_000_000_000)

    url = f"https://api.polygon.io/v3/trades/{symbol}"
    params = {
        "timestamp.gte": start_ns,
        "timestamp.lt": end_ns,  # strictly before the first websocket trade
        "order": "asc",
        "limit": 50000,
        "apiKey": polygon_api_key,
    }

    total = 0
    while True:
        try:
            resp = requests.get(url, params=params, timeout=10)
        except Exception as e:
            print(f"Backfill HTTP error for {symbol}: {e}")
            break

        if resp.status_code != 200:
            print(f"Backfill error {symbol}: {resp.status_code} {resp.text[:200]}")
            break

        data = resp.json()
        results = data.get("results", [])
        if not results:
            if total == 0:
                print(f"Backfill: no trades returned for {symbol} in window.")
            break

        # Print a sample trade structure once for debugging so we can confirm
        # the field names coming back from Polygon.
        if total == 0:
            print(f"Backfill sample trade for {symbol}:", results[0])

        for t in results:
            # v3/trades returns fields like 'price', 'size', and timestamp fields
            # such as 'sip_timestamp' and 'participant_timestamp' in nanoseconds.
            price = t.get("price") or t.get("p")
            size = t.get("size") or t.get("s") or t.get("q")
            ts_ns = (
                t.get("sip_timestamp")
                or t.get("participant_timestamp")
                or t.get("t")
                or t.get("timestamp")
            )

            if price is None or size is None or ts_ns is None:
                continue

            ts = (
                pd.to_datetime(ts_ns, unit="ns", utc=True)
                .tz_convert("US/Eastern")
            )
            if not _is_market_hours(ts):
                continue

            trade_value = float(price) * float(size)
            if trade_value <= 0:
                continue

            _update_aggregates(ts, trade_value)
            total += 1

        next_url = data.get("next_url")
        if not next_url:
            break

        # next_url already contains the appropriate query parameters;
        # we only need to provide the API key.
        url = next_url
        params = {"apiKey": polygon_api_key}

    if total > 0:
        print(f"Backfill completed for {symbol}: {total} trades aggregated.")


def handle_trades(msgs: List[WebSocketMessage]) -> None:
    """
    Polygon websocket callback for trade messages.

    For each trade:
      - compute trade value = price * size
      - bucket the value into:
          * $1‑$1,000
          * $1,001‑$10,000
          * $10,000+
      - increment trades‑per‑second for the corresponding second.
    """

    for m in msgs:
        # Extract fields with fallbacks in case of slight model differences
        symbol = getattr(m, "symbol", None) or getattr(m, "ticker", None)
        # Only keep trades for the currently selected symbol
        with symbol_lock:
            watched = current_symbol
        if symbol is None or symbol != watched:
            continue

        # Polygon trade messages typically expose price & size (or aliases)
        price = getattr(m, "price", None) or getattr(m, "p", None)
        size = getattr(m, "size", None) or getattr(m, "s", None) or getattr(m, "volume", None)

        if price is None or size is None:
            continue

        # Timestamp: try several possible attribute names and convert to US/Eastern
        ts_raw = None
        for attr in (
            "sip_timestamp",
            "participant_timestamp",
            "trade_timestamp",
            "timestamp",
            "t",
            "event_timestamp",
        ):
            ts_raw = getattr(m, attr, None)
            if ts_raw:
                break

        if ts_raw is None:
            # For the first few misses, print the message so we can see its structure.
            if message_count < 10:
                print("No timestamp found on message:", repr(m))
            continue

        # Handle both integer (epoch) and datetime-like timestamps
        if isinstance(ts_raw, (int, float)):
            # Heuristic: ns vs ms (current epoch in ns ~1e18, in ms ~1e12)
            unit = "ns" if ts_raw > 1e15 else "ms"
            ts = pd.to_datetime(ts_raw, unit=unit, utc=True).tz_convert("US/Eastern")
        else:
            ts = pd.to_datetime(ts_raw, utc=True).tz_convert("US/Eastern")

        # Optionally enforce market hours
        if not _is_market_hours(ts):
            continue

        # On the first qualifying trade for this symbol, backfill from
        # MARKET_OPEN up to just before this trade using the REST API.
        do_backfill = False
        with backfill_lock:
            if symbol not in backfilled_symbols:
                backfilled_symbols.add(symbol)
                do_backfill = True
        if do_backfill:
            try:
                _backfill_trades_for_symbol(symbol, ts)
            except Exception as e:
                print(f"Backfill failed for {symbol}: {e}")

        trade_value = float(price) * float(size)
        if trade_value <= 0:
            continue

        _update_aggregates(ts, trade_value)



def run_trade_stream() -> None:
    """Start the Polygon websocket trade stream for the configured symbol."""
    global ws_client
    client = WebSocketClient(
        api_key=polygon_api_key,
        feed=Feed.Delayed,
        market=Market.Stocks,
    )
    ws_client = client

    subscription = f"T.{SYMBOL}"
    client.subscribe(subscription)
    client.run(handle_msg=handle_trades)


# =============================================================================
# Dash dashboard
# =============================================================================

app = dash.Dash(__name__)
app.title = f"Order Flow – {SYMBOL}"


app.layout = html.Div(
    style={"fontFamily": "Arial, sans-serif", "margin": "20px"},
    children=[
        html.H2(f"Order Flow Dashboard – {SYMBOL}"),
        html.Div(
            style={"marginBottom": "10px"},
            children=[
                html.Label("Symbol:", style={"marginRight": "8px"}),
                dcc.Input(
                    id="symbol-input",
                    type="text",
                    value=SYMBOL,
                    style={"width": "100px", "marginRight": "8px"},
                ),
                html.Button("Update", id="symbol-submit", n_clicks=0),
                html.Span(
                    id="symbol-status",
                    style={"marginLeft": "10px", "fontStyle": "italic"},
                ),
            ],
        ),
        html.Div(
            style={"marginBottom": "10px"},
            children=[
                html.Label("History window (minutes):", style={"marginRight": "8px"}),
                dcc.Input(
                    id="history-minutes-input",
                    type="number",
                    value=int(HISTORY_SECONDS / 60),
                    min=10,
                    max=500,
                    step=10,
                    style={"width": "120px", "marginRight": "8px"},
                ),
                html.Button("Apply", id="history-submit", n_clicks=0),
                html.Span(
                    id="history-status",
                    style={"marginLeft": "10px", "fontStyle": "italic"},
                ),
            ],
        ),
        html.Div(
            [
                html.P(
                    "Streaming trade value buckets and trading velocity "
                    "during regular market hours (US/Eastern)."
                )
            ]
        ),
        dcc.Graph(id="value-buckets-chart"),
        dcc.Graph(id="trades-per-second-chart"),
        dcc.Interval(
            id="update-interval",
            interval=1000,  # 1 second
            n_intervals=0,
        ),
    ],
)


def _build_dataframe() -> pd.DataFrame:
    """Return a DataFrame of the current per‑second aggregates."""
    with data_lock:
        if not per_second_data:
            return pd.DataFrame(
                columns=[
                    "timestamp",
                    "value_1_1000",
                    "value_1001_10000",
                    "value_10001_100000",
                    "value_100000_plus",
                    "trades",
                ]
            )

        df = pd.DataFrame.from_dict(per_second_data, orient="index")

    df = df.sort_index()
    df.index.name = "timestamp"
    df = df.reset_index()
    return df


@app.callback(
    Output("value-buckets-chart", "figure"),
    Output("trades-per-second-chart", "figure"),
    Input("update-interval", "n_intervals"),
)
def update_charts(_n: int):
    df = _build_dataframe()

    if df.empty:
        empty_layout = go.Layout(
            xaxis={"title": "Time"},
            yaxis={"title": "Value"},
            margin={"l": 40, "r": 20, "t": 40, "b": 40},
        )
        return go.Figure(layout=empty_layout), go.Figure(layout=empty_layout)

    # Trade value buckets
    fig_value = go.Figure()
    fig_value.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["value_1_1000"],
            mode="lines",
            name="$1–$1,000",
        )
    )
    fig_value.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["value_1001_10000"],
            mode="lines",
            name="$1,001–$10,000",
        )
    )
    fig_value.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["value_10001_100000"],
            mode="lines",
            name="$10,001–$100,000",
        )
    )
    fig_value.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["value_100000_plus"],
            mode="lines",
            name="$100,000+",
        )
    )
    fig_value.update_layout(
        title="Trade Value by Bucket (per second)",
        xaxis_title="Time (US/Eastern)",
        yaxis_title="Trade Value (USD)",
        legend_title="Buckets",
        margin={"l": 40, "r": 20, "t": 40, "b": 40},
    )

    # Trades per second
    fig_trades = go.Figure()
    fig_trades.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["trades"],
            mode="lines",
            name="Trades per Second",
        )
    )
    fig_trades.update_layout(
        title="Trading Velocity (Trades per Second)",
        xaxis_title="Time (US/Eastern)",
        yaxis_title="Trades per Second",
        legend_title="",
        margin={"l": 40, "r": 20, "t": 40, "b": 40},
    )

    return fig_value, fig_trades


@app.callback(
    Output("symbol-status", "children"),
    Input("symbol-submit", "n_clicks"),
    State("symbol-input", "value"),
    prevent_initial_call=True,
)
def update_symbol(_n_clicks: int, value: str):
    """
    Update the currently watched symbol without restarting the script.

    This changes the in-memory filter used by handle_trades and, if the
    websocket client is available, subscribes to the new symbol on the
    existing websocket connection.
    """
    global current_symbol, ws_client
    if not value:
        return "Please enter a symbol."

    new_symbol = value.strip().upper()
    if not new_symbol:
        return "Please enter a symbol."

    # Update the watched symbol used by handle_trades
    with symbol_lock:
        current_symbol = new_symbol

    # Reset in-memory chart data so the selected symbol starts with a clean
    # history on the dashboard (even if re-selecting the same symbol).
    with data_lock:
        per_second_data.clear()

    # Allow backfill to run again for this symbol on the next qualifying trade
    # (so re-selecting the same symbol mimics a "soft restart" for that symbol).
    with backfill_lock:
        if new_symbol in backfilled_symbols:
            backfilled_symbols.remove(new_symbol)

    # Subscribe to the new symbol on the existing websocket connection.
    # We do not unsubscribe the old one to avoid race conditions; handle_trades
    # only processes trades for current_symbol, so extra subscriptions are cheap.
    if ws_client is not None:
        try:
            ws_client.subscribe(f"T.{new_symbol}")
        except Exception as e:
            return f"Watching {new_symbol}, but failed to subscribe on websocket: {e}"

    return f"Now watching {new_symbol} (chart data reset)"


@app.callback(
    Output("history-status", "children"),
    Input("history-submit", "n_clicks"),
    State("history-minutes-input", "value"),
    prevent_initial_call=True,
)
def update_history_window(_n_clicks: int, minutes: float | int | None):
    """
    Update the history window (in minutes) used for pruning per-second data,
    without restarting the script.

    Decreasing the window will immediately prune older data; increasing it will
    allow more history to accumulate going forward (but cannot restore already
    pruned data).
    """
    global history_seconds
    if minutes is None:
        return "Please enter a history window in minutes."

    try:
        minutes_val = float(minutes)
    except (TypeError, ValueError):
        return "Invalid history window."

    if minutes_val <= 0:
        return "History window must be positive."

    new_seconds = int(minutes_val * 60)

    # Update the global window and prune immediately against the most recent
    # timestamp we have, so charts reflect the new window.
    with data_lock:
        history_seconds = new_seconds
        if per_second_data:
            latest_ts = max(per_second_data.keys())
            _prune_old_data(latest_ts)

    return f"History window set to {int(minutes_val)} minutes."


def run_dashboard() -> None:
    """
    Run the Dash dashboard.

    Note: use_reloader=False is critical here; the Flask reloader would start a
    second process and cause run_trade_stream to be invoked twice, which would
    open multiple websocket connections to Polygon.
    """
    # visit http://127.0.0.1:8050 in a browser
    app.run(debug=True, use_reloader=False)


def main():
    """
    Entry point: run the Dash dashboard in a background thread and keep the
    Polygon websocket stream in the main thread so only a single websocket
    connection is opened.
    """
    dash_thread = Thread(target=run_dashboard, daemon=True)
    dash_thread.start()

    # This blocks and maintains a single websocket connection to Polygon.
    run_trade_stream()


if __name__ == "__main__":
    main()


