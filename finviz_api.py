from io import StringIO
from typing import Iterable

import pandas as pd
import requests

from market_data.api_keys import finviz_api_key


FINVIZ_SCREENER_EXPORT_URL = "https://elite.finviz.com/export/screener"
FINVIZ_ALL_COLUMN_IDS = (*range(71), *range(73, 89))


def screener_export(
    *,
    view: int = 111,
    filters: str | None = None,
    signal: str | None = None,
    columns: Iterable[int] | None = None,
    timeout: float = 30,
) -> pd.DataFrame:
    """Return an authenticated Finviz Elite screener export."""
    params = {
        "v": view,
        "auth": finviz_api_key,
    }
    if filters:
        params["f"] = filters
    if signal:
        params["s"] = signal
    if columns is not None:
        params["c"] = ",".join(str(column) for column in columns)

    response = requests.get(
        FINVIZ_SCREENER_EXPORT_URL,
        params=params,
        timeout=timeout,
    )
    try:
        response.raise_for_status()
    except requests.HTTPError:
        raise requests.HTTPError(
            f"Finviz export failed with HTTP {response.status_code}."
        ) from None

    result = pd.read_csv(StringIO(response.text))
    if "Ticker" not in result.columns:
        raise ValueError("The Finviz response does not contain a Ticker column.")
    return result


def all_screener_data(timeout: float = 30) -> pd.DataFrame:
    """Return all available custom screener columns."""
    return screener_export(
        view=152,
        columns=FINVIZ_ALL_COLUMN_IDS,
        timeout=timeout,
    )
