from market_data import dataclass, field
from market_data import pd
from typing import List

@dataclass(slots=True)
class SymbolData:
    """
    Store per-symbol tabular market data together with lightweight metadata.

    This dataclass wraps a symbol identifier, a pandas DataFrame, and several
    optional descriptive fields used elsewhere in the codebase. Attribute lookup
    falls back to DataFrame columns through `__getattr__`, so dataclass field
    names should not conflict with column names in `df`.

    Parameters
    ----------
    symbol : str
        Symbol identifier associated with `df`.
    df : pd.DataFrame
        Tabular data for the symbol. Column names are used by `__getattr__`
        and `__getitem__` for convenience access. The class stores the same
        DataFrame object that is passed in.
    sector : str or None, default None
        Optional sector label.
    industry : str or None, default None
        Optional industry label.
    market_cap : float or None, default None
        Optional market capitalization value.
    interest_factor : list[str] or None, default None
        Optional list of factors associated with the symbol.
    interest_direction : str or None, default None
        Optional direction label associated with `interest_factor`.
    interest_source : list[str], default empty list
        List of source labels for the interest metadata. A new empty list is
        created for each instance.
    theme : str or None, default None
        Optional theme label.
    """
    symbol: str
    df: pd.DataFrame
    #Sector and market cap represent the categorical variables that will be added later.
    #*When adding attributes check for conflicts with the dataframe columns. The conflicts are typically due to the use of a shortcut for attribute access(__getattr__).
    sector: str | None = None
    industry: str | None = None
    market_cap: float | None = None
    interest_factor: List[str] | None = None
    interest_direction: str | None = None
    interest_source: List[str] = field(default_factory=list)
    theme: str | None = None


    # convenience proxy for attribute access
    def __getattr__(self, item):          # let s.close mean s.df["close"]
        """
        Return a DataFrame column when normal attribute lookup fails.

        This method is only called after standard attribute resolution does not
        find `item` on the instance. It reads `df` via `object.__getattribute__`
        to avoid recursive lookup during initialization or unpickling, then
        returns `self.df[item]` when `item` matches a column name.

        Parameters
        ----------
        item : str
            Missing attribute name to resolve against `self.df.columns`.

        Returns
        -------
        object
            Whatever `self.df[item]` returns for the matching column label,
            typically a `pd.Series`.

        Raises
        ------
        AttributeError
            Raised when `df` is not yet available or when `item` does not match
            a column in `self.df`.
        """
        # Try to get df without triggering __getattr__ again
        try:
            df = object.__getattribute__(self, "df")
        except AttributeError:
            # df doesn't exist yet; behave like normal missing attribute
            raise AttributeError(item)        
        if item in self.df.columns:
            return self.df[item]
        raise AttributeError(item)

    # convenience proxy for subscript access
    def __getitem__(self, key):           # let s['close'] mean s.df['close']
        """
        Delegate bracket access to the underlying DataFrame.

        This convenience method forwards `key` directly to `self.df[key]` and
        returns the pandas result unchanged. The method does not copy `self.df`
        or modify the stored DataFrame.

        Parameters
        ----------
        key : object
            Column label, list-like column selector, boolean mask, or other
            key accepted by `pd.DataFrame.__getitem__`.

        Returns
        -------
        object
            Whatever `self.df[key]` returns for the provided key, typically a
            `pd.Series` for a single column or a `pd.DataFrame` for multi-column
            selection.
        """
        return self.df[key]
    
    def to_redis(self) -> dict:
        """
        Serialize the instance into a dictionary of JSON-friendly values.

        This method copies `self.df`, resets the index into the first output
        column, converts datetime-like columns to strings formatted as
        `%Y-%m-%d %H:%M:%S`, and returns a plain dictionary suitable for storage
        in a Redis-backed JSON payload. The original DataFrame is left unchanged.

        Returns
        -------
        dict
            Mapping with keys `symbol`, `df`, `sector`, `industry`,
            `market_cap`, `interest_factor`, `interest_direction`, and `theme`.
            The `df` value is the result of `df_copy.to_dict()` after index reset
            and datetime string conversion.

        Notes
        -----
        The returned payload does not include `interest_source`.
        """
        # Convert datetime columns to strings for JSON serialization
        df_copy = self.df.reset_index().copy()
        for col in df_copy.columns:
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        return {
            "symbol": self.symbol,
            "df": df_copy.to_dict(),
            "sector": self.sector,
            "industry": self.industry,
            "market_cap": self.market_cap,
            "interest_factor": self.interest_factor,
            "interest_direction": self.interest_direction,
            "theme": self.theme,
        }

    @classmethod
    def from_redis(cls, payload: dict):
        """
        Reconstruct a `SymbolData` instance from a serialized payload.

        This classmethod rebuilds a DataFrame from `payload["df"]`, attempts to
        parse string-valued columns as datetimes using the exact format
        `%Y-%m-%d %H:%M:%S`, restores the first column as the index, and passes
        the remaining metadata fields into the dataclass constructor.

        Parameters
        ----------
        payload : dict
            Mapping produced by `to_redis()` with keys `symbol`, `df`, `sector`,
            `industry`, `market_cap`, `interest_factor`, `interest_direction`,
            and `theme`. The `df` entry must be compatible with
            `pd.DataFrame(payload["df"])`.

        Returns
        -------
        SymbolData
            New instance populated from the serialized payload.

        Notes
        -----
        Any string column that does not parse with the exact datetime format is
        left unchanged. The method restores the index from the first column of
        the reconstructed DataFrame, regardless of that column's name.
        `interest_source` is not read from `payload` and therefore falls back to
        the dataclass default empty list on the returned instance.
        """
        df = pd.DataFrame(payload["df"])
        # Convert string datetime columns back to datetime
        for col in df.columns:
            if pd.api.types.is_string_dtype(df[col]):
                try:
                    # Try to parse as datetime if it looks like a datetime string
                    pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S', errors='raise')
                    df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S')
                except (ValueError, TypeError):
                    pass  # Not a datetime column, leave as string

        # Set the first column (which was the index) back as the index
        df = df.set_index(df.columns[0])
        return cls(
            symbol=payload["symbol"],
            df=df,
            sector=payload["sector"],
            industry=payload["industry"],
            market_cap=payload["market_cap"],
            interest_factor=payload["interest_factor"],
            interest_direction=payload["interest_direction"],
            theme=payload["theme"],
        )



@dataclass(slots=True)
class Intraday_SymbolData:
    """
    Store intraday symbol data and related derived metrics.

    This dataclass is a lightweight container for an intraday DataFrame plus a
    small set of optional metadata and summary objects. The class defines fields 
    only and does not add custom accessors or serialization logic.

    Parameters
    ----------
    symbol : str
        Symbol identifier associated with `df`.
    df : pd.DataFrame
        Intraday data stored on the instance without copying.
    sector : str or None, default None
        Optional sector label.
    market_cap : float or None, default None
        Optional market capitalization value.
    rvol_df : pd.Series or None, default None
        Optional relative-volume series associated with the symbol.
    daily_loi : dict[str, float] or None, default None
        Optional mapping of daily metrics keyed by string labels.
    """
    symbol: str
    df: pd.DataFrame
    sector: str | None = None
    market_cap: float | None = None
    rvol_df: pd.Series | None = None
    daily_loi: dict[str, float] | None = None



def full_report():
    """
    Act as a placeholder for a future report-building routine.

    The function currently contains only outline comments and a `pass`
    statement. Calling the function performs no computation and returns `None`.

    Returns
    -------
    None
        Always returned because the function has no implemented body.
    """
    #* The idea for this function may be better executed as an LLM prompt with RAG access to my code.
    #Business Overview
    ##Product/Revenue Stream
    
    #News
    ##Narratives
    
    #Analyses
    ##Compare/Contrast
    
    
    #Fundamentals
    
    
    #Technicals
    pass