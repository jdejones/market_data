from market_data import dataclass, field
from market_data import pd
from typing import List

@dataclass(slots=True)
class SymbolData:
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
        Let s.close mean s.df["close"], but avoid recursion during unpickling
        or when df is not yet set.
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
        return self.df[key]
    
    def to_redis(self) -> dict:
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
    symbol: str
    df: pd.DataFrame
    sector: str | None = None
    market_cap: float | None = None
    rvol_df: pd.Series | None = None
    daily_loi: dict[str, float] | None = None



def full_report():
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