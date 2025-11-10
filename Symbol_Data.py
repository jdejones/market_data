from market_data import dataclass
# import pandas as pd
from market_data import pd

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
    theme: str | None = None


    # convenience proxy for attribute access
    def __getattr__(self, item):          # let s.close mean s.df["close"]
        if item in self.df.columns:
            return self.df[item]
        raise AttributeError(item)

    # convenience proxy for subscript access
    def __getitem__(self, key):           # let s['close'] mean s.df['close']
        if key in self.df.columns:
            return self.df[key]
        raise KeyError(key)



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