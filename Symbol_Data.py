from market_data import dataclass
# import pandas as pd
from market_data import pd

@dataclass(slots=True)
class SymbolData:
    symbol: str
    df: pd.DataFrame
    #Sector and market cap represent the categorical variables that will be added later.
    sector: str | None = None
    market_cap: float | None = None
    interest_factor: str | None = None
    theme: str | None = None

    # convenience proxy
    def __getattr__(self, item):          # let s.close mean s.df["close"]
        if item in self.df.columns:
            return self.df[item]
        raise AttributeError(item)



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