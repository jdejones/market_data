from market_data import datetime, Union, pd
from market_data.add_technicals import AVWAP_by_date


def vwap_handoff(df: pd.DataFrame, bias: str = 'long') -> pd.DataFrame:
    """
    The handoffs may not be equal to handoffs in TC2000 because the price data
    from polygon.io is limited to 10 years and a weekly timeframe has to be
    applied in TC2000.
    Applies a sequence of anchored VWAPs based on directional bias.

    Args:
        df (pd.DataFrame): Time series with ['Open','High','Low','Close','Volume'].
        bias (str): 'long' for bullish (anchor at min low), 'short' for bearish (anchor at max high).

    Returns:
        pd.DataFrame: Original df with added VWAP columns per anchor date.
    """    
    if bias not in ('long','short'):
        raise ValueError("bias must be 'long' or 'short'")
    
    # 1. Initial anchor -------------------------
    anchor = df['Low'].idxmin() if bias=='long' else df['High'].idxmax()
    df = AVWAP_by_date(df, anchor)
    anchors = [anchor]
    init_col = f"VWAP {anchor}"
    
    # 2. Structural bias check ------------------
    if (bias=='long' and df['Close'].iloc[-1] < df[init_col].iloc[-1]) or \
       (bias=='short' and df['Close'].iloc[-1] > df[init_col].iloc[-1]):
        return df
    
    loop_count = 0
    # 3. Loop for subsequent handoffs ----------
    while True:
        prev = anchors[-1]
        col_prev = f"VWAP {prev}"
        segment = df.loc[df.index >= prev]

        # 3a. Detect breach
        mask = (segment['Low'] < segment[col_prev]) if bias == 'long' else (segment['High'] > segment[col_prev])
        
        # 3b. No breach case with “Additional Consideration”
        if (not mask.any()) or (not mask[1:].any()):
            # If last anchor is older than 20 bars ago, keep anchoring until first breach
            window_start = df.index[-20]
            if prev < window_start:
                # keep trying: find next candidate low/high in segment
                # we step forward one bar at a time until we find a breach
                for idx in segment.index:
                    # re-anchor at each new bar to see if breach occurs there
                    df = AVWAP_by_date(df, idx)
                    anchors.append(idx)
                    col_curr = f"VWAP {idx}"
                    # check if this new VWAP has a breach in the remaining bars
                    rem = df.loc[df.index > idx]
                    breach_rem = ((rem['Low'] < rem[col_curr]) if bias == 'long'
                                  else (rem['High'] > rem[col_curr]))
                    if breach_rem.any():
                        # once we’ve found a true breach, break out of this inner loop
                        break
                # now continue outer loop to pick up with new mask on updated VWAP
                continue
            else:
                # within 20 bars and no breach → stop
                break

        # 3c. Normal handoff when breach exists
        handoff = mask[mask].index[-1]
        df = AVWAP_by_date(df, handoff)
        anchors.append(handoff)
        col_curr = f"VWAP {handoff}"

        # 4. Discontinuation checks ---------------

        # 4a. Clears opposite extreme
        if (bias=='long' and df[col_curr].iloc[-1] > df['Low'].iloc[-1]) or \
           (bias=='short' and df[col_curr].iloc[-1] < df['High'].iloc[-1]):
            break

        # 4b. Too many recent handoffs
        threshold = df.index[-20]
        recent = [d for d in anchors[1:] if d >= threshold]
        if len(recent) > 5:
            # cleanup: drop all but the initial VWAP
            for d in anchors[1:]:
                col = f"VWAP {d}"
                if col in df.columns: 
                    df.drop(columns=[col], inplace=True)
            break
    # Rename VWAP columns to use only the date portion
    for anchor in anchors:
        old_col = f"VWAP {anchor}"
        if old_col in df.columns:
            new_col = f"VWAP {anchor.date()}"
            df.rename(columns={old_col: new_col}, inplace=True)
    return df

#Make filters using the results of vwap_handoff



#Make function for vwap pinch