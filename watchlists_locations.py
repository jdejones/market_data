
def make_watchlist(file_path: str) -> list[str]:
    with open(file_path, 'r') as f:
        return [line.strip() for line in f.readlines()]

hadv = "E:\Market Research\Studies\Sector Studies\Watchlists\High_AvgDV.txt"








#I'm not sure if I need these.
systematic_watchlists_root = r"C:\Users\jdejo\OneDrive\Documents\Python_Folders\Systematic Watchlists"
accumulation = systematic_watchlists_root + r"\accumulation.txt"

accumulation_breakout = systematic_watchlists_root + r"\accumulation_breakout.txt"

active_volatile_quadrant1 = systematic_watchlists_root + r"\active_volatile_quadrant1.txt"

alerts_10dma_pullback = systematic_watchlists_root + r"\alerts_10dma_pullback.txt"

alerts_20dma_pullback = systematic_watchlists_root + r"\alerts_20dma_pullback.txt"

alerts_50dma_pullback = systematic_watchlists_root + r"\alerts_50dma_pullback.txt"

alerts_ep_price_below_gap_avwap = systematic_watchlists_root + r"\alerts_ep_price_below_gap_avwap.txt"

buyer_capitulation = systematic_watchlists_root + r"\buyer_capitulation.txt"

distribution = systematic_watchlists_root + r"\distribution.txt"

distribution_breakdown = systematic_watchlists_root + r"\distribution_breakdown.txt"

downtrend = systematic_watchlists_root + r"\downtrend.txt"

downtrend_distribution = systematic_watchlists_root + r"\downtrend_distribution.txt"

downtrend_retracement = systematic_watchlists_root + r"\downtrend_retracement.txt"

earnings_beats = systematic_watchlists_root + r"\earnings_beats.txt"

earnings_misses = systematic_watchlists_root + r"\earnings_misses.txt"

episodic_pivots = systematic_watchlists_root + r"\episodic_pivots.txt"

episodic_pivots_quant_over_4 = systematic_watchlists_root + r"\episodic_pivots_quant_over_4.txt"
 
Fanned_Basic_Materials = systematic_watchlists_root + r"\Fanned_Basic_Materials.txt"
 
Fanned_Communication_Services = systematic_watchlists_root + r"\Fanned_Communication_Services.txt"

Fanned_Consumer_Cyclical = systematic_watchlists_root + r"\Fanned_Consumer_Cyclical.txt"

Fanned_Consumer_Defensive = systematic_watchlists_root + r"\Fanned_Consumer_Defensive.txt"

Fanned_Energy = systematic_watchlists_root + r"\Fanned_Energy.txt"

Fanned_Financial_Services = systematic_watchlists_root + r"\Fanned_Financial_Services.txt"

Fanned_Healthcare = systematic_watchlists_root + r"\Fanned_Healthcare.txt"

Fanned_Industrials = systematic_watchlists_root + r"\Fanned_Industrials.txt"

Fanned_Real_Estate = systematic_watchlists_root + r"\Fanned_Real Estate.txt"

Fanned_Technology = systematic_watchlists_root + r"\Fanned_Technology.txt"

Fanned_Up_Stored = systematic_watchlists_root + r"\Fanned_Up_Stored.txt"

Fanned_Utilities = systematic_watchlists_root + r"\Fanned_Utilities.txt"

finviz_high_p_e = systematic_watchlists_root + r"\finviz_high_p_e.txt"

finviz_high_short_interest = systematic_watchlists_root + r"\finviz_high_short_interest.txt"

finviz_horizontal_s_r = systematic_watchlists_root + r"\finviz_horizontal_s_r.txt"

finviz_tl_resistance = systematic_watchlists_root + r"\finviz_tl_resistance.txt"

finviz_tl_support = systematic_watchlists_root + r"\finviz_tl_support.txt"

general_long = systematic_watchlists_root + r"\general_long.txt"

high_quant = systematic_watchlists_root + r"\high_quant.txt"

high_short_interest = systematic_watchlists_root + r"\high_short_interest.txt"

high_short_interest_price_above_target = systematic_watchlists_root + r"\high_short_interest_price_above_target.txt"

high_short_interest_scraper = systematic_watchlists_root + r"\high_short_interest_scraper.txt"

long_list = systematic_watchlists_root + r"\long_list.txt"

long_list_quant_over_4 = systematic_watchlists_root + r"\long_list_quant_over_4.txt"

low_quant = systematic_watchlists_root + r"\low_quant.txt"

no_income = systematic_watchlists_root + r"\no_income.txt"

no_income_gapped = systematic_watchlists_root + r"\no_income_gapped.txt"

no_income_price_above_high = systematic_watchlists_root + r"\no_income_price_above_high.txt"

no_income_price_above_target = systematic_watchlists_root + r"\no_income_price_above_target.txt"

overlap10 = systematic_watchlists_root + r"\overlap10.txt"

overlap10_long = systematic_watchlists_root + r"\overlap10_long.txt"

overlap10_short = systematic_watchlists_root + r"\overlap10_short.txt"

overlap20 = systematic_watchlists_root + r"\overlap20.txt"

overlap20_long = systematic_watchlists_root + r"\overlap20_long.txt"

overlap20_short = systematic_watchlists_root + r"\overlap20_short.txt"

overlap50 = systematic_watchlists_root + r"\overlap50.txt"

overlap50_long = systematic_watchlists_root + r"\overlap50_long.txt"

overlap50_short = systematic_watchlists_root + r"\overlap50_short.txt"

price_above_high = systematic_watchlists_root + r"\price_above_high.txt"

price_above_target = systematic_watchlists_root + r"\price_above_target.txt"

relative_strength = systematic_watchlists_root + r"\relative_strength.txt"

relative_strength_tc2000_favorites = systematic_watchlists_root + r"\relative_strength_tc2000_favorites.txt"

relative_weakness_tc2000_favorites = systematic_watchlists_root + r"\relative_weakness_tc2000_favorites.txt"

seller_capitulation = systematic_watchlists_root + r"\seller_capitulation.txt"

short_list = systematic_watchlists_root + r"\short_list.txt"

short_list_quant_under_1half = systematic_watchlists_root + r"\short_list_quant_under_1half.txt"

technical_long_list = systematic_watchlists_root + r"\technical_long_list.txt"

technical_long_list_quant_over_4 = systematic_watchlists_root + r"\technical_long_list_quant_over_4.txt"

technical_short_list = systematic_watchlists_root + r"\technical_short_list.txt"

technical_short_list_quant_under_1half = systematic_watchlists_root + r"\technical_short_list_quant_under_1half.txt"

tss_depressed = systematic_watchlists_root + r"\tss_depressed.txt"

tss_elevated = systematic_watchlists_root + r"\tss_elevated.txt"

uptrend = systematic_watchlists_root + r"\uptrend.txt"

uptrend_accumulation = systematic_watchlists_root + r"\uptrend_accumulation.txt"

uptrend_retracement = systematic_watchlists_root + r"\uptrend_retracement.txt"