#
# get historical prices 
#
import pandas as pd
from mkt_data.mkt_timeseries import get_by_tickers
from dashboard.positions import get_positions


def get_hist_prices(tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get historical prices for a list of tickers between start_date and end_date.
    Returns a pandas DataFrame with dates as the index and tickers as columns.
    """
    tickers = ['SPY', 'AAPL', 'MSFT']  # Example tickers for testing; replace with actual tickers as needed
    from_date = "2023-01-01"
    end_date = "2026-03-13"
    df = get_by_tickers(tickers, from_date, end_date)
    return df

def get_hist_prices_for_positions(start_date: str, end_date: str) -> pd.DataFrame:  
    positions = get_positions("dummy_user")  # Replace with actual username if needed
    tickers = [pos["ticker"] for pos in positions if pos["ticker"] is not None]
    start_date = "2023-01-01"
    end_date = "2026-03-13"
    hist_prices = get_by_tickers(tickers, start_date, end_date)

    return pd.DataFrame(positions)
    
