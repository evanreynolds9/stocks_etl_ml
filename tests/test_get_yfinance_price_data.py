# Expose etl package to python path
import sys
import os

proj_top_level = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if proj_top_level not in sys.path:
    sys.path.insert(0, proj_top_level)


# Import functions from file
from etl.src.get_yfinance_price_data import get_daily_price_data, transform_price_data
import pandas as pd
import pytest

class TestGetYfinancePriceData:
    valid_tickers = pd.DataFrame({"ticker": ["TD.TO", "BMO.TO"]})
    half_valid_tickers = pd.DataFrame({"ticker": ["TD.TO", "BMO.TOZO"]})
    invalid_tickers = pd.DataFrame({"ticker": ["TD.TOZO", "BMO.TOZO"]})
    invalid_df = pd.DataFrame({"wrong_col_name": ["TD.TOZO", "BMO.TOZO"]})
    start_date = "2025-12-01"
    end_date = "2025-12-02"

    # Confirm we can successfully pull data for two tickers, returning two rows with no nulls
    def test_valid_row_output(self):
        df = get_daily_price_data(self.valid_tickers, self.start_date, self.end_date)
        assert len(df.dropna()) == 2

    # Confirm if our start and end dates are the same, we will get no rows
    def test_same_date_output(self):
        df = get_daily_price_data(self.valid_tickers, self.start_date, self.start_date)
        assert len(df) == 0

    # Confirm if our end date is before out start date, we will get no rows
    def test_invalid_date_output(self):
        df = get_daily_price_data(self.valid_tickers, self.end_date, self.start_date)
        assert len(df) == 0

    # confirm if only some of the tickers are valid, we will still get a full row count, just with
    def test_half_valid_tickers(self):
        df = get_daily_price_data(self.half_valid_tickers, self.start_date, self.end_date)
        assert len(df) == 2 and len(df.dropna()) == 1

    def test_invalid_tickers(self):
        df = get_daily_price_data(self.invalid_tickers, self.start_date, self.end_date)
        assert len(df) == 0

    def test_invalid_list(self):
        with pytest.raises(KeyError):
            get_daily_price_data(self.invalid_df, self.start_date, self.end_date)