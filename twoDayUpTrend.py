"""
查询连续两日k线上涨大于4%小于等于5%的股票
"""

import tushare as ts
import pandas as pd
import numpy as np
import json
import sys

try:
    with open("config.json", "r") as file:
        config = json.load(file)
        ts.set_token(config["token"])
except FileNotFoundError:
    print("Error: The config file does not exist")
    sys.exit(1)

pro = ts.pro_api()


def retrieve_medium_sized_stock(trade_date):
    # Define market capitalization thresholds for medium-sized stocks (in billion CNY)
    market_cap_min = 10  # Adjust the minimum threshold as needed
    market_cap_max = 65  # Adjust the maximum threshold as needed

    # Fetch the stock basic information
    stock_info = pro.query('stock_basic', exchange='', list_status='L',
                           fields='ts_code,symbol,name,area,industry,list_date')

    # Filter for SZ and SH stocks only
    stock_info = stock_info[(stock_info['ts_code'].str.endswith('.SZ')) | (stock_info['ts_code'].str.endswith('.SH'))]

    # Fetch the market capitalization for each stock
    # Using the latest available daily data
    stock_cap = pro.daily_basic(ts_code='', trade_date=trade_date,
                                fields='ts_code,total_mv')  # Replace trade_date as needed

    # Merge the basic info with market cap info
    merged_df = pd.merge(stock_info, stock_cap, on='ts_code')

    # Convert market cap from thousand CNY to billion CNY
    merged_df['total_mv'] = merged_df['total_mv'] / 10000

    merged_df.to_csv("stocklist.csv", index=False)
    # Filter for medium-sized stocks
    medium_stocks = merged_df[(merged_df['total_mv'] >= market_cap_min) & (merged_df['total_mv'] <= market_cap_max)]
    return np.array(medium_stocks['ts_code'].tolist()), merged_df


def has_two_consecutive_up_candles(stock_codes, trade_date_start, trade_date_end):
    ts_code_str = ','.join(stock_codes)
    # Fetch the daily data
    df = pro.daily(ts_code=ts_code_str, start_date=trade_date_start, end_date=trade_date_end)

    if df is None or df.empty:
        return np.array([])

        # Calculate the percentage change for each candlestick
    df['percent_change'] = (df['close'] - df['open']) / df['open'] * 100

    # Identify up candlesticks with a 4% to 5% increase
    df['up_candle'] = (df['percent_change'] >= 4) & (df['percent_change'] <= 5)

    # Group by stock code and check for two consecutive up candlesticks
    df['consecutive_up'] = df.groupby('ts_code')['up_candle'].rolling(window=2).sum().reset_index(0, drop=True) >= 2

    # Find the stock codes with consecutive up candlesticks
    result = df.groupby('ts_code').filter(lambda x: x['consecutive_up'].any())

    # Extract the unique stock codes that meet the criteria
    return result[(result['percent_change'] >= 4) & (result['consecutive_up'] == True)]


def split_list(lst, n):
    """Split a list into sub-lists of size n."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# List of stock codes to check
# stock_codes = ['000001.SZ', '600519.SH']  # Add more stock codes as needed
trade_date_start = '20240828'  # Replace with the desired date
trade_date_end = '20240902'
is_read_api = True

if is_read_api:
    codes, stocks_medium_size = retrieve_medium_sized_stock(trade_date_end)
else:
    stocks_medium_size = pd.read_csv("stocklist.csv")
    codes = np.array(stocks_medium_size['ts_code'].tolist())

# Split the stock_codes array into sub-arrays of 1000 items each
sub_arrays = list(split_list(codes, 1000))
result_df = pd.DataFrame()
for sub_array in sub_arrays:
    result_df = pd.concat([result_df, has_two_consecutive_up_candles(sub_array, trade_date_start, trade_date_end)])

# For each stock, find its industry from the csv file
if result_df.empty:
    print("Not Found")
else:
    result = pd.merge(result_df, stocks_medium_size[['ts_code', 'area', 'industry']], on='ts_code', how='left')
    print(result)
