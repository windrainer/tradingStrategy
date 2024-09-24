import tushare as ts
import pandas as pd
import numpy as np
import json
import sys

# Load the Tushare token from the config file
try:
    with open("config.json", "r") as file:
        config = json.load(file)
        ts.set_token(config["token"])
except FileNotFoundError:
    print("Error: The config file does not exist")
    sys.exit(1)

# Initialize the Tushare API
pro = ts.pro_api()
start_date = '20240701'
end_date = '20240923'


# Helper function to split a list into sub-lists of size n
def split_list(lst, n):
    """Split a list into sub-lists of size n."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# Read stock list and split into smaller chunks
stocks_medium_size = pd.read_csv("stocklist.csv")
codes = np.array(stocks_medium_size['ts_code'].tolist())
sub_arrays = list(split_list(codes, 1000))


# Define a function to process stock data for each group of stocks
def start_process_stock(stock_array, start_date, end_date):
    ts_code_str = ','.join(stock_array)

    # Fetch daily stock data for multiple stocks
    df = pro.daily(ts_code=ts_code_str, start_date=start_date, end_date=end_date)

    if df.empty:
        return pd.DataFrame()  # Return empty DataFrame if no data is retrieved

    df = df[(df['ts_code'].str.endswith('.SZ')) | (df['ts_code'].str.endswith('.SH'))]
    df = df[~df['ts_code'].str.startswith('688')]
    # Sort the data by trade_date to ensure correct moving average calculation
    df = df.sort_values(by=['ts_code', 'trade_date'])

    # Group the data by 'ts_code'
    grouped_df = df.groupby('ts_code')

    # Initialize an empty DataFrame to store selected stocks
    selected_stocks = pd.DataFrame()

    # Process each group of stock data
    for stock_code, group in grouped_df:
        result = process_stock_data(group)
        if result is not None:
            selected_stocks = pd.concat([selected_stocks, result])

    return selected_stocks


# Define a function to calculate 20-day MA and check the conditions
def process_stock_data(group):
    group = group.copy()  # Ensure we are working on a copy to avoid SettingWithCopyWarning

    # Calculate the 20-day moving average (MA20)
    group['ma20'] = group['close'].rolling(window=5).mean()

    # Calculate BIAS = ((Close - MA20) / MA20) * 100
    group['bias_ratio'] = (group['close'] - group['ma20']) / group['ma20'] * 100

    # Check condition 1: BIAS between 1 and 1.5
    bias_condition = (group['bias_ratio'] > 1) & (group['bias_ratio'] < 1.5)

    # Check condition 2: The price has been above the MA20 for at least 5 days
    #above_ma20_condition = (group['close'] > group['ma20']).rolling(window=5).sum() >= 5

    # Check condition 3: The MA20 trend is upward (MA20 is increasing)
    ma20_trend_condition = group['ma20'].diff() > 0

    # Apply the conditions to select stocks
    if (bias_condition  & ma20_trend_condition).any():
        return group
    return None


# Initialize an empty DataFrame to hold the final results
result_df = pd.DataFrame()

# Process each sub-array of stocks
for sub_array in sub_arrays:
    result_df = pd.concat([result_df, start_process_stock(sub_array, start_date, end_date)])

if result_df.empty:
    print("Not Found")
else:
    result_df.to_csv("result_buy2_"+end_date)
# Display the selected stocks
#print(result_df[['ts_code', 'trade_date', 'close', 'ma20', 'bias_ratio']])