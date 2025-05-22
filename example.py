import TradierLib as tl

# get balances
balance_dict = tl.get_balances(
    account_id='42069',
)
print(balance_dict)

# get positions
df = tl.get_positions(
    account_id='42069',
)
print(df)

# get historical
df = tl.get_historical(
    symbols=['AAPL'],
    start_date='2024-08-01',
    end_date='2024-08-15',
    resolution='daily',
)
print(df)
