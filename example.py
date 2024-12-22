import TradierLib as tl

with open('key') as f:
    tl.API_KEY = f.readlines()[0].strip()


# get a single quote
df = tl.get_historical_quote(
    symbol='AAPL',
    start_date='2024-08-01',
    end_date='2024-08-15',
)
print(df)

# get multiple quotes
df = tl.get_historical_quotes(
    symbols=['AAPL', 'TSLA', 'RKLB250417C00030000'],
    start_date='2024-08-01',
    end_date='2024-08-15',
)
print(df)


# get latest quote
df = tl.get_latest_quotes(
    symbols=['AAPL', 'TSLA', 'RKLB250417C00030000'],
)
print(df)
