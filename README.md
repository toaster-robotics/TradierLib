# Python Tradier Data API
## Reference
[Tradier Data HTML Docs](https://documentation.tradier.com/brokerage-api)
## Examples

### Loading API Key
```python
import TradierLib as tl

tl.API_KEY = '420.69'
```

### get a single quote
```python
df = tl.get_historical_quote(
    symbol='AAPL',
    start_date='2024-08-01',
    end_date='2024-08-15',
)
print(df)
```

### get multiple quotes
```python
df = tl.get_historical_quotes(
    symbols=['AAPL', 'TSLA', 'RKLB250417C00030000'],
    start_date='2024-08-01',
    end_date='2024-08-15',
)
print(df)
```


### get latest quote
```python
df = tl.get_latest_quotes(
    symbols=['AAPL', 'TSLA', 'RKLB250417C00030000'],
)
print(df)
```
