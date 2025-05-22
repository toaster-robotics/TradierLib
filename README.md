# Python Tradier Data API
## Reference
[Tradier Data HTML Docs](https://documentation.tradier.com/brokerage-api)
## Examples

### get balances
```python
balance_dict = get_balances(
    account_id='42069',
)
print(balance_dict)
```

### get positions
```python
df = get_positions(
    account_id='42069',
)
print(df)
```

### get historical
```python
df = get_historical(
    symbols=['AAPL'],
    start_date='2024-08-01',
    end_date='2024-08-15',
    resolution='daily',
)
print(df)
```
