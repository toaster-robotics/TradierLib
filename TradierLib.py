import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time
from typing import Optional, List, Literal

API_KEY = ''
BASE_URL = 'https://api.tradier.com/'


def troubleshoot(stream: str):
    with open('temp.txt', 'w') as f:
        f.write(stream)


def fetch_url(url: str, params: Optional[dict] = None, headers: Optional[dict] = None, max_retries: int = 3, sleep: float = 2):
    attempts = 0
    while attempts < max_retries:
        try:
            response = requests.get(url, params, headers=headers)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            attempts += 1
            if attempts < max_retries:
                time.sleep(sleep)


def get_historical_quote(symbol: str, start_date: str, end_date: str, resolution: Literal['daily', 'weekly', 'monthly'] = 'daily') -> pd.DataFrame:
    endpoint = 'v1/markets/history'
    symbol = symbol.upper()
    columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']

    params = {
        'symbol': symbol,
        'interval': resolution,
        'start': start_date,
        'end': end_date,
        'session_filter': 'all',
    }
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Bearer %s' % API_KEY,
    }
    response = fetch_url(BASE_URL + endpoint, params, headers)
    if response.status_code == 200:
        json_response = response.json()
        history = json_response.get('history', {})
        history = history if history is not None else {}
        data = history.get('day', [])
        if len(data) > 0:
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['symbol'] = symbol
            return df[columns]

    print('%s data not received' % symbol)
    print('\tResponse Code: %u' % response.status_code)
    print('\tRatelimit Available: %s' %
          response.headers.get('X-Ratelimit-Available'))
    print('\tRatelimit Resets in: %ss' %
          (int(response.headers.get('X-Ratelimit-Expiry')) / 1000 - int(time.time())))
    return pd.DataFrame(columns=columns)


def get_historical_quotes(symbols: List[str], start_date: str, end_date: str, resolution: Literal['daily', 'weekly', 'monthly'] = 'daily') -> pd.DataFrame:
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda symbol: get_historical_quote(
            symbol, start_date, end_date, resolution), symbols))
    results = [df for df in results if not df.empty]
    df = pd.concat(results, ignore_index=True)
    return df.sort_values(by=['symbol', 'date']).reset_index(drop=True)


def get_latest_quotes(symbols: List[str], greeks: bool = False) -> pd.DataFrame:
    endpoint = 'v1/markets/quotes'
    columns = ['symbol', 'description', 'exch', 'type', 'last', 'change', 'volume', 'open', 'high', 'low', 'close', 'bid', 'ask', 'change_percentage', 'average_volume',
               'last_volume', 'trade_date', 'prevclose', 'week_52_high', 'week_52_low', 'bidsize', 'bidexch', 'bid_date', 'asksize', 'askexch', 'ask_date', 'root_symbols']
    if greeks:
        columns += ['greeks.delta', 'greeks.gamma', 'greeks.theta', 'greeks.vega', 'greeks.rho', 'greeks.phi',
                    'greeks.bid_iv', 'greeks.mid_iv', 'greeks.ask_iv', 'greeks.smv_vol', 'greeks.updated_at']
    params = {
        'symbols': ','.join([i.upper() for i in symbols]),
        'greeks': greeks,
    }
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Bearer %s' % API_KEY,
    }
    response = fetch_url(BASE_URL + endpoint, params, headers)
    if response.status_code == 200:
        json_response = response.json()
        quotes = json_response.get('quotes', {})
        quotes = quotes if quotes is not None else {}
        data = quotes.get('quote', [])
        data = data if isinstance(data, list) else [data]
        if len(data) > 0:
            df = pd.DataFrame(data)
            df['trade_date'] = pd.to_datetime(df['trade_date'], unit='ms')
            df['bid_date'] = pd.to_datetime(df['bid_date'], unit='ms')
            df['ask_date'] = pd.to_datetime(df['ask_date'], unit='ms')
            return df[columns]
