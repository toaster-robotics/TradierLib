import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time
import os
from dotenv import load_dotenv
from typing import Optional, List, Literal, Union, Dict
load_dotenv()

API_KEY = os.getenv('TRADIER_KEY')
BASE_URL = 'https://api.tradier.com/'


def fetch_url(url: str, params: Optional[dict] = None, headers: Optional[dict] = None, max_retries: int = 3, sleep: float = 2):
    attempts = 0
    while attempts < max_retries:
        try:
            response = requests.get(url, params, headers=headers)
            # print(response.text)
            # print(response.status_code)
            # quit()
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            attempts += 1
            if attempts < max_retries:
                time.sleep(sleep)


def get_balances(account_id: str) -> Dict[str, Union[str, int, float]]:
    endpoint = f'/v1/accounts/{account_id}/balances'

    params = {}
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Bearer %s' % API_KEY,
    }
    response = fetch_url(BASE_URL + endpoint, params, headers)
    data = response.json().get('balances', {})
    return data


def get_positions(account_id: str) -> pd.DataFrame:
    endpoint = f'/v1/accounts/{account_id}/positions'

    params = {}
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Bearer %s' % API_KEY,
    }
    response = fetch_url(BASE_URL + endpoint, params, headers)
    data = response.json().get('positions')
    if data is not None:
        df = pd.DataFrame(data['position'])
        df['date_acquired'] = pd.to_datetime(
            df['date_acquired'], utc=True).dt.tz_convert('America/New_York')
        df['date_acquired'] = df['date_acquired'].dt.round('s')
        df = df[['symbol', 'date_acquired', 'quantity', 'cost_basis']]
        df = df.rename(columns={'quantity': 'shares'})
        df = df.set_index('symbol')
    else:
        return pd.DataFrame()


def get_historical_quotes(symbols: List[str], start_date: Optional[Union[pd.Timestamp, str]] = None, end_date: Optional[Union[pd.Timestamp, str]] = None, resolution: Literal['daily', 'weekly', 'monthly'] = 'daily') -> pd.DataFrame:
    def get_quote(symbol: str, start_date: Optional[Union[pd.Timestamp, str]] = None, end_date: Optional[Union[pd.Timestamp, str]] = None, resolution: Literal['daily', 'weekly', 'monthly'] = 'daily') -> pd.DataFrame:
        endpoint = 'v1/markets/history'
        symbol = symbol.upper()
        columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']

        params = {
            'symbol': symbol,
            'interval': resolution,
            'session_filter': 'open',
        }
        if start_date is not None:
            start_date = start_date.isoformat() if isinstance(
                start_date, pd.Timestamp) else start_date
            params['start'] = start_date
        if end_date is not None:
            end_date = end_date.isoformat() if isinstance(
                end_date, pd.Timestamp) else end_date
            params['end'] = end_date

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
            data = data if isinstance(data, list) else [data]
            if len(data) > 0:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(
                    df['date']).dt.tz_localize('America/New_York')
                df['symbol'] = symbol
                return df[columns].set_index(['date', 'symbol'])

        print('%s data not received' % symbol)
        print('\tResponse Code: %u' % response.status_code)
        print('\tResponse Data: %s' % response.text)
        print('\tRatelimit Available: %s' %
              response.headers.get('X-Ratelimit-Available'))
        print('\tRatelimit Resets in: %ss' %
              (int(response.headers.get('X-Ratelimit-Expiry')) / 1000 - int(time.time())))
        return pd.DataFrame(columns=columns)

    with ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda symbol: get_quote(
            symbol, start_date, end_date, resolution), symbols))
    results = [df for df in results if not df.empty]
    df = pd.concat(results)
    return df


def get_timesales(symbols: List[str], start_date: Optional[Union[pd.Timestamp, str]] = None, end_date: Optional[Union[pd.Timestamp, str]] = None, resolution: Literal['tick', '1min', '15min'] = '1min') -> pd.DataFrame:
    endpoint = 'v1/markets/timesales'
    columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']

    def get_quote(symbol: str, start_date: Optional[Union[pd.Timestamp, str]] = None, end_date: Optional[Union[pd.Timestamp, str]] = None, resolution: Literal['tick', '1min', '15min'] = '1min') -> pd.DataFrame:
        symbol = symbol.upper()

        params = {
            'symbol': symbol,
            'interval': resolution,
            'session_filter': 'open',
        }
        if start_date is not None:
            start_date = start_date.isoformat() if isinstance(
                start_date, pd.Timestamp) else start_date
            params['start'] = start_date
        if end_date is not None:
            end_date = end_date.isoformat() if isinstance(
                end_date, pd.Timestamp) else end_date
            params['end'] = end_date

        headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer %s' % API_KEY,
        }
        response = fetch_url(BASE_URL + endpoint, params, headers)
        if response.status_code == 200:
            json_response = response.json()
            series = json_response.get('series', {})
            series = series if series is not None else {}
            data = series.get('data', [])
            data = data if isinstance(data, list) else [data]
            if len(data) > 0:
                df = pd.DataFrame(data)

                df['date'] = pd.to_datetime(
                    df['timestamp'], unit='s', utc=True).dt.tz_convert('America/New_York')
                df['symbol'] = symbol
                return df[columns].set_index(['date', 'symbol'])

        print('%s data not received' % symbol)
        print('\tResponse Code: %u' % response.status_code)
        print('\tResponse Data: %s' % response.text)
        print('\tRatelimit Available: %s' %
              response.headers.get('X-Ratelimit-Available'))
        print('\tRatelimit Resets in: %ss' %
              (int(response.headers.get('X-Ratelimit-Expiry')) / 1000 - int(time.time())))
        return pd.DataFrame(columns=columns)

    with ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda symbol: get_quote(
            symbol, start_date, end_date, resolution), symbols))
    results = [df for df in results if not df.empty]
    df = pd.concat(results)
    return df


def get_historical(symbols: List[str], start_date: Optional[Union[pd.Timestamp, str]] = None, end_date: Optional[Union[pd.Timestamp, str]] = None, resolution: Literal['tick', '1min', '15min', 'daily', 'weekly', 'monthly'] = 'daily') -> pd.DataFrame:
    resolution = resolution.lower()
    if resolution in ['daily', 'weekly', 'monthly']:
        f = get_historical_quotes
    else:
        f = get_timesales

    df = f(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        resolution=resolution,
    )
    return df


def get_latest_quotes(symbols: List[str], greeks: bool = False) -> pd.DataFrame:
    endpoint = 'v1/markets/quotes'
    columns = ['symbol', 'description', 'exch', 'type', 'last', 'change', 'volume', 'open', 'high', 'low', 'close', 'bid', 'ask', 'change_percentage', 'average_volume',
               'last_volume', 'trade_date', 'prevclose', 'week_52_high', 'week_52_low', 'bidsize', 'bidexch', 'bid_date', 'asksize', 'askexch', 'ask_date', 'root_symbol']
    if greeks:
        columns += ['greeks.delta', 'greeks.gamma', 'greeks.theta', 'greeks.vega', 'greeks.rho', 'greeks.phi',
                    'greeks.bid_iv', 'greeks.mid_iv', 'greeks.ask_iv', 'greeks.smv_vol', 'greeks.updated_at']
    syms = ','.join([i.upper() for i in symbols])
    params = {
        'symbols': syms,
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
            df = pd.json_normalize(data)
            df['trade_date'] = pd.to_datetime(
                df['trade_date'], unit='ms', utc=True).dt.tz_convert('America/New_York')
            df['bid_date'] = pd.to_datetime(
                df['bid_date'], unit='ms', utc=True).dt.tz_convert('America/New_York')
            df['ask_date'] = pd.to_datetime(
                df['ask_date'], unit='ms', utc=True).dt.tz_convert('America/New_York')
            return df[columns]

    print('%s data not received' % syms)
    print('\tResponse Code: %u' % response.status_code)
    print('\tResponse Data: %s' % response.text)
    print('\tRatelimit Available: %s' %
          response.headers.get('X-Ratelimit-Available'))
    print('\tRatelimit Resets in: %ss' %
          (int(response.headers.get('X-Ratelimit-Expiry')) / 1000 - int(time.time())))
    return pd.DataFrame(columns=columns)


def get_chains(symbol: str, expiration: str, greeks: bool = True) -> pd.DataFrame:
    endpoint = 'v1/markets/options/chains'
    params = {
        'symbol': symbol.upper(),
        'expiration': expiration,
        'greeks': greeks,
    }
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Bearer %s' % API_KEY,
    }
    response = fetch_url(BASE_URL + endpoint, params, headers)
    if response.status_code == 200:
        json_response = response.json()
        chains = json_response.get('options', {})
        chains = chains if chains is not None else {}
        data = chains.get('option', [])
        data = data if isinstance(data, list) else [data]
        if len(data) > 0:
            df = pd.json_normalize(data)
            print(df.columns)
            df['trade_date'] = pd.to_datetime(df['trade_date'], unit='ms')
            df['bid_date'] = pd.to_datetime(df['bid_date'], unit='ms')
            df['ask_date'] = pd.to_datetime(df['ask_date'], unit='ms')
            df['greeks.updated_at'] = pd.to_datetime(df['greeks.updated_at'])
            return df

    columns = ['symbol', 'description', 'exch', 'type', 'last', 'change', 'volume', 'open', 'high', 'low', 'close', 'bid', 'ask', 'underlying', 'strike', 'change_percentage', 'average_volume', 'last_volume', 'trade_date', 'prevclose', 'week_52_high', 'week_52_low', 'bidsize', 'bidexch', 'bid_date', 'asksize',
               'askexch', 'ask_date', 'open_interest', 'contract_size', 'expiration_date', 'expiration_type', 'option_type', 'root_symbol', 'greeks.delta', 'greeks.gamma', 'greeks.theta', 'greeks.vega', 'greeks.rho', 'greeks.phi', 'greeks.bid_iv', 'greeks.mid_iv', 'greeks.ask_iv', 'greeks.smv_vol', 'greeks.updated_at']
    print('%s data not received' % symbol)
    print('\tResponse Code: %u' % response.status_code)
    print('\tResponse Data: %s' % response.text)
    print('\tRatelimit Available: %s' %
          response.headers.get('X-Ratelimit-Available'))
    print('\tRatelimit Resets in: %ss' %
          (int(response.headers.get('X-Ratelimit-Expiry')) / 1000 - int(time.time())))
    return pd.DataFrame(columns=columns)


def get_strikes(symbol: str, expiration: str) -> List[float]:
    endpoint = 'v1/markets/options/strikes'
    params = {
        'symbol': symbol.upper(),
        'expiration': expiration,
    }
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Bearer %s' % API_KEY,
    }
    response = fetch_url(BASE_URL + endpoint, params, headers)
    if response.status_code == 200:
        json_response = response.json()
        data = json_response.get('strikes', {})
        data = data if data is not None else {}
        data = data.get('strike', [])
        data = data if isinstance(data, list) else [data]
        if len(data) > 0:
            return data

    print('%s data not received' % symbol)
    print('\tResponse Code: %u' % response.status_code)
    print('\tResponse Data: %s' % response.text)
    print('\tRatelimit Available: %s' %
          response.headers.get('X-Ratelimit-Available'))
    print('\tRatelimit Resets in: %ss' %
          (int(response.headers.get('X-Ratelimit-Expiry')) / 1000 - int(time.time())))
    return []


if __name__ == '__main__':
    pass

    # df = get_historical_quotes(
    #     symbols=['TSLA'],
    #     start_date='2025-05-15',
    #     # end_date='2025-05-15',
    # )
    # print(df)

    # df = get_historical_quotes(
    #     symbols=['TSLA', 'AAPL'],
    #     start_date='2025-05-15',
    # )
    # print(df)

    # df = get_timesales(
    #     symbols=['TSLA'],
    #     start_date='2025-05-15',
    #     end_date='2025-05-16',
    # )
    # print(df)

    # df = get_historical(
    #     symbols=['TSLA', 'AAPL'],
    #     start_date='2025-05-15',
    #     end_date='2025-05-17',
    #     resolution='15min'
    # )
    # print(df)

    balance = get_balances(os.getenv('ACCOUNT_ID'))
    print(balance['cash'])
    # get_positions(ACCOUNT_ID)
