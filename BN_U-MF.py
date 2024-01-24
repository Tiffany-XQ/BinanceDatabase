import pandas as pd
import time
import os
import logging
import traceback
import ccxt

BINANCE_CONFIG = {
    'apiKey': '',
    'secret': '',
    'timeout': 8000,
    'rateLimit': 10,
    'verbose': False,
    'hostname': 'binancezh.com',
    'enableRateLimit': False}

EXCHANGE = ccxt.binance(BINANCE_CONFIG)

MAX_TRIAL = 3

ROOT_PATH = os.getcwd()
UMF_PATH = os.path.join(ROOT_PATH, 'Database', EXCHANGE.id, 'U-MF')

def save_klines(params, date, symbol, time_interval):
    
    df = EXCHANGE.fapiPublicGetKlines(params=params)    # USDT-M feature: fapi
    df = pd.DataFrame(df, dtype=float,
                    columns=['Open_time', 'Open', 'High', 'Low', 'Close', 'Volume',
                            'Close_time', 'Quote_vol', 'Num_trades',
                            'Taker_buy_base_asset_vol', 'Taker_buy_quote_asset_vol', 
                            'Ignore'])
    df['Open_time'] = pd.to_datetime(df['Open_time'], unit='ms')
    df['Close_time'] = pd.to_datetime(df['Close_time'], unit="ms")
    df.drop(["Ignore"], axis=1)

    path = os.path.join(UMF_PATH, str(pd.Timestamp(date).date()))
    os.makedirs(path, exist_ok=True)
    file_name = '_'.join([symbol.replace('/', '-'), time_interval]) + '.csv'
    path = os.path.join(path, file_name)

    df.to_csv(path, index=False)


def main(date):

    Symbols = ['BTC/USDT', 'ETH/USDT']

    Intervals = ['5m', '15m', '30m']

    start_time = EXCHANGE.parse8601(date)
    end_time = pd.Timestamp(date)+pd.Timedelta("1 day")
    end_time = EXCHANGE.parse8601(str(end_time))

    logging.basicConfig(format='%(asctime)s || %(message)s', datefmt='%Y-%m-%d %H:%M:%S', 
                        level=logging.INFO)

    for symbol in Symbols:

        for time_interval in Intervals:

            logging.info(f'Fetching Binance {pd.Timestamp(date).date()} {symbol} {time_interval} USDT-M Features klines...')

            for _ in range(MAX_TRIAL):

                try:
                    params = {'symbol': symbol.replace('/', ''), 'interval': time_interval,
                            'limit': 1000, 'startTime': start_time, 'endTime': end_time}
                    save_klines(params, date, symbol, time_interval)
                    logging.info('Fetching success.')
                    time.sleep(2)
                    break

                except:
                    traceback.print_exc()
                    logging.info(f'Fetching fail.')
                    continue


if __name__ == '__main__':
    Start = "2022-01-01"
    End = "2022-01-03"

    Date = pd.date_range(start=Start, end=End, freq='1D')
    Date = Date.strftime("%Y-%m-%d 00:00:00")

    for date in Date:
        main(date)
