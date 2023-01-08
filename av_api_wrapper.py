"""
Wrapper to Alpha Vantage API
"""

import os
import logging
import requests
import pandas as pd

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

api_key = os.environ['ALPHA_VANTAGE_API_KEY']


def time_series_intraday(symbol: str, outputsize: str, interval: str = '5min'):
    if outputsize in ['full', 'compact']:
        pass
    else:
        logger.error('Wrong output size, %s data was not retrieved.', symbol)
        return pd.DataFrame()

    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=' \
          f'{symbol}&interval' \
          f'={interval}&outputsize={outputsize}&apikey={api_key}'

    r = requests.get(url)
    data = r.json()
    if 'Error Message' in data:
        logger.info(data['Error Message'])
        return pd.DataFrame()
    else:
        logger.info('%s data retrieved successfully (output size: %s).',
                    symbol, outputsize)

    data = pd.DataFrame.from_dict(data['Time Series (5min)'], orient='index').reset_index()
    data.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
    return data


if __name__ == '__main__':
    print(time_series_intraday(symbol='AAPL', outputsize='full'))
