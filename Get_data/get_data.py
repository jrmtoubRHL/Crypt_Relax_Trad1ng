import requests
from datetime import datetime
import pandas as pd
from stockstats import StockDataFrame


symbols_lst = ['ETH', 'BTC', 'XPR', 'BCH', 'ADA', 'LTC', 'MIOTA', 'DASH',
               'XLM',
           'XMR', 'EOS', 'BTG', 'NEO', 'QTUM', 'ETC', 'BCC' , 'LSK', 'TRX',
           'XVG', 'XBR', 'ZEC', 'STRAT', 'DOGE', 'ARK',  ]

exchanges_lst = ['Bitfinex', 'Bitstamp', 'Poloniex', 'GDAX', ' HitBTC' ,
                 'Bittrex', 'Coinbase']

datetime_interval = 'hour'
to_symbol = 'USD'



def get_filename(from_symbol, to_symbol, exchange, datetime_interval, download_date):
    return '%s_%s_%s_%s_%s.csv' % (from_symbol, to_symbol, exchange, datetime_interval, download_date)


def download_data(from_symbol, to_symbol, exchange, datetime_interval):
    supported_intervals = {'minute', 'hour', 'day'}
    assert datetime_interval in supported_intervals,\
        'datetime_interval should be one of %s' % supported_intervals

    print('Downloading %s trading data for %s %s from %s' %
          (datetime_interval, from_symbol, to_symbol, exchange))
    base_url = 'https://min-api.cryptocompare.com/data/histo'
    url = '%s%s' % (base_url, datetime_interval)

    params = {'fsym': from_symbol, 'tsym': to_symbol,
              'limit': 2000, 'aggregate': 1,
              'e': exchange}
    request = requests.get(url, params=params)
    data = request.json()
    return data


def convert_to_dataframe(data):
    df = pd.io.json.json_normalize(data, ['Data'])
    df['datetime'] = pd.to_datetime(df.time, unit='s')
    df = df[['datetime', 'low', 'high', 'open',
             'close', 'volumefrom', 'volumeto']]
    return df


def filter_empty_datapoints(df):
    indices = df[df.sum(axis=1) == 0].index
    print('Filtering %d empty datapoints' % indices.shape[0])
    df = df.drop(indices)
    return df


def get_CSV(macd=True):
	'''

	For each currencies, in each exchange , return a .csv with price and more,


	:param macd: Add collums with moving average stuffs
	:return:
	'''
	for simbolito in symbols_lst:

		for changito in exchanges_lst:
			data = download_data(simbolito, to_symbol, changito, datetime_interval)
			try:
				df = convert_to_dataframe(data)
			except:
				print('Cannot convert dataframe {} {}'.format(simbolito, changito))
				continue
			df = filter_empty_datapoints(df)

			current_datetime = datetime.now().date().isoformat()
			filename = get_filename(simbolito, to_symbol, changito, datetime_interval, current_datetime)

			if macd == True:
				#Stcokmarket stuff ;
				df = StockDataFrame.retype(df)
				df['macd'] = df.get('macd')  # calculate MACD

			print(df.head(2))
			print('Saving data to %s' % filename)
			df.to_csv(filename, index=False)



def read_dataset(filename):
    print('Reading data from %s' % filename)
    df = pd.read_csv(filename)
    df.datetime = pd.to_datetime(df.datetime) # change type from object to datetime
    df = df.set_index('datetime')
    df = df.sort_index() # sort by datetime
    # print(df.shape)
    return df


 # get_CSV(macd=False)