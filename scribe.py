#This file is meant to take in data requests from a requests table in sqlite3

from alpha_vantage.timeseries import TimeSeries

key = 'YHOJ4YDE4QUFKCGZ'
ts = TimeSeries(key,output_format='csv')

print(ts.get_intraday('MSFT', interval='1min',outputsize = 'full'))
