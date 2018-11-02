#This file is meant to take in data requests from a requests table in sqlite3
#   So if there is any api retreval needing to be done, this is the script to come to.

from alpha_vantage.timeseries import TimeSeries
import csv
import sqlite3
import time



#Takes in a list of stock tickers that need to be updated on an intraday basis, 
#   if no list is provided, then it updates all stocks
def getSymbols_(someSymbols=None):
    #connect to the sqlite3 db
    con = sqlite3.connect('/home/stefan/SimpleTraderBot/markets')
    cur = con.cursor()
    
    #connect to the Alpha Vantage REST(-ish?) Api
    key = open('/home/stefan/SimpleTraderBot/key', 'r').read()
    ts = TimeSeries(key,output_format='csv')
    
    
    ##retrieve the different tickers
    #declaring local symbol storage 
    symbols = [] 
   
    #if function is not used for a specific set of stocks, update the whole set
    if someSymbols == None:
         #get the list of tickers from the database
         markets = ['NYSE','NASDAQ']

             
         for i in markets:
             conn = sqlite3.connect('/home/stefan/SimpleTraderBot/markets')
             
             cur = conn.cursor()
             cur.execute("select Symbol FROM "+i)
             
             #this is an array of all of the tickers from the query
             symbols.extend(cur.fetchall()) 
    
    else:
        symbols = someSymbols
   

    for symbol in symbols:  

        #attempt to fetch the stock's information for the day
        try: 
            time.sleep(12)
            csvfileRaw = ts.get_intraday(symbol, interval='1min',outputsize = 'full')
        except:
            print('Retrieval error on ' +symbol)
        csvfile = list(csvfileRaw)
        
        for rows in csvfile:  
           # try: 
                #convert the csv row into a list
                if rows is not None:
                    row = list(rows) 
                    
                    for cell in row[1:]:
                        print('insert into '+symbol[0]+' (timestamp,open,high,low,\
                                close,volume) values (' + cell[0] + ',' +\
                                cell[1] + ','+\
                                cell[2] + ','+\
                                cell[3] + ','+\
                                cell[4] + ','+\
                                cell[5] + ');')
                        cur.execute('insert into '+symbol[0]+' (timestamp,open,high,low,close,volume) values (\'' + cell[0] + '\',' +\
                                cell[1] + ','+\
                                cell[2] + ','+\
                                cell[3] + ','+\
                                cell[4] + ','+\
                                cell[5] + ');')
 
                else:
                    continue
getSymbols_()
