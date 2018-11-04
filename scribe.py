#This file is meant to take in data requests from a requests table in sqlite3
#   So if there is any api retreval needing to be done, this is the script to come to.

from alpha_vantage.timeseries import TimeSeries
import csv
import sqlite3
import time
import contextlib
import numpy

#An SQL execution statement
def sqlStatement(statement):
    pathToDB =  '/home/stefan/SimpleTraderBot/markets'
    with contextlib.closing(sqlite3.connect(pathToDB,timeout=10)) as conn:
        with conn:
            with contextlib.closing(conn.cursor()) as cursor:
                cursor.execute(statement)


#Takes in a list of stock tickers that need to be updated on an intraday basis, 
#   if no list is provided, then it updates all stocks
def getSymbols_(someSymbols=None):

    #connect to the Alpha Vantage REST(-ish?) Api
    key = open('/home/stefan/key', 'r').read()
    ts = TimeSeries(key,output_format='csv')
     
    ##retrieve the different tickers
    #declaring local symbol storage 
    symbols = [] 
   
    #if function is not used for a specific set of stocks, update the whole set
    if someSymbols == None:
         #get the list of tickers from the database
         markets = ['NYSE','NASDAQ']

             
         for i in markets:
             #connect to the sqlite3 db
             con = sqlite3.connect('/home/stefan/SimpleTraderBot/markets',timeout=10)
             cur = con.cursor()
                
             #cur = conn.cursor()
             cur.execute("select Symbol FROM "+i)
             con.commit()
             #this is an array of all of the tickers from the query
             symbols.extend(cur.fetchall()) 

             con.close()
    
    else:
        symbols = someSymbols
  
    #all symbols that errored out are stored here
    errors = []

    for symbol in symbols:  

        #attempt to fetch the stock's information for the day--if not able to reach the API, try again, if daily limit reached, wait 24 hours
        try: 
            time.sleep(12)
            csvfileRaw = ts.get_daily(symbol,outputsize = 'full')

        except:
            try: 
                time.sleep(12)
                csvfileRaw = ts.get_daily(symbol,outputsize = 'full')
            except:
                print("Going into hibernation..."+symbol[0])
                time.sleep(86400) 
                try: 
                    csvfileRaw = ts.get_daily(symbol,outputsize = 'full')
                except:    
                    print('Retrieval error on ' +symbol[0])
                    errors.extend(symbol[0])
        csvfile = list(csvfileRaw)
        
        for rows in csvfile:  
           # try:
                #convert the csv row into a list
                if rows is not None:
                    row = list(rows) 
       
                    try:
                         for cell in row[1:-1]:
                             statement = 'insert into '+symbol[0]+' (timestamp,open,high,low,close,volume) values (\'' + cell[0] + '\' ,' +\
                                     cell[1] + ','+\
                                     cell[2] + ','+\
                                     cell[3] + ','+\
                                     cell[4] + ','+\
                                     cell[5] + ');'

                             sqlStatement(statement)
 
                    except:
                        print("Error with this symbol: "+symbol[0])
                        continue
    numpy.save('ERRORS', errors)

getSymbols_()
