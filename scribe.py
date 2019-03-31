#This file is meant to take in data requests from a requests table in sqlite3
#   So if there is any api retreval needing to be done, this is the script to come to.

import os
import csv
import sqlite3
import time
import contextlib
import numpy
from alpha_vantage.timeseries import TimeSeries

#A log writting function
def logw(symbol, logType):
    
   if logType == 1:
       logFile = open(os.getcwd()+"/logSuccess.txt","a")
       logFile.write(symbol+"\n")
       logFile.close()

   if logType == 0:
       logError = open(os.getcwd()+"/errorLog.txt", "a")
       logError.write(symbol+"\n")
       logError.close()



def sqlStatement(csvfile,symbol):

    pathToDB =  os.getcwd()+'/markets'
    with contextlib.closing(sqlite3.connect(pathToDB,timeout=10)) as conn:
        with contextlib.closing(conn.cursor()) as cursor:
            for cell in csvfile[:0:-1]: #header is stored at end + 1, we don't want the header
                statement = 'insert into '+symbol[0]+' (timestamp,open,high,low,close,volume) values (\'' + cell[0] + '\' ,' +\
                cell[1] + ','+\
                cell[2] + ','+\
                cell[3] + ','+\
                cell[4] + ','+\
                cell[5] + ');'
            cursor.execute(statement)
            cursor.close()
            conn.commit()


#Takes in a list of stock tickers that need to be updated on an intraday basis, 
#   if no list is provided, then it updates all stocks
def getSymbols(someSymbols=None):

    os.chdir("/home/pi/Documents/SimpleTraderBot")
    #connect to the Alpha Vantage Api
    key = open(os.getcwd()+'/key', 'r').read()
    ts = TimeSeries(key,output_format='csv')
     
    ##retrieve the different tickers
    #declaring local symbol storage 
    symbols = []
   
    #if function is not used for a specific set of stocks, update the whole set
    if someSymbols == None:
         #get the list of tickers from the database
         markets = ['NASDAQ']
             
         for i in markets:
             #connect to the sqlite3 db
             con = sqlite3.connect(os.getcwd()+'/markets',timeout=10)
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
    symbol_last_successful='MSFT'#this is used to make sure that the ticker being used 

    for symbol in symbols: 

        #attempt to fetch the stock's information for the day--if not able to reach the API, try again after a minute, if daily limit reached, wait 24 hours 
        csvfileRaw = ts.get_daily(symbol,outputsize = 'full')
        csvfile = list(list(csvfileRaw)[0])
        logw(symbol[0],1)
        logw('    trying to pull data',1)
        if csvfile[0] == ['{']:
               logw('    Minute wait',1) 
               time.sleep(60)
               csvfileRaw = ts.get_daily(symbol,outputsize = 'full')
               csvfile = list(csvfileRaw)[0]
               csvfile = list(csvfile)

               if csvfile[0] == ['{']:
                   time.sleep(60) 
                   #check if the api is actually timed out and the symbol is not just defunct
                   if (list(list(ts.get_daily(symbol_last_successful,outputsize = 'full'))[0])[0]) != ['{']:
                       logw("Not in market" + symbol[0],0)
                       continue
        
                   logw("Going into hibernation..."+symbol[0],1) 
                   logw("    day long wait",1)
                   time.sleep(86400) 

                   csvfileRaw = ts.get_daily(symbol,outputsize = 'full')
                   csvfile    = list(list(csvfileRaw)[0])

                   #if for some reason no result is returned....
                   if csvfile[0]  == ['{']:
    
                       logw('Day Long Wait Error' + symbol[0],0)
                       continue
    
        symbol_last_successful = symbol[0]
        print(symbol)
        #logw(symbol[0],1)
        
        #In case you need a refresher in how Alpha Vantage sends csv objects 
        #print("BROKEN FOR EMERGENCY!")
        #csvfileRaw_temp = ts.get_daily(symbol,outputsize = 'full')
        #nonNullSection = list(csvfileRaw_temp)[0]
        #count = list(nonNullSection) 
        #print('Counter of symbol: ' +str(len(count)))
        ##for i in nonNullSection:
        #    #print(i)
        #break

        #convert the csv row into a list, reverse list and take off csv header text
    
        logw("    Successful pull down, starting write",1)
        sqlStatement(csvfile,symbol)
        #write success to log
        logw(symbol[0],1) 
        logw('    write done, moving on to next symbol',1)
getSymbols()
