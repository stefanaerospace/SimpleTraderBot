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
def logw(symbol, bitType):
   
   if bitType == 1:
       logFile = open(os.getcwd()+"/logSuccess.txt","a")
       logFile.write(symbol+"\n")
       logFile.close()

   if bitType == 0:
       logError = open(os.getcwd()+"/errorLog.txt", "a")
       logError.write(symbol+"\n")
       logError.close()



#An SQL execution statement
def sqlStatement(statement):
    pathToDB =  os.getcwd()+'/markets'
    with contextlib.closing(sqlite3.connect(pathToDB,timeout=10)) as conn:
        with conn:
            with contextlib.closing(conn.cursor()) as cursor:
                cursor.execute(statement)


#Takes in a list of stock tickers that need to be updated on an intraday basis, 
#   if no list is provided, then it updates all stocks
def getSymbols(someSymbols=None):

    os.chdir("/home/pi/Documents/SimpleTraderBot")
    #connect to the Alpha Vantage REST(-ish?) Api
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
    errors = []

    for symbol in symbols: 
       
        #attempt to fetch the stock's information for the day--if not able to reach the API, try again after a minute, if daily limit reached, wait 24 hours 

       #print("On symbol: "+symbol[0]) 
       csvfileRaw = ts.get_daily(symbol,outputsize = 'full') 
       
       if list(list(csvfileRaw)[0])[0] == ['{']:
              time.sleep(60)
              csvfileRaw = ts.get_daily(symbol,outputsize = 'full')
              
              if (list(list(csvfileRaw)[0])[0]) == ['{']:
                  #check if the api is actually timed out and the symbol is not just defunct
                  if (list(list(cts.get_daily(old_symbol,outputsize = 'full'))[0])[0])  ['{']:
                      logw("Not in market" + symbol,0)
                      continue

                  logw("Going into hibernation..."+symbol[0],1)
                  
                  time.sleep(86400) 
                  csvfileRaw = ts.get_daily(symbol,outputsize = 'full')
                  
                  if (list(list(csvfileRaw)[0])[0]) == ['{']:
    
                      logw('Day Long Wait Error' + symbol[0],0)
                      continue

       csvfile = list(csvfileRaw)
       #convert the csv row into a list
          
       try:
           for rows in csvfile:  
              if rows is not None:
                  row = list(rows) 
                  #start feeding the rows into the sql db
                  for cell in row:
                      statement = 'insert into '+symbol[0]+' (timestamp,open,high,low,close,volume) values (\'' + cell[0] + '\' ,' +\
                               cell[1] + ','+\
                               cell[2] + ','+\
                               cell[3] + ','+\
                               cell[4] + ','+\
                               cell[5] + ');'
                               
                      qlStatement(statement)

           
       except:
           #write failure to log
           logw('SQL Error' + symbol[0],0)
           continue

       #write success to log
       print("Success: "+symbol[0])
       logw(symbol[0],1)

getSymbols()
