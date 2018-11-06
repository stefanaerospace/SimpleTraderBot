#This file is meant to take in data requests from a requests table in sqlite3
#   So if there is any api retreval needing to be done, this is the script to come to.

import csv
import sqlite3
import time
import contextlib
import numpy
from alpha_vantage.timeseries import TimeSeries

#A log writting function
def logw(symbol, bitType):
   
    #This writes to a success or error log, the control flow around it allows 
    #   for remote access to the same file without failing the script (just in case).
    for i in range(0,100):
        done = False
        while True:

            try:

                if bitType == 1:
                    logFile = open("/home/pi/Documents/SimpleTraderBot-master/logSuccess.txt","a")
                    logFile.write(symbol+"\n")
                    logFile.close()

                if bitType == 0:
                    logError = open("/home/pi/Documents/SimpleTraderBot-master/errorLog.txt", "a")
                    logError.write(symbol+"\n")
                    logError.close()

                    done = True
            except:
                sleep(90)
                continue
            break

        if done ==True:
            break
    

#An SQL execution statement
def sqlStatement(statement):
    pathToDB =  '/home/pi/Documents/SimpleTraderBot-master/markets'
    with contextlib.closing(sqlite3.connect(pathToDB,timeout=10)) as conn:
        with conn:
            with contextlib.closing(conn.cursor()) as cursor:
                cursor.execute(statement)


#Takes in a list of stock tickers that need to be updated on an intraday basis, 
#   if no list is provided, then it updates all stocks
def getSymbols_(someSymbols=None):

    #connect to the Alpha Vantage REST(-ish?) Api
    key = open('/home/pi/Documents/SimpleTraderBot-master/key', 'r').read()
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
             con = sqlite3.connect('/home/pi/Documents/SimpleTraderBot-master/markets',timeout=10)
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
           time.sleep(20)
           csvfileRaw = ts.get_daily(symbol,outputsize = 'full')

       except:
           try: 
               time.sleep(20)
               csvfileRaw = ts.get_daily(symbol,outputsize = 'full')
           except:
               print("Going into hibernation..."+symbol[0])
               time.sleep(86400) 
               try: 
                   csvfileRaw = ts.get_daily(symbol,outputsize = 'full')
               except:    
                   print('Retrieval error on ' +symbol[0])
                   logw(symbol[0],0)
       csvfile = list(csvfileRaw)
       
       #convert the csv row into a list
          
       try:
           for rows in csvfile:  
              if rows is not None:
                  row = list(rows) 

                  #start feeding the rows into the sql db
                  for cell in row[1:-1]:
                      statement = 'insert into '+symbol[0]+' (timestamp,open,high,low,close,volume) values (\'' + cell[0] + '\' ,' +\
                               cell[1] + ','+\
                               cell[2] + ','+\
                               cell[3] + ','+\
                               cell[4] + ','+\
                               cell[5] + ');'
                       
                      sqlStatement(statement)
                      print(statement)
                      break 
           #wirte success to log
           logw(symbol[0],1)

           
       except:
           #write failure to log
           logw(symbol[0],0)
           continue
getSymbols_()
