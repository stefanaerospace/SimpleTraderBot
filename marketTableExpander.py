#This is a script that takes a database that has two tables in it (NYSE and NASDAQ)
#   and creates child tables based off of every row they have this way every ticker
#   has a table, this can also be used to update the table with new tickers.

#Future: needs an additional loop to remove defunct tickers

import sqlite3

rows = []

conn = sqlite3.connect('/home/stefan/SimpleTraderBot/markets')
cur = conn.cursor()

def allTickers():   

    #this function gets all of the tickers in the markets 
    #returns a list of tickers that contain strings called rows
    markets = ['NYSE','NASDAQ']
    rows = [] 
    
    for i in markets:
        conn = sqlite3.connect('/home/stefan/SimpleTraderBot/markets')
        
        cur = conn.cursor()
        cur.execute("select Symbol FROM "+i)
        
        #this is an array of all of the things from the query
        rows.extend(cur.fetchall())

    return rows

allTickers()

for symbol in rows:
        
        print(symbol[0]) 
        
        try:
            #try and find a table that has the same name as the symbol 
            cur.execute('select * from [' + symbol[0] +']')
    
        except:
            cur.execute('create table [' + symbol[0] + '] (timestamp text, open real,\
                    high real, low real, close real, volume integer);')
