'''
Created on Nov 17, 2014

@author: ialshaba
'''

import bz2
import pandas as pd
import numpy
from GeoBases import GeoBase
from datetime import datetime
import re
# first exercise : count the number of lines of files 

bookings = open("../data/bookings.csv", 'r')
for i, line in enumerate(bookings):
    pass
print 'The number of lines for bookings.csv is' , i + 1
bookings.close()

# searches = bz2.BZ2File("../data/searches.csv.bz2", 'r')
searches=open("../data/searches.csv", "r")
for i, line in enumerate(searches):
    pass
print 'The number of lines for searches.csv is' , i + 1
searches.close()


# second exercise : top ten arrival airoprts in the world in 2013 using bookings file
#bookings_reader = pd.read_csv(bookings, sep = '^', usecols = ['source'], chunksize = 10000)


# 
# geo_o= GeoBase(data='ori_por', verbose=False)

# HDFS store filename
hdfile = '../data/groupby.h5'
# Data filename
#bookingsFile = './data/bookings2.csv'
bookingsFile = '../data/bookings.csv'
resultFile  ='../data/topairports.csv'
CREATE_HDFS = True

def createHDhdfile(store_filename, data_filename):
    store = pd.HDFStore(store_filename, mode='w')
    try:
        if 'df' in store:
            store.remove('df')

        bookings_cols = ['pax', 'arr_port']

        for chunk in pd.read_csv(data_filename, dtype={'arr_port': numpy.str_, 'pax': numpy.float32}, chunksize=50000, error_bad_lines=False, sep='^', usecols=bookings_cols):
                #chunk['arr_port'] = chunk['arr_port'].str.strip() 
                store.append('df',chunk, data_columns=True)
    finally:
        store.close()

# Create the HDhdfile
print datetime.now().strftime("%Y-%m-%d %H:%M:%S %f") + ': ' +"Start"    
if CREATE_HDFS:
    
    createHDhdfile(hdfile, bookingsFile)

# Open the created store
store = pd.HDFStore(hdfile, mode='r')

try:
    # get groups

    groups = store.select_column('df','arr_port').unique()

    # create an empty data frame as result
    df = pd.DataFrame(columns=['airport', 'total'], index=groups)

    geo_o = GeoBase(data='ori_por', verbose=False)

    # iterate over groups and apply my operations

    for g in groups:

        grp = store.select('df', where = "arr_port='%s'" % g)

        # Set the sum in the empty dataframe
        total = grp[['pax']].sum()
        df['total'][g] = total['pax']
        df['airport'][g] = geo_o.get(g.strip(), 'name', default="Undefined")
 
    # Sort in descending order

    result = df.sort(['total'], ascending=[0])
    
    # Store result

    result.to_csv(resultFile, sep='\t')
    print datetime.now().strftime("%Y-%m-%d %H:%M:%S %f") + ': ' +"End"
    print "\nTop 10 arrival airports:\n%s" % result[:10]
finally:
    # Release HDFS store
    store.close()


import os
import numpy
import pandas as pd
from datetime import datetime

# HDFS store filename
hdfile = '../data/searches.h5'
# Data filename
#bookingsFile = './data/searches2.csv'
searchesFile = '../data/searches.csv'
CREATE_HDFS = True

def log(msg):
    """To log message with datetime."""
    print datetime.now().strftime("%Y-%m-%d %H:%M:%S %f") + ': ' + msg
    
def createHDhdfileSearches(store_filename, data_filename):
    
    try:
        store = pd.HDFStore(store_filename, mode='w')

        if 'df' in store:
            store.remove('df')

        searches_cols = ['Date', 'Destination']

        for chunk in pd.read_csv(data_filename, dtype={'Date': numpy.str_, 'Destination': numpy.str_}, chunksize=50000, sep='^', usecols=searches_cols):
            #chunk['Destination'] = chunk['Destination'].str.strip()
            try:
                # Drop row with NA value
                chunk = chunk.dropna()
                store.append('df',chunk, data_columns=['dateTime', 'Destination'])
            except ValueError as e:
                chunk.to_csv('../data/error.csv')
                raise
    finally:
        store.close()

def searchesByAirport(destination, store):
    """Count searches by destination airport."""
    log('Select searches for %s destination airport' % destination)
    selection = store.select('df', where=["Destination='%s'" % destination])
    
    # group by month
    log('Convert date column to datetime type (%s)' % destination)
    selection['Date'] = pd.to_datetime(selection['Date'])
    
    log('Group by date with a frequency of one month (%s)' % destination)
    grouped = selection.groupby([pd.Grouper(freq='1M', key='Date')])
    
    # count the number of search
    log('Count the number of searches (%s)' % destination)
    total = grouped.count()
    
    # Rearrange DataFrame
    log('Rearrange DataFrame (%s)' % destination)
    del total['Destination']
    total.columns = [destination]
    
    return total
 
# Create the HDhdfile    
if CREATE_HDFS:
    log('Create HDFS store')
    createHDhdfileSearches(hdfile, searchesFile)

# Open the created store
store = pd.HDFStore(hdfile, mode='r')

try:    
    # Malaga
    malaga = searchesByAirport('AGP', store)
    # Madrid
    madrid = searchesByAirport('MAD', store)
    # Barcelona
    barcelona = searchesByAirport('BCN', store)
    
    # Plot results
    log('Plot results')
    ax = malaga.plot()
    madrid.plot(ax = ax)
    barcelona.plot(ax = ax)
finally:    
    # Release HDFS store
    store.close()
    
