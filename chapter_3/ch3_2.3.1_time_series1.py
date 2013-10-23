#! /usr/bin/env python
'''
Demonstrates how simple group can be used as one-to-many
relationship using a column family
'''

import util
from pycassa.columnfamily import ColumnFamily
from pycassa.types import *
import time
from threading import Thread
import json
import datetime
import random

# Load data from data/movies
def loadData():
  events = util.readCSV('data/events.csv')
  eventsList = []

  for event in events:
    e = {
          'user': event[0],
          'page': event[1],
          'event': event[2],
          'element': event[3]
        }
    eventsList.append(e)

  for i in range(2):
    t = Thread(target=randomEvent, args=(i, eventsList))
    t.start()


def randomEvent(i, events):
  event = random.choice(events)
  con = util.getConnection()
  eventsCF = ColumnFamily(con, 'event_log')

  for j in range(50):
    event = random.choice(events)
    rowkey = event['user']
    timestamp = datetime.datetime.utcnow()
    colval = json.dumps(event)

    print '[Thread:{3}] Inserting: [{0}=> {{{1}:{2}}}]'.format(rowkey, timestamp, colval, i)
    eventsCF.insert(rowkey, {timestamp: colval})

    time.sleep(0.1) #100 milliseconds

  print 'finishished insertion.'    
  con.dispose()

def getByTag(tag):
  print '''-- MOVIES GROUPED BY USER FOR A GIVE TAG --'''
  print '''tag: {}'''.format(tag)
  con = util.getConnection()
  tagCF = ColumnFamily(con, 'tag_videos_composite')

  movies = tagCF.get(tag.strip().lower())
  for key, val in movies.iteritems():
    compositeCol = key
    print '([{0}],[{1}]) => {2}'.format(compositeCol[0], compositeCol[1], val)
    
  movieSlice = tagCF.get(tag.strip().lower(), column_start=("Kara", "The Croods:Kara"), column_finish=("Sally","Gx" ))
  #movieSlice = tagCF.get(tag.strip().lower(), column_start=("Kara", ), column_finish=(("Leo Scott",False),))
  print '-- SLICES --'
  for key, val in movieSlice.iteritems():
    compositeCol = key
    print '([{0}],[{1}]) => {2}'.format(compositeCol[0], compositeCol[1], val)
    

  con.dispose()


if __name__ == '__main__':
  loadData()
  #getByTag('action')
