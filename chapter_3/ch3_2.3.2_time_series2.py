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
import collections

MUX = 4
startTime = None
userId = None
THREAD_DONE={}

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

  #set a user, some starttime to lookup during get
  global userId
  global startTime
  global THREAD_DONE
  userId = eventsList[0]['user']
  startTime = datetime.datetime.utcnow()

  for i in range(2):
    THREAD_DONE[i] = False #set as running
    t = Thread(target=randomEvent, args=(i, eventsList))
    t.start()


def randomEvent(i, events):
  global THREAD_DONE
  event = random.choice(events)
  con = util.getConnection()
  eventsCF = ColumnFamily(con, 'event_log_mux')

  for j in range(10):
    event = random.choice(events)
    event['insert_time'] = time.time()
    user = event['user']
    muxid = random.randint(1, MUX)
    rowkey = '{0}:{1}'.format(user, muxid)
    timestamp = datetime.datetime.utcnow()
    colval = json.dumps(event)

    print '[Thread:{3}] Inserting: [{0}=> {{{1}:{2}}}]'.format(rowkey, timestamp, colval, i)
    eventsCF.insert(rowkey, {timestamp: colval})

    time.sleep(0.1) #100 milliseconds

  THREAD_DONE[i] = True
  print 'finishished insertion.'    
  con.dispose()

def get():
#################### TEMP
  #userId = 'user-784b9158-5233-454e-8dcf-c229cdff12c6'
  print 'Getting result for userId: {0} between time {1} and {2}'.format(userId, startTime, startTime)
  con = util.getConnection()
  logCF = ColumnFamily(con, 'event_log_mux')
  
  rowKeys = ['{0}:{1}'.format(userId, i+1) for i in range(4)]
  rows = logCF.multiget(rowKeys)
  
  print 'Shows rows multiplexes into different rows each individually sorted in reverse cronological order:'
  merge = {}
  for row in rows:
    print '>> '+str(row) 
    merge = dict(merge.items() + rows[row].items())
    for col in rows[row]:
      colstr = rows[row][col]
      coljson = json.loads(colstr)
      print '\tInsertion Timestamp: {0}'.format(coljson['insert_time'])
  
  final = collections.OrderedDict(sorted(merge.items(), reverse=True))
  for k,v in final.iteritems():
    coljson = json.loads(v)
    print 'insertion timestamp: {0}'.format(coljson['insert_time'])

  """
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
  """

if __name__ == '__main__':
  global THREAD_DONE
  loadData()
  insertDone = False
  while(not insertDone):
    print 'waiting for write...'
    time.sleep(1)
    allDone = True
    for k,v in THREAD_DONE.iteritems():
      allDone = allDone and v
    insertDone = allDone
  get()
