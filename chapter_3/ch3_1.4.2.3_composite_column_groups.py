#! /usr/bin/env python
'''
Demonstrates how simple group can be used as one-to-many
relationship using a column family
'''

import util
from pycassa.columnfamily import ColumnFamily
from pycassa.types import *

# Load data from data/movies
def loadData():
  con = util.getConnection()
  cf = ColumnFamily(con, 'videos')
  tagCF = ColumnFamily(con, 'tag_videos_composite')
  movies = util.readCSV('data/movies')
  for movie in movies:
    title = movie[0]
    uploader = movie[1]
    runtime = int(movie[2]) #convert to match column validator
    tags = movie[3]
    rowKey = title+":"+uploader


    print "Inserting in videos: {}.".format(str(movie))
    row = \
      {
        'title':title,
        'user_name':uploader,
        'runtime_in_sec':runtime,
        'tags_csv': tags
      }

    cf.insert(rowKey, row)

    print 'inserting tags: {}'.format(tags)
    for tag in tags.split(','): 
      tagCF.insert( 
          tag.strip().lower(),        #row-key = tag name 
          { 
            (uploader, rowKey): title #(uploader,videoId)=title 
          } 
      );

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
  getByTag('action')
