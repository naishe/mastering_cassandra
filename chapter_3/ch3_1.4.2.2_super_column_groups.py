#! /usr/bin/env python
'''
Demonstrates how simple group can be used as one-to-many
relationship using a column family
'''

import util
from pycassa.columnfamily import ColumnFamily

# Load data from data/movies
def loadData():
  con = util.getConnection()
  cf = ColumnFamily(con, 'videos')
  tagCF = ColumnFamily(con, 'tag_videos_sup')
  movies = util.readCSV('data/movies')
  for movie in movies:
    title = movie[0]
    uploader = movie[1]
    runtime = int(movie[2]) #convert to match column validator
    tags = movie[3]
    rowKey = title+":"+uploader


    print "Inserting in videos: {}.".format(str(movie))
    cf.insert(
      rowKey, 
      {
        'title':title,
        'user_name':uploader,
        'runtime_in_sec':runtime,
        'tags_csv': tags
      })
    for tag in tags.split(','):
      print 'adding tag: {0} for movie: {1}'.format(tag, title)
      tagCF.insert(
          tag.strip().lower(),   # row-key = tag name
          {
            uploader: {          # level 1 nesting = uploader name
              rowKey: title      # level 2 nesting = videoId, value = title
             }
          }
      );

  print 'finishished insertion.'    
  con.dispose()

def getByTag(tag):
  print '''-- MOVIES GROUPED BY USER FOR A GIVE TAG --'''
  print '''tag: {}'''.format(tag)
  con = util.getConnection()
  tagCF = ColumnFamily(con, 'tag_videos_sup')

  movies = tagCF.get(tag.strip().lower())
  for key, val in movies.iteritems():
    username = key
    print '''
    {{
      {0}:'''.format(username)
    
    for k, v in val.iteritems():
      print '''\t{{{0}=> {1}}}'''.format(k, v)
    print '''
    }'''

  con.dispose()


if __name__ == '__main__':
  getByTag('action')
