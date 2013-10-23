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
  tagCF = ColumnFamily(con, 'tag_videos')
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
          tag.strip().lower(),
          {
            uploader+ "_" + rowKey: title
          }
      );

  print 'finishished insertion.'    
  con.dispose()

def getByTag(tag):
  con = util.getConnection()
  vidCF = ColumnFamily(con, 'videos')
  tagCF = ColumnFamily(con, 'tag_videos')

  movies = tagCF.get(tag.strip().lower())
  for key, val in movies.iteritems():
    vidId = key.split('_')[1]
    movieDetail = vidCF.get(vidId)
    print '''
    {{
      user: {0},
      movie: {1},
      tags: {2}
    }}'''.format(movieDetail['user_name'], movieDetail['title'], movieDetail['tags_csv'])

  con.dispose()


if __name__ == '__main__':
  getByTag('action')
