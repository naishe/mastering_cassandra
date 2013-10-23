#! /usr/bin/env python
'''
Demonstrates how simple group can be used as one-to-many
relationship using a column family
'''

import util
from pycassa.columnfamily import ColumnFamily
from pycassa.types import *
from pycassa.index import *

# Load data from data/movies
def loadData():
  con = util.getConnection()
  cf = ColumnFamily(con, 'videos')
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

  print 'finishished insertion.'    
  con.dispose()

'''
gets all the videos for a given username, with length >= max_length
'''
def getVids(username, max_length=0):
  con = util.getConnection()
  videoCF = ColumnFamily(con, 'videos')
  username_criteria = create_index_expression('user_name', username)
  length_criteria = create_index_expression('runtime_in_sec', max_length, LTE) #LTE? Less than or equals
 
  user_only_clause = create_index_clause([username_criteria], count=3) #just pull 3
  movies_by_user = videoCF.get_indexed_slices(user_only_clause)
  print '''-- movies for username: {}  --'''.format(username)
  print_movie(movies_by_user)

  user_runtime_clause = create_index_clause([username_criteria, length_criteria]) #pull all
  movies_by_user_runtime = videoCF.get_indexed_slices(user_runtime_clause)
  print '''-- movies for username: {} and length <= {} --'''.format(username, max_length)
  print_movie(movies_by_user_runtime)
  
  """
  # Intended to fail
  runtime_clause = create_index_clause([length_criteria]) # At least on equality neccessary!
  movies_by_runtime = videoCF.get_indexed_slices(runtime_clause)
  print '''-- movies for length <= {} --'''.format(max_length)
  print_movie(movies_by_runtime)
  """
  con.dispose()

def print_movie(movies):
    for key, val in movies:
      print '''{{
             user: {0},
             title: {1},
             runtime: {2}
            }}'''.format(val['user_name'], val['title'], val['runtime_in_sec'])
    
if __name__ == '__main__':
  loadData()
  getVids('Sally', 7500)
