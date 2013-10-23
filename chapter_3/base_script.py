#! /usr/bin/env python
'''
Creates keyspace, column family, and performs basic
bootstraping for the examples to run.

This file MUST be executed before running any example
'''
from pycassa.pool import ConnectionPool
from pycassa.system_manager import *
from pycassa.types import *

keyspace = 'mastering_cassandra'

#Open connection
sys = SystemManager('localhost:9160') #assume default installation

print 'dropping and recreating {} keyspace.'.format(keyspace)
try:
  sys.drop_keyspace(keyspace)
except Exception as e:
  #ignore, the keyspace does not exist
  pass

sys.create_keyspace( keyspace, SIMPLE_STRATEGY, {'replication_factor': '1'})

#Create CFs
print 'creating videos CF'
sys.create_column_family(keyspace, 'videos', super=False, comparator_type=UTF8_TYPE)
sys.alter_column(keyspace, 'videos', 'title', UTF8_TYPE)
sys.alter_column(keyspace, 'videos', 'user_name', UTF8_TYPE)
sys.alter_column(keyspace, 'videos', 'runtime_in_sec', INT_TYPE)
sys.alter_column(keyspace, 'videos', 'tags_csv', UTF8_TYPE)

print 'creating tag_videos CF'
sys.create_column_family(keyspace, 'tag_videos', super=False, comparator_type=UTF8_TYPE)

print 'creating tag_videos_sup super-CF'
sys.create_column_family(keyspace, 'tag_videos_sup', super=True, comparator_type=UTF8_TYPE)

print 'creating tag_videos_composite composite column CF'
colNameType = CompositeType(UTF8Type(), UTF8Type()) # (username, videoId)
sys.create_column_family(keyspace, 'tag_videos_composite', super=False, comparator_type=colNameType)

#Add Index
print 'Adding indexes to videos CF'
sys.create_index(keyspace, 'videos', 'user_name', UTF8_TYPE)
sys.create_index(keyspace, 'videos', 'runtime_in_sec', INT_TYPE)

#Create CFs
print 'creating videos_denorm CF'
sys.create_column_family(keyspace, 'videos_denorm', super=False, comparator_type=UTF8_TYPE)
sys.alter_column(keyspace, 'videos_denorm', 'title', UTF8_TYPE)
sys.alter_column(keyspace, 'videos_denorm', 'user_name', UTF8_TYPE)
sys.alter_column(keyspace, 'videos_denorm', 'runtime_in_sec', INT_TYPE)

#Create CFs
print 'creating reverse chronologically sorted event_log CF'
event_log_comp = TimeUUIDType(reversed=True)
sys.create_column_family(keyspace, 'event_log', super=False, comparator_type=event_log_comp)

print 'creating reverse chronologically sorted event_log_mux CF'
event_log_mux_comp = TimeUUIDType(reversed=True)
sys.create_column_family(keyspace, 'event_log_mux', super=False, comparator_type=event_log_mux_comp)

#Close connection
sys.close()

print 'Bootstrapping finished!'
