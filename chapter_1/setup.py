from pycassa.system_manager import *
from pycassa.cassandra.ttypes import InvalidRequestException

# Create keyspace
sys = SystemManager('localhost:9160')

print "Dropping 'blog_ks' keyspace"
try:
	sys.drop_keyspace('blog_ks')
except InvalidRequestException as e:
	print "No keyspace 'blog_ks' exists. {0}".format(e,)

print "Creating 'blog_ks' keyspace"	
sys.create_keyspace('blog_ks', SIMPLE_STRATEGY, {'replication_factor': '1'})

# blog_md CF
print "Creating blog metadata CF"
sys.create_column_family('blog_ks', 'blog_md', comparator_type=UTF8_TYPE, key_validation_class=UTF8_TYPE)
sys.alter_column('blog_ks', 'blog_md', 'name', UTF8_TYPE)
sys.alter_column('blog_ks', 'blog_md', 'email', UTF8_TYPE)
sys.alter_column('blog_ks', 'blog_md', 'password', UTF8_TYPE)

# posts CF
print "Creating posts CF"
cf_kwargs0 = {'key_validation_class': TIME_UUID_TYPE, 'comparator_type':UTF8_TYPE}
sys.create_column_family('blog_ks', 'posts', **cf_kwargs0)
sys.alter_column('blog_ks', 'posts', 'title', UTF8_TYPE)
sys.alter_column('blog_ks', 'posts', 'text', UTF8_TYPE)
sys.alter_column('blog_ks', 'posts', 'blog_name', UTF8_TYPE)
sys.alter_column('blog_ks', 'posts', 'author_name', UTF8_TYPE)
sys.alter_column('blog_ks', 'posts', 'timestamp', DATE_TYPE)

# comments CF
print "Creating comments CF"
sys.create_column_family('blog_ks', 'comments', **cf_kwargs0)
sys.alter_column('blog_ks', 'comments', 'comment', UTF8_TYPE)
sys.alter_column('blog_ks', 'comments', 'author', UTF8_TYPE)
sys.alter_column('blog_ks', 'comments', 'timestamp', DATE_TYPE)

# user post time series and post comment time series
print "Creating time series for posts and comments"
cf_kwargs1 = {'comparator_type': LONG_TYPE, 'default_validation_class': TIME_UUID_TYPE, 'key_validation_class': UTF8_TYPE}
cf_kwargs2 = {'comparator_type': LONG_TYPE, 'default_validation_class': TIME_UUID_TYPE, 'key_validation_class': TIME_UUID_TYPE}
sys.create_column_family('blog_ks', 'blog_posts', **cf_kwargs1)
sys.create_column_family('blog_ks', 'post_comments', **cf_kwargs2)

# posts counter
cf_kwargs = {'default_validation_class':COUNTER_COLUMN_TYPE, 'comparator_type': UTF8_TYPE, 'key_validation_class':TIME_UUID_TYPE }
print "Creating vote counter for posts"
sys.create_column_family('blog_ks', 'post_votes', **cf_kwargs)
sys.alter_column('blog_ks', 'post_votes', 'upvotes', COUNTER_COLUMN_TYPE)
sys.alter_column('blog_ks', 'post_votes', 'downvotes', COUNTER_COLUMN_TYPE)

# comments counter
print "Creating vote counter for comments"
sys.create_column_family('blog_ks', 'comment_votes', **cf_kwargs)
sys.alter_column('blog_ks', 'comment_votes', 'upvotes', COUNTER_COLUMN_TYPE)
sys.alter_column('blog_ks', 'comment_votes', 'downvotes', COUNTER_COLUMN_TYPE)
