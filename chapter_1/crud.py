from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.cassandra.ttypes import NotFoundException
import uuid, time
from Markov import Markov
from random import randint, choice

cpool = ConnectionPool(keyspace='blog_ks', server_list=['localhost:9160'])
blog_metadata = ColumnFamily(cpool, 'blog_md')
posts = ColumnFamily(cpool, 'posts')
comments = ColumnFamily(cpool, 'comments')
blog_posts = ColumnFamily(cpool, 'blog_posts')
post_comments = ColumnFamily(cpool, 'post_comments')
post_votes = ColumnFamily(cpool, 'post_votes')
comment_votes = ColumnFamily(cpool, 'comment_votes')


def add_blog(blog_name, author_name, email, passwd):
	"""Adds a new blog. blog_name is the row_key"""
	blog_metadata.insert(blog_name, {'name': author_name, 'email': email, 'password': passwd})

def add_post(title, text, blog_name, author_name):
	post_id = uuid.uuid1()
	timestamp = int(time.time() * 1e6 )
	posts.insert(post_id, {'title':title, 'text': text, 'blog_name': blog_name, 'author_name': author_name, 'timestamp':int(time.time())})
	blog_posts.insert(blog_name, {timestamp: post_id})
	return post_id

def add_comment(post_id, comment, comment_auth):
	comment_id = uuid.uuid1()
	timestamp = int(time.time() * 1e6)
	comments.insert(comment_id, {'comment': comment, 'author': comment_auth, 'timestamp': int(time.time())})
	post_comments.insert(post_id, {timestamp: comment_id})
	return comment_id

def vote_post(post_id, downvote = False):
	if(downvote):
		post_votes.add(post_id, 'downvotes')
	else:
		post_votes.add(post_id, 'upvotes')

def vote_comment(comment_id, downvote = False):
	if(downvote):
		comment_votes.add(comment_id, 'downvotes')
	else:
		comment_votes.add(comment_id, 'upvotes')

def get_post_list(blog_name, start='', page_size=10):
	"""
		Gets 100 char summary of a blog, votes on each post and next button
	"""
	next = None
	#Get latest 10
	try:
		# gets posts in reverse chronological order. The last column is extra. It is the oldest, and will have lowest timestamp
		post_ids = blog_posts.get(blog_name, column_start = start, column_count = page_size+1, column_reversed = True)
	except NotFoundException as e:
		return ([], next)
	
  # if we have items more than the page size, that means we have the next item
	if(len(post_ids) > page_size):
		#get the timestamp of the oldest item, it will be the first item on the next page
		timestamp_next = min(post_ids.keys())
		next = timestamp_next
		# remove the extra item from posts to show
		del post_ids[timestamp_next]
	
	# pull the posts and votes
	post_id_vals = post_ids.values()
	postlist = posts.multiget(post_id_vals)
	votes = post_votes.multiget(post_id_vals)
	
	# merge posts and votes and yeah, trim to 100 chars. Ideally, you'd want to strip off any HTML tag here.
	post_summary_list = list()
	for post_id, post in postlist.iteritems():
		post['post_id'] = post_id
		post['upvotes'] = 0
		post['downvotes'] = 0
		
		try:
			vote = votes.get(post_id)
			if 'upvotes' in vote.keys():
				post['upvotes'] = vote['upvotes']
			if 'downvotes' in vote.keys():
				post['downvotes'] = vote['downvotes']
		except NotFoundException:
			pass

		text = str(post['text'])
		if(len(text) > 100):
			post['text'] = text[:100] + '... [Read more]'
		else:
			post['text'] = text

		comments_count = 0
		try:
			comments_count = post_comments.get_count(post_id)
		except NotFoundException:
			pass
		
		post['comments_count'] = comments_count

		# Note we do not need to go back to blog metadata CF as we have stored the values in posts CF
		post_summary_list.append(post)

	return (post_summary_list, next)

if( __name__ == '__main__'):
	blog_post_counts = 20
	total_comments = 200
	total_upvotes = 300
	total_downvotes = 100

	textgen = Markov(open('alice_in_wonderland.txt'))
	blogs = ['blog1', 'blog2', 'blog3']
	authors = ['Jim Ellis', 'Sammy Jenkins', 'Marla Singer']
	emails = ['jim@example.com', 'sammyj@doctors.com', 'ms@corn-blue-ties.org']
	passwd = 'passwd1234'

	print "Adding blog metadata"
	for i in range(len(blogs)):
		add_blog(blogs[i], authors[i], emails[i], passwd)
	
	post_ids = list()
	comment_ids = list()
	for i in range(blog_post_counts):
		title = textgen.generate_markov_text(randint(3,10))
		text = textgen.generate_markov_text(randint(30, 500))
		idx = randint(0,len(blogs)-1)
		blog = blogs[idx]
		auth = authors[idx] #yes, we can always pull it from blogs_md CF
		pid = add_post(title, text, blog, auth)
		post_ids.append(pid)

	for i in range(total_comments):
		pid = choice(post_ids)
		comment = textgen.generate_markov_text(randint(3,20))
		cid = add_comment(pid, comment, 'commentor#'+str(i))
		comment_ids.append(cid)
	
	for i in range(total_upvotes):
		vote_post(choice(post_ids))
		vote_comment(choice(comment_ids))

	for i in range(total_downvotes):
		vote_post(choice(post_ids), True)
		vote_comment(choice(comment_ids), True)

	print "-------------- INSERTIONS DONE ----------------"
	
	posts, nxt = get_post_list(blogs[0])
	print """
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
{0}                  
vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
	""".format(blogs[0].upper())

	for post in posts:
		print """
		{0} [votes: +{1}/-{2}]

		{3}
		-- {4} on {5}

		[{6} comment(s)]
		-- * -- -- * -- -- * -- -- * -- -- * -- -- * -- -- * -- -- * -- 
		""".format(str(post['title']).upper(), post['upvotes'], post['downvotes'], post['text'], post['author_name'], post['timestamp'], post['comments_count'])
