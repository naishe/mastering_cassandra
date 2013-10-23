MASTERING CASSANDRA CHAPTER 1 CODE NOTES
----------------------------------------

1. The code represents a simple blog application where you can:
 
 a. create a blog

 b. add posts

 c. add comments to the posts

 d. upvote and downvote a post or comment

2. There are three parts of the code

 a. **setup.py** -- creates neccessary Keyspace and Column Families

 b. **crud.py** -- contains functions that perform the tasks listed in #1

 c. **Markov.py** -- uses Markov chains to generate random text sample for demonstration purposes

3. Prerequisites:

 a. Python 2.7.x

 b. Make sure you have Pycassa installed. Ideally, you would do this:
		 
    pip install pycassa

 for more details refer: http://pycassa.github.io/pycassa/installation.html

 c. Make sure you have Cassandra up and running. This example used Cassandra 1.1.11, but any version above Cassandra 1.1.0 is good.

4. Running the example:

 a. Create a setup:

        python setup.py

 b. Run a demonstration

        python crud.py

5. Errors and Exceptions:

 The code is not for production use. It is simplified to understand the concept. It is possible to have some unexpected error.  
 Running the demonstration may cause an issue because Markov.py breaks sometime.  
 A good idea is to rerun setup and crud.

6. Thanks:

 The Markov code is taken from http://agiliq.com/blog/2009/06/generating-pseudo-random-text-with-markov-chains-u/
