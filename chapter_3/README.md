MASTERING APACHE CASSANDRA: CHAPTER 3
-------------------------------------

This is the codebase that is used in snippets of Chapter 3. Please keep this handy while working through that chapter.
It assumes that you have Cassandra 1.1.x and Pycassa installed on your computer. Cassandra is running on default `9160` port.

###1. Organization

  *base_script.py*: It must be executed the first. It creates essential infrastructure for other scripts.

  _ch3* files_: These are the files that demonstrates the full code of snippets in chapter 3.

  _util.py_: It has some utility methods.

  _data directory_: Data directory has some preloaded data and an event generator that is used for creation of data.
  
###2. How to use these files

  Usually, you will only required to read through the codes in _ch3*_ files while reading chapter 3. In case, you wanted to execute this code. Here are the steps:
  
  1. Make sure you have Cassandra up and running locally at port `9160`
  2. cd to this directory
  3. execute base script `python base_script.py`
  4. based on what you are planning to do execute corresponding Python script. For example: `python ch3_1.4.2.1_simple_groups.py`
