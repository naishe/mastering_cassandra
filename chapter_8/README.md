MASTERING APACHE CASSANDRA: CHAPTER 8
-------------------------------------

This chapter is about Cassandra and Hadoop integration. This code base contains Java code that is used in the chapter to execute a simple word count example.

###Prerequisites:

1. Oracle/Sun Java 6 JDK (http://www.oracle.com/technetwork/java/javasebusiness/downloads/java-archive-downloads-javase6-419409.html#jdk-6u45-oth-JPR)
2. Maven build tool (http://maven.apache.org/download.cgi)
3. Cassandra 1.1.x up and running (http://archive.apache.org/dist/cassandra/1.1.11/)
4. Hadoop 1.1.x up and running (http://archive.apache.org/dist/hadoop/core/hadoop-1.1.2/)

###Build the code:

1. cd to this directory. (see a pom.xml?)
2. execute `mvn clean install`
3. there should be a `target` directory generated. This should contain two jar files: `masteringcassandra_ch8-1.0.jar` and `masteringcassandra_ch8-1.0-jar-with-dependencies.jar`

###Executing the code:

Assuming `$HADOOP_HOME` and `$CASSANDRA_HOME` are installation directories of Hadoop and Cassandra respectively.

1. Format HDFS if it not already:
    
        $HADOOP_HOME/bin/hadoop namenode -format

2.  Start Hadoop:

        $HADOOP_HOME/bin/start-all.sh 

3. Start Cassandra: (-f is to start in foreground)

        $CASSANDRA_HOME/bin/cassandra -f

4. Check if all is up execute `jps` (JPS is utility tool to see all the running Java processes. It is a part of Sun/Oracle JDK 6)

        $ jps
        9834 DataNode
        10180 JobTracker
        9519 NameNode
        10086 SecondaryNameNode
        10432 TaskTracker
        8560 CassandraDaemon
        13251 Jps
    
5. Build code as instructed in last section.
6. Load data: (assume code is downloaded under `/home/nishant/Desktop/ch8/`)
  
        java -cp /home/nishant/ch8/target/masteringcassandra_ch8-1.0-jar-with-dependencies.jar in.naishe.mc.extra.LoadData

7. Execute word count.

        $HADOOP_HOME/bin/hadoop jar /home/nishant/Desktop/ch8/target/masteringcassandra_ch8-1.0.jar in.naishe.mc.CassandraWordCount
    
8. View the result, (we'll use CQL3) mind the quote around CF name.

        bin/cqlsh -3
        cqlsh> use testks;
        cqlsh:testks> select * from "resultCF" where key = 'the';
         key | count
        -----+-------
         the |  1664

        cqlsh:testks> select * from "resultCF" limit 10;
         key         | count
        -------------+-------
            subjects |     1
                fine |     2
         immediately |     3
             worried |     1
           execution |     1
               Sends |     1
               claws |     2
                 WAS |     4
                lest |     1
            596-1887 |     1

