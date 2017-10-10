PageRank Checker and MapReduce using Spark and Hadoop on EC2


Implemented two major algorithms PageRank and MapReduce using RDD on Spark.

Steps:

1)	Setup four nodes of AWS EC2 instances and install spark on all of them.
2)	One of them would be Master and other three are Slave nodes
3)	Ensure that there is connectivity between all four nodes.
4)	Copy the jar file that needs to be executed into S3 and submit jar through terminal.

Operations performed:

1)	Implemented PageRank using spark on dataset of Wikipedia articles to find top articles.
2)	Implemented same PageRank algorithm using scala library graphX 
3)	Spark implementation to find top hundred universities list.
4)	On a given word set implemented Word Count
5)	On a given dataset implemented double Word Count
6)      Using distributed Cache, found the frequency of words that are in one list from another list.
