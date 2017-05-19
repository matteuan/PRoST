# SparkVP
Query large RDF graph with SPARQL using Spark. 
The project is divided in three modules:
+ The **loader** takes as input a file in HDFS, containing a RDF graph, and loads its content using Vertical Partitioning in the Hive Metastore.
+ The **translator** parses a SPARQL query and transform it in a *JoinTree*, an abstract intermediate format that represent the query relations and the join attributes.
+ The **executor** calculates the results of the input query, given in the *JoinTree* format, and execute the GYM algorithm using Spark.

## Limitations
This project is higly experimental. The focus is on the performances of the algorithm that computes the results.
The translator works only with a subset of SPARQL queries, enough to execute the popular queries on the most popular dataset (for this purpose) like YAGO and WatDiv.
