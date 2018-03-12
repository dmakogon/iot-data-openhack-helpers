// Databricks notebook source
// MAGIC %md
// MAGIC # Intro to Spark's data components
// MAGIC 
// MAGIC 
// MAGIC Spark has an entire infrastructure built out with the express purpose of optimizing data processing on large workloads. If, for example, you were just performing some simple word-counts and other actions on some small text files, there are lots of tools (including command-line tools) that can probably handle this type of job without much effort. But, instead, imagine trying to perform analysis on all of Wikipedia for common words and phrases, or most frequently-referenced sources. Now, you can imagine a command-line tool being a bit unwieldy to work with. Now, instead of a fixed set of data, imagine a live Twitter firehose stream, where you're trying to identify trending hashtags. Suddenly, local tools are not looking like such a good option. And this is where Spark comes in to play.
// MAGIC 
// MAGIC When working with Spark, the first thing you'll likely experience, when going through various tutorials, is the notion of a Dataframe. But before jumping into Dataframes, it's worth knowing about the basic layers of functionality you'll end up working with.
// MAGIC 
// MAGIC ## Step 0: the Driver
// MAGIC 
// MAGIC Spark is a distributed data processing environment. And if you imagine a cluster with several nodes, all available to help process a block or stream of data, the workload tends to get distributed across those nodes. The distribution, and general job management interface that you'll work with, is the **Driver**. And when you execute specific actions (or submit jobs to the cluster), it's the Driver's responsibility to distribute the activity or job across the clusters.
// MAGIC 
// MAGIC Within the Spark environment, you'll have pre-defined variables. One such variable is `spark`, which is essentially your driver.
// MAGIC 
// MAGIC ## RDD's
// MAGIC 
// MAGIC Spark uses *Resilient Distributed Datasets* (RDDs) to partition data across nodes in a cluster. These are designed so that they can perform actions in parallel. Further, they are designed with resiliency in mind: as they progress through their data processing tasks, they are able to store intermediate results and counters, to persistent storage. This way, in case of a disruption (such as a node rebooting), the cluster can recover from a saved checkpoint and continue, without having to restart the entire operation.
// MAGIC 
// MAGIC While there are many operations and transforms you can perform on an RDD, there isn't much structure to the data. And that is where DataFrames come into play.
// MAGIC 
// MAGIC ## DataFrames
// MAGIC 
// MAGIC A Spark dataframe can be thought of as a table. Similar to a relational data table, with columns and rows. In reality, there aren't exactly *rows*. More like chunks of data (static or streaming), organized into the dataframe in a similar way to how you'd organize data in a database table.
// MAGIC 
// MAGIC Where DataFrames shine is with their ability to let you partition, query, group, order, and aggregate content. There are many additional operations available as well: flattening arrays, substituting default values for missing data, eliminating rows with null values, replacing values within an existing column, on and on.
// MAGIC 
// MAGIC 
// MAGIC ## SQL
// MAGIC 
// MAGIC As it turns out, the Structured Query Language (SQL) has become commonplace. And, to make sure Spark is as capable as the tools that preceded it (such as Hive), Spark now has a very powerful SQL query engine built-in, available for you when performing queries.
// MAGIC 
// MAGIC As `spark` is your entry point into the driver, `spark.sql()` is your way to execute a SQL query. As an example: Imagine a `People`file, read into a data frame. Once this is done, you can create an in-memory table to query, and execute SQL against it:
// MAGIC 
// MAGIC ```
// MAGIC peopleDF.createOrReplaceTempView("people") // this will overwrite an existing table named 'people'
// MAGIC spark.sql("SELECT name, age FROM people WHERE age >= 18")
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ## Learning more
// MAGIC 
// MAGIC  - Spark's introductory programming guide is [here](https://spark.apache.org/docs/latest/sql-programming-guide.html#loading-data-programmatically), giving a great overview of working with RDDs, Datasets, and SQL.
// MAGIC  - Download the free eBook from Databricks, "A Gentle Introduction to Apache Spark", [here](https://pages.databricks.com/gentle-intro-spark.html)
// MAGIC  - Spark has an entire set of programming guides at [spark.apache.org](https://spark.apache.org). A few specific pages that are very helpful:
// MAGIC    - [Quick start](https://spark.apache.org/docs/latest/quick-start.html)
// MAGIC    - [SQL, Datasets, and DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html)
// MAGIC    - [Scala programming guide](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package)
// MAGIC    - [SQL programming guide](https://spark.apache.org/docs/latest/api/sql/index.html)
// MAGIC                                                                                             
// MAGIC                                                  
// MAGIC                                                