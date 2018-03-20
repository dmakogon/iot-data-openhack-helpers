// Databricks notebook source
// MAGIC %md
// MAGIC # Working with Storage
// MAGIC 
// MAGIC Spark has the ability to read content not only from streaming sources (such as Azure's Event Hubs and IoT Hub), but also from files. Out of the box, Spark supports several file formats, such as `csv`, `json`, `avro`, and `parquet`. Spark also provides the ability for you to work with custom formats.
// MAGIC 
// MAGIC ## Methods for reading and writing
// MAGIC 
// MAGIC Spark provides two general sets of reading & writing methods:
// MAGIC  - `read()`, `write()`, and `save()` - you will use these with static content
// MAGIC  - `readStream()`, `writeStream()`, and `start()` - you will use these with streaming content
// MAGIC  
// MAGIC ## Connecting to Azure
// MAGIC 
// MAGIC When working with content in Azure, you'll first need to configure your Spark session to have a properly-authenticated connection to Azure blob storage. Databricks has fully documented the process, [here](https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html), including details about mounting a blob container as a file system mount.
// MAGIC 
// MAGIC Note that, in the Databricks example, they show how to read a parquet file (`spark.read.parquet()`). Just remember that this is one of many built-in formats, and there is no dependency between Azure and a specific file format.
// MAGIC 
// MAGIC If you are working with a Storage account that is in your subscription, then you'll have access to both the account name and account key. With these two parameters, you may configure Azure storage with those two configuration elements:
// MAGIC 
// MAGIC ```
// MAGIC spark.conf.set(
// MAGIC   "fs.azure.account.key.{YOUR STORAGE ACCOUNT NAME}.blob.core.windows.net",
// MAGIC   "{YOUR STORAGE ACCOUNT ACCESS KEY}")
// MAGIC ```
// MAGIC 
// MAGIC However: If someone else is granting you access to a given container (or if you don't want to embed an entire storage account's key within your app), you'll need to use a Shared Access Signature, which is a  key generated for granting access to a given blob or container. To use a SAS, the call is slightly different:
// MAGIC 
// MAGIC ```
// MAGIC spark.conf.set(
// MAGIC   "fs.azure.sas.{YOUR CONTAINER NAME}.{YOUR STORAGE ACCOUNT NAME}.blob.core.windows.net",
// MAGIC   "{COMPLETE QUERY STRING OF YOUR SAS FOR THE CONTAINER}")
// MAGIC ```
// MAGIC 
// MAGIC At this point, you're all set, and can read and write files to Azure blob storage. For example:
// MAGIC 
// MAGIC ```
// MAGIC val df = spark.read.json("wasbs://{YOUR CONTAINER NAME}@{YOUR STORAGE ACCOUNT NAME}.blob.core.windows.net/{YOUR DIRECTORY NAME}/yourfile.json")
// MAGIC ```
// MAGIC 
// MAGIC ## Additional resources
// MAGIC 
// MAGIC  - [Spark data sources](https://docs.databricks.com/spark/latest/data-sources/index.html)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Moving data from CSV to SQL Azure
// MAGIC In the next paragraph you're going to extracting data from a CSV file stored in an Azure Blob Store, do some basic queries using Spark SQL and then save the loaded data into an Azure SQL database. 

// COMMAND ----------

// MAGIC %md 
// MAGIC Configure spark to access an existing Azure Blob store where the CSV file that we want to import is stored

// COMMAND ----------

spark.conf.set("fs.azure.account.key.openhackspark.blob.core.windows.net",  "xlkvzaPoN5MQvYgT/Yg70s6sEw2KBkrLpiqhbrR9IhHC8gbvP41MeMGjuljPpsAjvCzUn3MIjSaQ/w8oXDoroQ==")

// COMMAND ----------

// MAGIC %md
// MAGIC In order to manipulate data using Spark SQL, specialized function and types needs to be imported

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md
// MAGIC Create a schema for the CSV file so that data can be manipulated more easily and also checked for inconsistencies. The approach of creating the schema *after* having loaded data somewhere, it is called **schema-on-read** as opposed to the **schema-on-write** approach. **Schema-on-write** more suitable where youknow in advance the shape of your data, and you want to make sure that only the data compliant with the schema is loaded.
// MAGIC In an IoT scenario, **the schema-on-read** is usually preferred since it gives more flexbility and favor the idea of storing the data even if we don't know how to deal with it just yet. Think, for example, at the case of adding a shiny new sensor to a set of existing one. The new sensor may return additional data that you don't want to miss, even if your application is not yet ready to deal with it, but it will be in future. 

// COMMAND ----------

val DecimalType = DataTypes.createDecimalType(15, 10)

val schema = StructType(
  StructField("SepalLength", DecimalType, nullable = false) ::
  StructField("SepalWidth", DecimalType, nullable = false) ::
  StructField("PetalLength", DecimalType, nullable = false) ::
  StructField("PetalWidth", DecimalType, nullable = false) ::
  StructField("Class", StringType, nullable = false) ::
  Nil
)

// COMMAND ----------

// MAGIC %md 
// MAGIC Read the file. The file is the famous Iris Dataset taken from the https://archive.ics.uci.edu/ml/datasets/Iris. If you want to start exploring Machine Learning, this is a great dataset to get get started.

// COMMAND ----------

// file originally from 
// https://archive.ics.uci.edu/ml/datasets/iris
val irisDF = sqlContext.read.schema(schema).format("csv").load("wasb://sample-data@openhackspark.blob.core.windows.net/iris.data")

// COMMAND ----------

// MAGIC %md
// MAGIC Show first 10 lines using the *take* method on the created DataFrame

// COMMAND ----------

// Make sure we actualy read something
irisDF.take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Create a temporary view so that we can manipulate data using standard SQL commands, that will make data manipulation much easier

// COMMAND ----------

irisDF.createOrReplaceTempView("iris")

// COMMAND ----------

// MAGIC %md 
// MAGIC Execute a very simple SQL query on created view. Spark SQL supports ANSI SQL:2003 that allows really complexy data manipulation, perfect for data science needs (https://en.wikipedia.org/wiki/SQL:2003)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM iris LIMIT 10

// COMMAND ----------

// MAGIC %md
// MAGIC Note that you can also run your SQL command through `spark`, as the next example shows.

// COMMAND ----------

spark.sql("select * from iris limit 10").show(truncate=false)

// COMMAND ----------

// MAGIC %md
// MAGIC Connect to an Azure SQL database using JDBC driver. 
// MAGIC Make sure you create your own Azure SQL database (https://docs.microsoft.com/en-us/azure/sql-database/sql-database-get-started-portal), and the get the host name, database name, login and password and use in the following code

// COMMAND ----------

val jdbcHostname = "<your host name>.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase ="<your database name>"

// Create the JDBC URL without passing in the user and password parameters.
val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
import java.util.Properties
val connectionProperties = new java.util.Properties()
val jdbcUsername = "<your database user name>"
val jdbcPassword = "<your database password>"
connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")

// Set JDBC Driver
val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
connectionProperties.setProperty("Driver", driverClass)

// COMMAND ----------

// MAGIC %md
// MAGIC Execute a command to check that connection is working properly

// COMMAND ----------

// Let's check if connection with Azure SQL is up and running
// (https://docs.azuredatabricks.net/spark/latest/data-sources/sql-databases.html#push-down-a-query-to-the-database-engine)
val serverName = spark.read.jdbc(jdbcUrl, "(select @@servername as ServerName) t", connectionProperties)

// COMMAND ----------

// MAGIC %md
// MAGIC View the result: it should be the name of the Azure SQL server you are connected to

// COMMAND ----------

display(serverName)

// COMMAND ----------

// MAGIC %md
// MAGIC Copy the Iris Spark table content to Azure SQL. Depending on the specificed *mode* option, target table can be created automatically or not.
// MAGIC - https://docs.azuredatabricks.net/spark/latest/data-sources/sql-databases.html#write-data-to-jdbc
// MAGIC - https://spark.apache.org/docs/2.3.0/api/scala/index.html#org.apache.spark.sql.DataFrameWriter

// COMMAND ----------

import org.apache.spark.sql.SaveMode

// Drop existing table if needed, create a new table and fill it 
spark.sql("select * from iris")
     .write
     .mode(SaveMode.Overwrite)
     .jdbc(jdbcUrl, "iris", connectionProperties)

// COMMAND ----------

// MAGIC %md
// MAGIC Done! Let's check table content, by reading back the table on Azure SQL

// COMMAND ----------

spark.read.jdbc(jdbcUrl, "dbo.Iris", connectionProperties).show()