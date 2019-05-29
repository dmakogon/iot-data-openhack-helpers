// Databricks notebook source
// MAGIC %md # Parsing JSON data
// MAGIC 
// MAGIC 
// MAGIC In this notebook, we'll work with JSON content within the `Body`, and see how to extract
// MAGIC individual JSON properties, so that we can execute queries on these properties.
// MAGIC 
// MAGIC Note: To simplify this exercise, sample data has been created for you, in `weatherdata-xxxxx.json` (where `xxxxx` represents a zip code), so that you don't need to
// MAGIC create your own weather data simulator. To use this data, upload the json files to an Azure Storage container, and then
// MAGIC provide your storage account credentials below, along with the container you chose for storing these json files.
// MAGIC 
// MAGIC Note: The test data, along with this notebook, is located in GitHub, at  [github.com/dmakogon/iot-data-openhack-helpers](https://github.com/dmakogon/iot-data-openhack-helpers).

// COMMAND ----------

// MAGIC %md
// MAGIC Before anything, let's import required namespaces:

// COMMAND ----------

// First, imports
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC #Setting up Azure Blob storage
// MAGIC 
// MAGIC Here, we are configuring Spark to work with your Azure Storage account, and then setting up your sample data as a streaming source.
// MAGIC 
// MAGIC Note that this will stream all content contained in the named container. In this example, our data is partitioned by zipcode, with each zipcode's data stored in a single file. In a real-world weather data scenario, data would likely be partitioned differently, but this should suffice for demo purposes.

// COMMAND ----------

// Fill in your Azure Storage settings here
spark.conf.set(
  "fs.azure.account.key.<storage-name>.blob.core.windows.net",
  "<storage-key>")

// Connect to blob storage and read all content within the input container into a dataframe:
val inputBlobDF = spark.read
    .json("wasbs://<input-container>@<storage-name>.blob.core.windows.net/")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a peek at a bit of the input data. Note that while it looks like JSON, it's currently just one long string:

// COMMAND ----------

display(inputBlobDF)

// COMMAND ----------

// MAGIC %md
// MAGIC # File system support
// MAGIC 
// MAGIC In case you want to peruse the contents of the blobs we're working with, you can do this directly from spark. For example, here is a file listing of our input container:

// COMMAND ----------

// MAGIC %fs ls "wasbs://<input-container>@<storage-name>.blob.core.windows.net/"

// COMMAND ----------

// MAGIC %md
// MAGIC And we can display the first part of a specific file:

// COMMAND ----------

// MAGIC %fs head "wasbs://<input-container>@<storage-name>.blob.core.windows.net/weatherdata-12345.json"

// COMMAND ----------

// MAGIC %md
// MAGIC # Setting up a JSON schema
// MAGIC 
// MAGIC Let's work with sample weather data, and assume each message body contains the following JSON:
// MAGIC 
// MAGIC  `{ "timestamp": "2018-10-01", "zipcode": "12345", "temperature": 75 }`
// MAGIC 
// MAGIC We can now define a schema which defines each of these properties. The order of the properties in the schema doesn't matter, but the spelling and case *do* matter.

// COMMAND ----------

// Define the schema to apply to our weather data:
val schema = StructType(
           StructField("timestamp", TimestampType) ::
           StructField("zipcode", StringType) ::
           StructField("temperature", IntegerType) :: Nil)

// Apply the schema to our data frame, creating a new data frame.
// Applying the schema lets us decode the `body` field (from the original Event Hubs message) into individual properties,
// as defined by the schema. No need to cast the entire `body` payload to `string`, as we originally did in the
// Intro to EventHubs notebook.
//
// Each JSON object will be rendered into an object in our dataframe. We need to give that object
// a name, for querying purposes. In this example, we're calling it "reading" (a temperature reading).
val schemaDF = inputBlobDF.select(from_json(col("body"), schema).alias("reading"))

// COMMAND ----------

// MAGIC %md
// MAGIC # Peeking at our dataframe
// MAGIC 
// MAGIC Note that Spark has *transforms* and *actions*. Transforms are lazy: nothing happens until an action is executed.
// MAGIC 
// MAGIC Applying a schema? Transform. Displaying content: Action!

// COMMAND ----------

display(schemaDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Note: All of the available data types are documented [here](https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/types/DataType.html). In the above example, the temperature value is set to `IntegerType`, which is a subclass of `NumericType`.

// COMMAND ----------

// MAGIC %md
// MAGIC # Set up temporary table, for querying

// COMMAND ----------

schemaDF.createOrReplaceTempView("weatherdata")

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading: From table
// MAGIC We should now have our temporary table filling with our sample weather data from the JSON file.

// COMMAND ----------

// Observe the data, as parsed into separate columns:
spark.sql("SELECT reading.* from weatherdata").show(truncate=false)


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Note that you can also use a `%sql` cell, as shown in the next example:

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT reading.timestamp, reading.temperature
// MAGIC from weatherdata
// MAGIC where reading.zipcode=22334
// MAGIC and reading.temperature > 65
// MAGIC order by reading.temperature

// COMMAND ----------

// MAGIC %md
// MAGIC You can also use traditional SQL aggregations such as `AVG` and `COUNT`:

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT reading.zipcode,avg(reading.temperature) as AverageTemperature,count(reading.temperature) as SampleCount
// MAGIC from weatherdata
// MAGIC group by reading.zipcode

// COMMAND ----------

// MAGIC %md
// MAGIC # Alternative: Use SQL instead of Scala
// MAGIC 
// MAGIC This example creates a temporary table by reading directly from blob storage into a table.

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS sqlrawweatherdata; 
// MAGIC CREATE TEMPORARY TABLE sqlrawweatherdata
// MAGIC   USING json
// MAGIC   OPTIONS (path "wasbs://<input-container>@<storage-name>.blob.core.windows.net/", mode "FAILFAST");

// COMMAND ----------

// MAGIC %md
// MAGIC Now that the table has been created, we can query it. Note that we will only have a `body` since we don't do any parsing of the incoming content. And since that content was a JSON-formatted document, that's exactly what we see here.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM sqlrawweatherdata;

// COMMAND ----------

// MAGIC %md
// MAGIC Here is another table being created, but this time we will apply a schema.

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP VIEW IF EXISTS sqlweatherdata; 
// MAGIC CREATE TEMPORARY VIEW sqlweatherdata AS
// MAGIC SELECT get_json_object(body,'$.temperature') AS temperature,
// MAGIC        get_json_object(body,'$.zipcode') AS zipcode,
// MAGIC        get_json_object(body,'$.timestamp') AS timestamp
// MAGIC FROM sqlrawweatherdata;

// COMMAND ----------

// MAGIC %md
// MAGIC Now if we query, we will see each individual property as a column.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM sqlweatherdata

// COMMAND ----------

// MAGIC %md
// MAGIC # Writing to storage
// MAGIC 
// MAGIC Let's say we want to write our incoming data to storage instead of a temporary table. In this example, we are reading  sample data from blob storage, but in a real-world application, thousands (millions?) of weather data points would arrive via Event Hubs or Iot Hub, and we'd want to store it for later processing.
// MAGIC 
// MAGIC When storing, we can optionally partition data by a given set of properties. In this example, we will add an additional column, `hour`, that we can include in a partitioning scheme (zipcode + day + hour). In a real-world scenario, you would likely partition by something like year, month, day, and optionally hour, and store more than just the temperature reading (maybe barometric pressure, precipitation, humidity, etc). 

// COMMAND ----------

// Grab needed columns for partitioning. We'll parse down to hour of day within zipcode, as a simple example.
// This effectively grabs 3 columns, creates an additional parsed column called "hour", and then selects
// all columns (including the data we want, along with extra parsed properties for partitioning purposes)
val partitionDF = schemaDF
.select("reading.temperature", "reading.timestamp", "reading.zipcode")
.withColumn("hour", hour(col("timestamp").cast("timestamp"))) // extracting hour from timestap column, into new "hour" column
.select("zipcode", "hour", "temperature") // our final set of columns to work with

// COMMAND ----------

// MAGIC %md
// MAGIC Our new dataframe (`partitionDF`) has been enhanced to contain an `hour` column:

// COMMAND ----------

display(partitionDF)

// COMMAND ----------

// MAGIC %md
// MAGIC At this point, we can write our data to blob storage. First, a simple write, without partitioning:

// COMMAND ----------

partitionDF.write
.option("header","true")
.mode("overwrite")
.option("delimiter",",")
.csv("wasbs://<output-container>@<storage-name>.blob.core.windows.net/alldata")

// COMMAND ----------

// MAGIC %md
// MAGIC Now let's write in a partitioned way:

// COMMAND ----------

partitionDF.write
.option("header","true")
.mode("overwrite")
.option("delimiter",",")
.partitionBy("zipcode","hour")
.csv("wasbs://<output-container>@<storage-name>.blob.core.windows.net/partitiondata")

// COMMAND ----------

// MAGIC %md
// MAGIC If you now browse your storage account, you'd find several folders under the data output folder, each representing a specific zip code. Under these, you'll find additional folders for each hour. You can download and view any of these files.

// COMMAND ----------

// MAGIC %fs ls "wasbs://<output-container>@<storage-name>.blob.core.windows.net/partitiondata"

// COMMAND ----------

// MAGIC %fs ls "wasbs://<output-container>@<storage-name>.blob.core.windows.net/partitiondata/zipcode=12345"

// COMMAND ----------

// MAGIC %fs ls "wasbs://<output-container>@<storage-name>.blob.core.windows.net/partitiondata/zipcode=12345/hour=1"
