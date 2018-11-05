// Databricks notebook source
// MAGIC %md # Parsing incoming data
// MAGIC 
// MAGIC As shown in the Intro to Event Hubs notebook, data arriving via Event Hubs is stored in the `Body` field of each message.
// MAGIC The `Body` content is binary-encoded. If string-based data is being transmitted, it's necessary
// MAGIC to cast this data into a usable format. The Event Hubs notebook shows how to do this, casting to `string`.
// MAGIC 
// MAGIC In this notebook, we'll work with JSON content within the `Body`, and see how to extract
// MAGIC individual JSON properties, so that we can execute queries on these properties.
// MAGIC 
// MAGIC Note: To simplify this exercise, sample data has been created for you, in `weatherdata-xxxxx.json` (where `xxxxx` represents a zip code), so that you don't need to
// MAGIC create your own weather data simulator. To use this data, upload the json files to an Azure Storage container, and then
// MAGIC provide your storage account credentials below, along with the container you chose for storing these json files.

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
  "fs.azure.account.key.<StorageAccountName>.blob.core.windows.net",
  "<StorageAccountKey>")

// Create a schema for the incoming data, to treat the body as string
val bodySchema = new StructType().add("body", "string")

// Connect to blob storage and treat it as a stream. Specify the storage account name and container name you configured:
val inputBlobStreamDF = spark.readStream.schema(bodySchema)
    .json("wasbs://<containerName>@<StorageAccountName>.blob.core.windows.net/")

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
val schemaDF = inputBlobStreamDF.select(from_json(col("body"), schema).alias("reading"))

// COMMAND ----------

// MAGIC %md
// MAGIC Note: All of the available data types are documented [here](https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/types/DataType.html). In the above example, the temperature value is set to `IntegerType`, which is a subclass of `NumericType`.

// COMMAND ----------

// MAGIC %md
// MAGIC # Set up in-memory table, for querying
// MAGIC 
// MAGIC Just as we did with Event Hubs data, let's stream our JSON file data to an in-memory table, for querying purposes:

// COMMAND ----------

// Set up an in-memory table.
// Note: the moment `start()` is called, everything is set into motion, and data will
// begin streaming into our new in-memory table.

val memoryQuery = schemaDF.writeStream
    .format("memory")
    .queryName("weatherdata") // this is the table name to be used for our in-memory table
    .start()

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading: From memory
// MAGIC We should now have our in-memory table filling with our sample weather data from the JSON file.

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
// MAGIC # Shutting down in-memory table stream
// MAGIC 
// MAGIC We can easily shut our stream down after we're done querying, either by canceling it within the cell where we started it, or by the command below:

// COMMAND ----------

memoryQuery.stop()
