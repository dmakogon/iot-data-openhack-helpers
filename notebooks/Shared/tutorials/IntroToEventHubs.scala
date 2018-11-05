// Databricks notebook source
// MAGIC %md # Getting started with Event Hubs + Spark
// MAGIC 
// MAGIC This notebook helps get you set up with various "plumbing" for Event Hubs, Blob storage, and 
// MAGIC Dataframes. And then you'll be able to create your own queries against data streaming through the Event Hubs endpoint.
// MAGIC 
// MAGIC Note: This notebook is written in Scala, but you can also use Python or R for your own projects.
// MAGIC 
// MAGIC Also note: The basics of Spark are all documented online, [here](https://spark.apache.org/docs/latest/), and includes full programming guides and API docs.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC First, we'll set up Event Hubs, which is the same setup for IoT Hubs. To do this, we'll need to first:
// MAGIC 
// MAGIC  - ensure that the Event Hubs SDK has been added as a library, and attached to a running cluster
// MAGIC  - add required import statements (which are equivalent to c#'s "using" statement)
// MAGIC  
// MAGIC ## Spark Connector SDK
// MAGIC The Spark Connector SDK may be found [here](https://github.com/Azure/azure-event-hubs-spark). But there's a much easier way to install the correct driver, if you know its Maven coordinates. 
// MAGIC Note: Maven is a dependency/build manager tool for Java. Similar to Nuget for .net and npm for Node.js. Here are the instructions for installing the correct SDK, based on the Maven coordinates.
// MAGIC 
// MAGIC 
// MAGIC ### Selecting and initializing the correct driver
// MAGIC 
// MAGIC It's important to choose the correct Event Hubs SDK, depending on which version of Spark you're working with.
// MAGIC 
// MAGIC For Databricks, these are the Maven coordinates for the Event Hubs SDK for Databricks:
// MAGIC 
// MAGIC  - Cluster v4.2 and above (Scala 2.11+):  `com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.4`
// MAGIC  
// MAGIC Note: The versioning is periodically updated. To use the latest version, you may choose to search Maven for the latest version.
// MAGIC  
// MAGIC  To install the SDK in Databricks, traverse to the `Shared` folder (or your own personal folder) and select `Create Library`:
// MAGIC  
// MAGIC ![menu for creating library](https://github.com/dmakogon/iot-data-openhack-helpers/blob/master/images/create-library-menu.png?raw=true)
// MAGIC 
// MAGIC Change the source to "Maven Coordinate". Then, to search for the latest driver version, choose "Search Spark Packages and Maven Central":
// MAGIC 
// MAGIC ![select Maven option](https://github.com/dmakogon/iot-data-openhack-helpers/blob/master/images/source-maven.png?raw=true)
// MAGIC 
// MAGIC In the upper-right, the default set of packages is "Spark" - change this to "Maven Central" and type "`eventhubs`" in the Search Packages box. This should present you with a list of packages. Choose the one with Group Id `com.microsoft.azure` and Artifact Id `azure-eventhubs-spark_2.11`. Expand the Releases dropdown and choose the latest (version 2.3.4 currently).
// MAGIC 
// MAGIC 
// MAGIC ![search for Maven package](https://github.com/dmakogon/iot-data-openhack-helpers/blob/master/images/search-packages.png?raw=true)
// MAGIC 
// MAGIC Click the Select button on the far-right, which will return you to the Import form, with all details filled in. Click Create Library.
// MAGIC 
// MAGIC ![menu for creating library](https://github.com/dmakogon/iot-data-openhack-helpers/blob/master/images/maven-create.png?raw=true)
// MAGIC 
// MAGIC You must attach the SDK to a cluster. You will be shown a list of your clusters. Choose whichever cluster(s) you are using, and select the checkbox. You may also choose "Attach automatically to all clusters."
// MAGIC 
// MAGIC ![menu for creating library](https://github.com/dmakogon/iot-data-openhack-helpers/blob/master/images/attach-driver-to-cluster.png?raw=true =300x)
// MAGIC 
// MAGIC At this point, your Event Hubs Spark library is ready to use.
// MAGIC 
// MAGIC ## Imports
// MAGIC Next, we'll define some important import statements, required for the Spark Connector. 
// MAGIC 
// MAGIC ### A word about cells
// MAGIC Notice that these imports are defined in their own *cell*. Cells are similar to functions or methods, in that they are an execution block: if you run a cell, all the instructions in the cell are run.
// MAGIC 
// MAGIC A notebook may have many cells. They all share the same variable scope. That is, if you define variable `foo` in one cell, and run that cell, `foo` is now a valid variable that may be accessed in other cells.
// MAGIC 
// MAGIC Likewise, once you define imports and run the cell with the definition of those imports, you may now run code in any cell that has a dependency on those imports.
// MAGIC 
// MAGIC Bonus: Once you run a cell defining your imports, you don't have to run that cell again, until your cluster is restarted.
// MAGIC 
// MAGIC For a bit more info on cells, take a look at <a href="$./IntroToNotebooks">this notebook</a>.

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.eventhubs._

// COMMAND ----------

// MAGIC %md
// MAGIC # Setting Up Event Hubs / IoT Hub connection
// MAGIC Ok, we have our imports. How, let's set up the Event Hubs or IoT Hub connection. You'll need all of your Event Hub (or IoT Hub) settings for this, from the Azure portal.
// MAGIC 
// MAGIC One setting you might not have configured is the `consumerGroup`. Each Event Hubs endpoint may have multiple consumer groups, with a default consumer group defined when the endpoint is created. You'll want to create your own consumer group, as this gives you your own independent view into the incoming data stream, which does not conflict with others who might also be reading from the same stream. If you haven't done so, please create a new consumer group for yourself.
// MAGIC 
// MAGIC Here is an example of where you'd find the Event Hubs compatible connection string for an IoT Hub, along with Event Hubs name and Consumer Group:
// MAGIC 
// MAGIC ![example of event hubs connection string details](https://github.com/dmakogon/iot-data-openhack-helpers/blob/master/images/event-hubs-settings.png?raw=true =300x)
// MAGIC 
// MAGIC Now, using these properties, set up your connection below, replacing `<foo>` placeholders with your real setting name (without the `<>` brackets, of course).

// COMMAND ----------

// Modify to include your event hubs parameters here
// Note: This code works only with the latest Event Hubs driver,
// which is supported by both Databricks v3.5 & v4.0 and HDInsight v3.5

import org.apache.spark.eventhubs._

val iotConnString = "<YOUR.EVENTHUB.COMPATIBLE.ENDPOINT>"

val ehName = "<YOUR.EVENTHUB.COMPATIBLE.NAME>"

val consumerGroup = "<YOUR.CONSUMER.GROUP>"

// Build connection string with the above information 
val connectionString = ConnectionStringBuilder(iotConnString)
  .setEventHubName(ehName)
  .build

// this sets up our event hubs configuration, including consumer group
val ehConf = EventHubsConf(connectionString)
  .setConsumerGroup(consumerGroup)

// COMMAND ----------

// MAGIC %md
// MAGIC # Connecting to Event Hubs
// MAGIC Ok, now we need to wire up a dataframe to Event Hubs. If you haven't worked with Dataframes before: for the purposes of this exercise, just imagine a very large database table, that allows for operations to be partitioned and performed in parallel, with data that could either be static or streaming in from a live source.
// MAGIC 
// MAGIC For this simple example, we are using Event Hubs as the streaming source of our Dataframe, and taking advantage of the `readStream` function to read directly from Event Hubs. `readStream` is similar to a file object's `read` method that you might have seen in other languages.
// MAGIC 
// MAGIC It is important to understand the difference between `read` function and `readStream`. Simply stated:
// MAGIC `read` => For reading static data or data in batches
// MAGIC `readStream` => For reading streaming data
// MAGIC 
// MAGIC **See also:** [reading data from event hubs](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/structured-streaming-eventhubs-integration.md#reading-data-from-event-hubs)

// COMMAND ----------

// Create a data frame representing the Event Hubs incoming stream
val eventhubsDF = spark
  .readStream
  .format("eventhubs")
  .options(ehConf.toMap)
  .load()

// COMMAND ----------

// MAGIC %md
// MAGIC # Extracting data from Event Hubs
// MAGIC 
// MAGIC Each message coming from Event Hubs has the following schema:
// MAGIC 
// MAGIC | Column | Type |
// MAGIC |----------|----------|
// MAGIC | body           | binary      |
// MAGIC | offset         | string      |
// MAGIC | sequenceNumber | long     |
// MAGIC | enqueuedTime   | timestame |
// MAGIC | publisher | string |
// MAGIC | partitionKey | string |
// MAGIC 
// MAGIC For our purposes, we only need `body`. The issue is, `body` is transmitted as binary data by Event Hubs, by default.  So, we will do a cast to convert this data to a string.

// COMMAND ----------

// create a new dataframe with decoded body as string
val stringbodyDF = eventhubsDF
  .selectExpr("CAST(body as STRING)")

// COMMAND ----------

// MAGIC %md
// MAGIC # Writing: To memory
// MAGIC First thing we'll want to do is write our streaming data *somewhere*, so that we can query a bit of it and see what it looks like. From a dev/test standpoint, the easiest way to get started is to write to an in-memory table.
// MAGIC **see also:** [Structured streaming guide: output sinks](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks)

// COMMAND ----------

// Set up an in-memory table.
// Note: the moment `start()` is called, everything is set into motion, and data will
// begin streaming into our new in-memory table.

val memoryQuery = stringbodyDF.writeStream
    .format("memory")
    .queryName("sampledata") // this is the table name to be used for our in-memory table
    .start()

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading: From memory
// MAGIC We should now have our in-memory table filling with data from our Event Hubs source, which we can now query, to get an idea of what our data looks like.
// MAGIC 
// MAGIC At this point, you can experiment with this query in any way you see fit. Here are two ways to display data coming from `spark.sql()`:

// COMMAND ----------

// if you omit the 'truncate' parameter, it defaults to 'true', 
// which shortens output strings for display purposes
spark.sql("SELECT * from sampledata").show(truncate=false)

// COMMAND ----------

display(spark.sql("SELECT * from sampledata"))

// COMMAND ----------

// MAGIC %md
// MAGIC # Shutting down in-memory table stream
// MAGIC Snce we saved off the stream variable earlier, we can easily shut it down after we're done querying.

// COMMAND ----------

memoryQuery.stop()

// COMMAND ----------

// MAGIC %md
// MAGIC #Data Sources
// MAGIC 
// MAGIC With Spark, you have many options for working with data sources. See <a href="$./IntroToDataSources">this Notebook</a> for more information about data sources.
