// Databricks notebook source
// MAGIC %md In this example, we take lines of text and split them up into words.  Next, we count the number of occurances of each work in the set using a variety of Spark API.

// COMMAND ----------

dbutils.fs.put("/textlines","""
Hello hello world
Hello how are you world
""", true)

// COMMAND ----------

import org.apache.spark.sql.functions._

// Load a text file and interpret each line as a java.lang.String
val ds = sqlContext.read.text("/textlines").as[String]
val result = ds
  .flatMap(_.split(" "))               // Split on whitespace
  .filter(_ != "")                     // Filter empty words
  .toDF()                              // Convert to DataFrame to perform aggregation / sorting
  .groupBy($"value")                   // Count number of occurences of each word
  .agg(count("*") as "numOccurances")
  .orderBy($"numOccurances" desc)      // Show most common words first

display(result)