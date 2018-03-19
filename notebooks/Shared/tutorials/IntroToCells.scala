// Databricks notebook source
// MAGIC %md
// MAGIC # Intro to Notebooks
// MAGIC 
// MAGIC A notebook is comprised of *cells.* Each individual cell is run in its entirety, and has its own output. You can think of a cell as a function or method in common programming languages, in that all of the code within the cell are run as an atomic unit.
// MAGIC 
// MAGIC A key thing to note: All cells of a notebook share a common memory space. That is, if you defined a variable in one cell, then it's available in other cells.
// MAGIC 
// MAGIC Let's look at a small example:

// COMMAND ----------

// Here, we define a simple variable
val greeting = "Hello world!"

// COMMAND ----------

// MAGIC %md
// MAGIC In the cell above, a string, called `greeting`, is defined. To run this cell, either choose <b>"Play"</b> arrow button in the top-right of the cell, or use the `<shift>-<enter>` shortcut while your cursor is anywhere within the cell. You should then see something similar to:
// MAGIC 
// MAGIC `greeting: Strting = Hello world!`
// MAGIC 
// MAGIC Now, in the next cell, we simply print out the greeting. Because all cells share a session, the `greeting` variable should already be defined. Go ahead and run the next cell.

// COMMAND ----------

print(greeting)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Default programming language
// MAGIC Now: when you first created your notebook, you were required to choose a *language* for the notebook: Python, Scala, SQL or R. This cannot be changed, once the notebook is created. However, for any given cell, you may override the language used within that cell.
// MAGIC 
// MAGIC For example: This Notebook's default language is Scala. Let's say you wanted to re-create the demo above with the `greeting` string, but with Python. You can do this, by specifying the desired language at the top of the cell, with a special `%` directive, such as `%python`. The next cell demonstrates this (feel free to run it)

// COMMAND ----------

// MAGIC %python
// MAGIC python_greeting = 'Hello world!'
// MAGIC print(python_greeting)
// MAGIC print(greeting) # this should fail - variable is defined in scala, not python

// COMMAND ----------

// MAGIC %md
// MAGIC ## Using Markdown to provide formatted text like this
// MAGIC If you're creating your own Notebook, and you want to provide formatted text to help document it, you may use the `%md` directive, to specify that a cell contains Markdown. This Notebook makes use of Markdown (including this cell.)

// COMMAND ----------

// MAGIC %md
// MAGIC ## More about running code
// MAGIC 
// MAGIC Instead of just hitting the "Play" button, you may also choose other options, by using the dropdown next to the "Play" button:
// MAGIC 
// MAGIC ![dropdown for Play options](https://github.com/dmakogon/iot-data-openhack-helpers/blob/master/images/play-options.png?raw=true)
// MAGIC 
// MAGIC From a given cell, you can run everything above or below a cell, instead of running just the current cell. Additionally, you might want to run the entire Notebook, in order. 
// MAGIC 
// MAGIC Sometimes, you may want to get into a "clean state", sort of like clearing your cache (this is recommended for when you restart your cluster). You can also clear out all the variables and, then run everything. You'll see all of these options at the top of the notebook:
// MAGIC 
// MAGIC ![dropdown for clear and run options](https://github.com/dmakogon/iot-data-openhack-helpers/blob/master/images/run-all.png?raw=true)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Autocomplete & IntelliSense
// MAGIC 
// MAGIC While true intelliSense doesn't exist in a notebook environment, there is autocomplete. To use autocomplete, simply press `<tab>` and it will give you all the options to apply to an object.
// MAGIC 
// MAGIC For instance, if you press `<tab>` after the period `.`, you will get a list of all the functions that can be applied on the val/var.
// MAGIC 
// MAGIC ![Autocomplete In Notebooks](https://github.com/dmakogon/iot-data-openhack-helpers/blob/master/images/autocomplete.png?raw=true)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Keyboard Shortcuts
// MAGIC 
// MAGIC You don't have to use dropdowns to execute code, or work with Notebooks in general. There is an entire set of keyboard shortcuts (like `<shift>-<enter>` that you already used) at your disposal. Click the keyboard icon, in the top-right of the notebook, to view all shortcuts. Here's a snippet of what you'll see when clicking the keyboard icon:
// MAGIC 
// MAGIC ![keyboard shortcuts](https://github.com/dmakogon/iot-data-openhack-helpers/blob/master/images/keyboard-shortcuts.png?raw=true)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Notebooks and Collaboration
// MAGIC If you're working alone, it's fine to store your Notebooks within your user-specific folder in the `Workspace` area of Databricks. However, if you're collaborating with teammates, it might be better to place this notebook in the `Shared` folder, so that every user has access.
// MAGIC 
// MAGIC Further: Databricks Notebooks are linkable to version control systems such as github, which helps considerably when trying to track changes. In general, this lets you treat Notebooks the same way you'd treat any other source file.
// MAGIC 
// MAGIC For more details on Databricks Github integration, take a look at [this article](https://docs.databricks.com/user-guide/notebooks/github-version-control.html).

// COMMAND ----------

