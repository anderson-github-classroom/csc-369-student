# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,md,py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.8.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + [markdown] slideshow={"slide_type": "slide"} hideCode=false hidePrompt=false
# # Chapter 5 - Spark SQL
#
# ## And related data topics
#
# Paul E. Anderson

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Ice Breaker
#
# What was the first concert you ever went to?

# + [markdown] slideshow={"slide_type": "subslide"}
# While this text can be viewed as PDF, it is most useful to have a Jupyter environment. I have an environment ready for each of you, but you can get your own local environment going in several ways. One popular way is with Anaconda (<a href="https://www.anaconda.com/">https://www.anaconda.com/</a>. Because of the limited time, you can use my server.

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Update on lab grades

# + [markdown] slideshow={"slide_type": "subslide"}
# <img src='../labs/grade_summary.png' width=600>

# + [markdown] slideshow={"slide_type": "slide"}
# ## Spark SQL
# * Designed for structured data
# * Seamlessly mix SQL queries with Spark programs
# * Connects to many difference datasources: Hive, Avro, Parquet, ORC, JSON, and JDBC.
# * You can join across datasources

# + [markdown] slideshow={"slide_type": "slide"}
# ## JSON
# * JSON stands for JavaScript Object Notation
# * JSON is simply a way of representing data independent of a platform
# * An alternative to JSON is XML

# + [markdown] slideshow={"slide_type": "subslide"}
# ## XML vs JSON
# * Both are human-readable and machine-readable
# * Most people would agree that JSON is easier to read
# * JSON is faster for computers to process
# * Both contain actual data and meta-information.

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Why are we talking about JSON? I thought this was Spark SQL
# * Spark SQL is all about structured data
# * We will therefore need to talk about different ways to represent structured data

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Example JSON file
# <img src="https://static.goanywhere.com/images/tutorials/read-json/ExampleJSON2.png">

# + [markdown] slideshow={"slide_type": "subslide"}
# ### JSON Syntax
# * Collection of attribute/value pairs enclosed by curly brackets.
# * The attribute is just the name of the attribute surrounded by double quotes (double and not single)
# * The value can be:
#     * a string (in double quotes), 
#     * a number,  
#     * a list of *things* of the same type (in square brackets). 
# * The *thing* can be a string, a number, or a JSON expression.
# * : is put between the attribute and the value and the different attribute/value pairs are separated by comma.

# + [markdown] slideshow={"slide_type": "subslide"}
# ## JSON to Python
# <img src="https://miro.medium.com/max/1484/1*uMSJMK2XLpDBfPABFZ9kTg.png" width=400>

# + [markdown] slideshow={"slide_type": "slide"}
# ## Back into Spark SQL

# + slideshow={"slide_type": "skip"}
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# + [markdown] slideshow={"slide_type": "subslide"}
# Examine: <a href="https://corgis-edu.github.io/corgis/json/covid/">https://corgis-edu.github.io/corgis/json/covid/</a>

# + slideshow={"slide_type": "skip"}
from pathlib import Path
home = str(Path.home())
# -

# ### Requirement of Spark is that JSON is flat

# + slideshow={"slide_type": "fragment"}
import json
data = json.loads(open(f"{home}/csc-369-student/data/corgis/datasets/json/covid/covid.json").read())
open(f"{home}/csc-369-student/data/corgis/datasets/json/covid/covid_flat.json","w").write(json.dumps(data));

# + slideshow={"slide_type": "subslide"}
# spark is an existing SparkSession
df = spark.read.json(f"{home}/csc-369-student/data/corgis/datasets/json/covid/covid_flat.json")
# Displays the content of the DataFrame to stdout
df.show()

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Print the schema in a tree format

# + slideshow={"slide_type": "fragment"}
df.printSchema()

# + [markdown] slideshow={"slide_type": "subslide"}
# ### How do we grab a single column?

# + slideshow={"slide_type": "fragment"}
df.select("Location").show()

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Nested
# -

df.select("Date.Day").show()

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Filtering

# + slideshow={"slide_type": "fragment"}
df.filter(df['Date.Day'] > 21).show()

# + [markdown] slideshow={"slide_type": "subslide"}
# ### GroupBy
# -

df.groupBy("Location.Country").count().show()

# + [markdown] slideshow={"slide_type": "subslide"}
# ### But what happened to the SQL?

# + slideshow={"slide_type": "fragment"}
df.createOrReplaceTempView("covid") # create a temporary view so we can query our data

sqlDF = spark.sql("SELECT * FROM covid")
sqlDF.show()

# + [markdown] slideshow={"slide_type": "subslide"}
# ## A lot to examine
#
# ### Returns dataframe column names and data types
# df.dtypes
# ### Displays the content of dataframe
# df.show()
# ### Return first n rows
# df.head()
# ### Returns first row
# df.first()
# ### Return first n rows
# df.take(5)
# ### Computes summary statistics
# df.describe().show()
# ### Returns columns of dataframe
# df.columns
# ### Counts the number of rows in dataframe
# df.count()
# ### Counts the number of distinct rows in dataframe
# df.distinct().count()

# + slideshow={"slide_type": "fragment"}
df.take(5)

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Convert to Pandas

# + slideshow={"slide_type": "fragment"}
df.toPandas()

# + [markdown] slideshow={"slide_type": "slide"} hideOutput=true
# ## Parquet
# * Column oriented data format where data are stored by column rather than by row.
# * Most expensive operations on hard disks are seeks
# * Related data should be stored in a fashion to minimize seeks
# * Many data driven tasks don't need all the columns of a row, but they do need all the data for a subset of the columns

# + [markdown] slideshow={"slide_type": "subslide"}
# Example of row-oriented:
# <pre>
# 001:10,Smith,Joe,60000;
# 002:12,Jones,Mary,80000;
# 003:11,Johnson,Cathy,94000;
# 004:22,Jones,Bob,55000;</pre>

# + [markdown] slideshow={"slide_type": "subslide"}
# Example of column-oriented
# <pre>
# 10:001,12:002,11:003,22:004;
# Smith:001,Jones:002,Johnson:003,Jones:004;
# Joe:001,Mary:002,Cathy:003,Bob:004;
# 60000:001,80000:002,94000:003,55000:004;</pre>

# + [markdown] slideshow={"slide_type": "subslide"}
# ## And it is as easy as this to work with them in Spark

# + slideshow={"slide_type": "fragment"}
# DataFrames can be saved as Parquet files, maintaining the schema information.
df.write.parquet("/tmp/covid.parquet2")

# Read in the Parquet file created above.
# Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a DataFrame.
parquetFile = spark.read.parquet("/tmp/covid.parquet2")

# + slideshow={"slide_type": "subslide"}
parquetFile.select('Location.Country').show()

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Wrap-up
# In addition to the Spark Core API, Spark provides convienent and flexible mechanisms to access structured data.
