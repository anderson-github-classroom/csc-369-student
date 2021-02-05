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

# + [markdown] slideshow={"slide_type": "slide"}
# # Lab 3 - Spark and Databricks
#
# Please review Lab 1 and Lab 2 before proceeding. We will focus on the word counting problem first. Finally, we will move on to a new example using the same dataset. We will then write a function that allows a user to extract the books with the highest count of a list of words.
#
# While you could wrangle your way on this lab using my server, you are much better off heading over to https://community.cloud.databricks.com/. Please do so :)
# -

# I've noticed during interactions that some folks are skipping the line below. It is my fault for not explaining it. In Python when you import a file it is never reloaded even if the contents change on disk. If you run the cell below before an import, then it will reload automatically for you.

# + slideshow={"slide_type": "skip"}
# %load_ext autoreload
# %autoreload 2

# + slideshow={"slide_type": "skip"}
# make sure your run the cell above before running this
import Lab3_helper
# -

# ## Spark
# There is a lot to unpack when being introduced to Spark and Hadoop. We are going to do a little bit at a time.

# ### Databricks
#
# You are going to need to pull data into the distributed filesystem available to you on Databricks. To do this you need to do the following:
# 1. Create a cluster
# 2. Create a notebook for you to do the lab
# 3. You'll need to run the code below in order to get the books onto your system. It downloads a bunch of books into databricks for you. It puts them in dbfs:/data/gutenberg. Databricks provides all kinds of different mechanisms to get data into the cluster. Here is just one way.

#     
# ```python
# dbutils.fs.mkdirs("dbfs:/data/gutenberg")
#
# import urllib.request
# urllib.request.urlretrieve("https://raw.githubusercontent.com/anderson-github-classroom/csc-369-student/main/data/gutenberg/order.txt","/tmp/order.txt")
# lines = open('/tmp/order.txt').read().split("\n")
# for line in lines:
#   try:
#     urllib.request.urlretrieve(f"https://raw.githubusercontent.com/anderson-github-classroom/csc-369-student/main/data/gutenberg/{line}-0.txt",f"/tmp/{line}-0.txt")
#     dbutils.fs.mv(f"file:/tmp/{line}-0.txt",f"dbfs:/data/gutenberg/{line}-0.txt")
#   except: pass
# ```

# You can check out what is in a directory using:

# ```python
#  dbutils.fs.ls("dbfs:/data/gutenberg")
# ```

# The code for counting words from a single file is very much available on the internet :) One such version is the following:
#
# ```python
# from operator import add
#
# lines = sc.textFile("dbfs:/data/gutenberg/1080-0.txt") # read the file into the cluster
#
# counts = lines.flatMap(lambda x: x.split(' ')) \
#               .map(lambda x: (x, 1)) \
#               .reduceByKey(add)
# output = counts.collect()
# ```

# One line at a time. The first line of interest is
#
# ```python
# lines = sc.textFile("dbfs:/data/gutenberg/1080-0.txt") # read the file into the cluster
# ```
#
# One of the variables available to you in a databricks notebook is the <a href="https://spark.apache.org/docs/latest/api/java/org/apache/spark/SparkContext.html#:~:text=Main%20entry%20point%20for%20Spark,before%20creating%20a%20new%20one.">Spark Context</a> (``sc``). This context allows us to interact with a Spark cluster you have started. Databricks handles all of this in the background. The string we pass has an important portion: "dbfs". This tells Spark we are going to load a text file.
#
# The next portion to consider is flatMap. 
#
# > PySpark flatMap() is a transformation operation that flattens the RDD/DataFrame (array/map DataFrame columns) after applying the function on every element and returns a new PySpark RDD/DataFrame. In this article, you will learn the syntax and usage of the PySpark flatMap() with an example.
#
# https://sparkbyexamples.com/pyspark/pyspark-flatmap-transformation/
#
# So flatMap is going to act as our "for" loop and run the first anonymous function on each line. Pretty reasonable that this is a split. Each line ``x`` is a Python string, and we map it to a list of strings using split. So what about the next line that call ``map``. Why map and not flatMap?
#
# > The difference is that the map operation produces one output value for each input value, whereas the flatMap operation produces an arbitrary number (zero or more) values for each input value.
#
# So we need flatMap at first because we are mapping one line to many outputs. We just need map for the second because we are changing a string to a tuple consisting of a string and the integer value of 1. Example: ('the',1). This is perhaps one of the least intuitive steps. Why 1? And why a tuple? The tuple is because we have entered a key-value functional programming paradigm. The key is very important because the data will be shuffled according to this key. In other words, all items with the same key will be grouped together. This is important because we want to count the words, so if we make the key equal to the word, then we can sum them up. The 1 is important because this is the value in the key value. We are going to sum or add up the values to get the total count. Thus the anonymous function inside of the map call returns a key and a value. We then reduce these groups that are organized by keys (our words) and then add up all the values. This is what the final line ``reduceByKey(add)`` does for us. 
#
# Finally, just because we defined counts doesn't mean it actually runs. We need to collect the data in order for it to be executed. This line runs and collects the output:
#
# ```python
# output = counts.collect()
# ```

# ## Word Counting
# Please see previous labs for more information on word counting. 

# **Exercise 1:**
#
# Replace the function ``add`` with a function of your own. What are the arguments and what exactly is <a href="https://spark.apache.org/docs/latest/api/python/_modules/pyspark/rdd.html#RDD.reduceByKey">reduceByKey</a> doing? Call this function exercise_1_add and make sure you put it into Lab3_helper.py.

# + [markdown] slideshow={"slide_type": "subslide"}
# **Exercise 2:**
#
# Write a function that reads all of the books into a single RDD. Call this function exercise_2_load_rdd_all_books and make sure you put it into Lab3_helper.py.
# -

# **Exercise 3:**
#
# Let's do something different now :) Not that different. There is a function to read multiple files, ``sc.wholeTextFiles`` this creates a RDD where each book may be processed separately. Use wholeTextFiles and the ``map`` function to return the word counts for each book. Call this function exercise_3_book_word_counts and make sure you put it into Lab3_helper.py.


