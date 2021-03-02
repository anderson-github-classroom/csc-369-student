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
# # Lab 5 - MongoDB Introduction

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Disclaimer
#
# This lab has you work with current data about COVID-19 infections in the
# United States. This includes information about both infections, and deaths
# due to COVID-19. Some of the data is used to analyze the fatalities
# due to COVID-19 and compare them across time, and across different states.
# I am giving you this assignment because I feel learning about engineering and technology
# should be done with a greater purpose in mind. I understand fully that this
# may be a sensitive issue to some of you. Please reach out to me.
# -

# ## Introduction to pymongo
#
# We will be using pymongo for our mongo labs. For this first lab, we will be working through a subset of this tutorial (https://pymongo.readthedocs.io/en/stable/tutorial.html) on our own dataset.
#
# I have inserted ``data/daily.json`` into the database in the collection called ``daily`` in a database called ``csc-369``. You may gain access to it using the following commands:

from pymongo import MongoClient
client = MongoClient()

# +
db = client["csc-369"]

col = db["daily"]
# -

# You can take a look at one of the records using

import pprint
pprint.pprint(col.find_one())

# ## Information about the data
# The collection contains information about COVID-19
# infections in the United States. The data comes from the COVID Tracking
# Project web site, specifically, from this URL:
#
# https://covidtracking.com/api
#
# We will be using the JSON version of the daily US States data, available
# directly at this endpoint:
#
# https://covidtracking.com/api/states/daily
#
# For the sake of reproducibility, we will be using a data file Dr. Dekhtyar downloaded
# on April 5, that includes all available data from the beginning of the tracking (March 3, 2020) through April 5, 2020. 
#
# The data file is available for download from the course web site.
# The COVID Tracking project Website describes the format of each JSON
# object in the collection as follows:
# * state - State or territory postal code abbreviation.
# * positive - Total cumulative positive test results.
# * positiveIncrease - Increase from the day before.
# * negative - Total cumulative negative test results.
# * negativeIncrease - Increase from the day before.
# * pending - Tests that have been submitted to a lab but no results have
# been reported yet.
# * totalTestResults - Calculated value (positive + negative) of total test
# results.
# * totalTestResultsIncrease - Increase from the day before.
# * hospitalized - Total cumulative number of people hospitalized.
# * hospitalizedIncrease - Increase from the day before.
# * death - Total cumulative number of people that have died.
# * deathIncrease - Increase from the day before.
# * dateChecked - ISO 8601 date of the time we saved visited their website
# * total - DEPRECATED Will be removed in the future. (positive + negative + pending). Pending has been an unstable value and should not count in any totals.
#
# In addition to these attributes, the JSON objects will contain the following
# attributes (explained elsewhere in the API documentation):
# * date - date for which the data is provided in the YYYYMMDD format
# (note: JSON treats this value as a number - make sure you parse
# correctly).
# * fips - Federal Information Processing Standard state code
# * hash - the hash code of the record
# * hospitalizedCurrently - number of people currently hospitalized
# * hospitalizedCumulative - appears to be the new name for the hospitalized attribute
# * inIcuCurrently - number of people currently in the ICU
# * inIcuCumulative - total cumulative number of people who required ICU hospitalization
# * onVentilatorCurrently - number of people currently on the ventilator
# * onVentilatorCumulative - total cumulative number of people who at some point were on ventilator
# * recovered - total cumulative number of people who recovered from COVID-19
#
# Note: ”DEPRECATED” attribute means an attribute that can be found
# in some of the earlier JSON records, that that is not found in the most
# recent ones.

# I've noticed during interactions that some folks are skipping the line below. It is my fault for not explaining it. In Python when you import a file it is never reloaded even if the contents change on disk. If you run the cell below before an import, then it will reload automatically for you.

# + slideshow={"slide_type": "skip"}
# %load_ext autoreload
# %autoreload 2

# + slideshow={"slide_type": "skip"}
# make sure your run the cell above before running this
import Lab6_helper
# -

# **NOTE:** For this lab, I am working directly from the tutorial linked above. Solutions to this lab are modifications of commands you can find there. 

# **Exercise 1:** Use find_one to find a record with an object ID equal to 60392e3656264fee961ca817. As always, put your solution in Lab6_helper.

record = Lab6_helper.exercise_1(col,'60392e3656264fee961ca817')
record

# **Exercise 2:** Use count_documents to count the number of records/documents that have ``state`` equal to 'CA'. 

record = Lab6_helper.exercise_2(col,'CA')
record

# **Exercise 3:** Write a function that returns all of the documents that have a date less than ``d``. Sort the documents by the data, and convert the result to a list.

d = 20200315 # YYYY-MM-DD
record = Lab6_helper.exercise_3(col,d)
record

# Good job!
# Don't forget to push with ./submit.sh