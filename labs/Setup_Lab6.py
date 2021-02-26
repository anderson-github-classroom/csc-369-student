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
# # Setup of Lab 6
# -

import pymongo

from pymongo import MongoClient
client = MongoClient()

# +
db = client["csc-369"]

col = db["daily"]
# -

import json
data = json.loads(open('../data/daily.json').read())

x = col.insert_many(data)

x

import pprint
pprint.pprint(list(col.find()))


