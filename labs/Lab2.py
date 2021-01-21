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
# # Lab 2 - Creating an inverted index (again) and Word Counting
#
# Please review Lab 1 before proceeding. Part of this lab is creating an inverted index, but using Ray instead of Parallel. We'll then move onto the more complicated word counting example.

# + slideshow={"slide_type": "skip"}
# %load_ext autoreload
# %autoreload 2

# + slideshow={"slide_type": "skip"}
import Lab2_helper

# + slideshow={"slide_type": "skip"}
import ray
ray.init()

# + slideshow={"slide_type": "skip"}
display_available = True
try:
    display('Verifying you can use display')
    from IPython.display import Image
except:
    display=print
    display_available = False
try:
    import pygraphviz
    graphviz_installed = True # Set this to False if you don't have graphviz
except:
    graphviz_installed = False
    
import os
from pathlib import Path
home = str(Path.home())

def isnotebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False      # Probably standard Python interpreter
    
import sys
sys.path.insert(0,f'{home}/csc-369-instructor/tests')

import test_Lab2
# -

# ## Inverted Index

# + [markdown] slideshow={"slide_type": "subslide"}
# **Exercise 1:**
#
# You have already written most of what you need to use Ray to construct distributed inverted indices. Here I want you to modify Lab2_helper.py to use Ray and return the final inverted index. I'm supplying the code that divides your books into three sets.

# + slideshow={"slide_type": "subslide"}
import glob
def get_book_files(input_dir):
    return glob.glob(f"{input_dir}/*.txt")


# + slideshow={"slide_type": "subslide"}
group1 = get_book_files(f"{home}/csc-369-student/data/gutenberg/group1")
group2 = get_book_files(f"{home}/csc-369-student/data/gutenberg/group2")
group3 = get_book_files(f"{home}/csc-369-student/data/gutenberg/group3")

# + slideshow={"slide_type": "subslide"}
index = Lab2_helper.merge([group1,group2,group3])
# -

index['Education']

# + slideshow={"slide_type": "subslide"}
index['Education']
# -

# clean up memory
index = None
import gc
gc.collect()

# ## Word Counting
# Now consider a different problem of common interest. Suppose we have a large corpus (fancy word common in natural language processing) and we want to calculate the number of times a word appears. We could try to hack our inverted index, but let's insert the requirement that this must be a clean implementation. In other words, I'll be manually reviewing your design and making you redo the assignment if it isn't "clean". 

# + [markdown] slideshow={"slide_type": "subslide"}
# **Exercise 2:**
#
# Write a function that counts the words in a book. Output format shown below. You do not have to worry about punctuation and capitalization. In other words, please stick to simple f.readlines() and line.split(" "). Do not strip anything out.
# -

counts = Lab2_helper.count_words(group1[0])

counts

# + [markdown] slideshow={"slide_type": "subslide"}
# **Exercise 3**
#
# Now let's distribute this using Ray. Please implement a function that parallelizes the word counting and subsequent merges.
# -

merged_counts = Lab2_helper.merge_count_words([group1,group2,group3])

merged_counts['things']

# + slideshow={"slide_type": "skip"}
# Don't forget to push!
# -

