---
jupyter:
  jupytext:
    encoding: '# -*- coding: utf-8 -*-'
    formats: ipynb,md,py
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.8.0
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

<!-- #region slideshow={"slide_type": "slide"} -->
# Lab 1 - Creating an inverted index

Overview of inverted indexes: <a href="https://en.wikipedia.org/wiki/Inverted_index">https://en.wikipedia.org/wiki/Inverted_index</a>

In this lab you will create an inverted index for the Gutenberg books. What I want you to do is create a single index that you can quickly return all the lines from all the books that contain a specific word. We will be using the basic and naive split functionality from the chapter (i.e., don't worry about punctuation, etc). Those are details that are not necessary for our exploration into distributed computing. We will use GNU Parallel to distributed our solution.
<!-- #endregion -->

```python slideshow={"slide_type": "skip"}
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
if home == '/home/runner':
    home = os.getcwd()

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
```

<!-- #region slideshow={"slide_type": "subslide"} -->
### Read in the book files for testing purposes
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
from os import path
book_files = []
for book in open(f"{home}/csc-369-student/data/gutenberg/order.txt").read().split("\n"):
    if path.isfile(f'{home}/csc-369-student/data/gutenberg/{book}-0.txt'):
        book_files.append(f'{home}/csc-369-student/data/gutenberg/{book}-0.txt')
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 1:**

Complete the following function that returns a line that is read after seeking to ``pos`` in ``book``.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
def read_line_at_pos(book, pos):
    with open(book,encoding="utf-8") as f:
        # YOUR SOLUTION HERE
        return f.readline()
```

```python slideshow={"slide_type": "subslide"}
if isnotebook():
    line = read_line_at_pos(book_files[0],100)
    # Way to get started!
    line
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Notice that readline reads from the current position until the end of the line.** For the inverted index, you'll want to make sure to record only the positions that get you to the beginning of the line.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
if isnotebook():
    read_line_at_pos(book_files[0],95)
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 2:**

Complete the following function that returns a Python dictionary representing the inverted index. The dictionary should contain an offset that puts the file point at the beginning of the line. 
<!-- #endregion -->

```python
# Read in the file once and build a list of line offsets
def inverted_index(book):
    file = open(book,encoding="utf-8")
    index = {}
    # YOUR SOLUTION HERE
    # Check out https://stackoverflow.com/a/40546814/9864659 for inspiration using seek and tell
    return index
```

```python slideshow={"slide_type": "subslide"}
if isnotebook():
    index = inverted_index(book_files[0])
    index['things']
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 3:**

Write a function that reads all of inverted into a single inverted index in the format shown below.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
def merged_inverted_index(book_files):
    index = {}
    for book in book_files:
        book_index = inverted_index(book)
        # YOUR SOLUTION HERE
        pass
    return index
```

```python slideshow={"slide_type": "subslide"}
if isnotebook():
    index = merged_inverted_index(book_files)
    # Getting there!
```

```python
if isnotebook():
    import pandas as pd
    pd.Series(index.keys())
```

```python
if isnotebook():
    index['things']
```

```python slideshow={"slide_type": "subslide"}
if isnotebook():
    import pandas as pd
    # I am only using pandas here to make this display nicely on our screens
    pd.Series(index['things'])
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 4:**

Write a function that returns all of the lines from all of the books that contain a word. Duplicate lines are correct if the line has more than one occurence of the word. Format shown below.
<!-- #endregion -->

```python
def get_lines(index,word):
    lines = []
    for book in index[word]:
        # YOUR SOLUTION HERE
        pass
    return lines
```

```python
if isnotebook():
    lines = get_lines(index,'things')
    lines
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 5:**

Write a Python script that I can execute using Parallel in the following manner. I have hard coded an example script that will return the incorrect answer, but it will run. Your job is to remove the hard coded answer and insert the correct solution that will produce the correct answer. I have supplied the directory structure, and the parallel commands. You do need to write code that merges the groups back together.
<!-- #endregion -->

**Here are the three groups.** Each directory has about 25 books. We could distribute these to different machines in a cluster, but you get the idea without that step.

```python
!ls -d $HOME/csc-369-student/data/gutenberg/group*
```

```python
!ls $HOME/csc-369-student/data/gutenberg/group1
```

```python
!ls $HOME/csc-369-student/data/gutenberg/group2
```

```python
!ls $HOME/csc-369-student/data/gutenberg/group3
```

**Running a single directory:** You can run a single directory with the following command and store the results to a file.

```python slideshow={"slide_type": "subslide"}
!python Lab1_exercise5.py $HOME/csc-369-student/data/gutenberg/group1 > group1.json
```

We can easily read these back into Python by relying on the JSON format. While more strict than Python dictionaries. They are very similar for our purposes (<a href="https://www.json.org/json-en.html">https://www.json.org/json-en.html</a>. 

```python
import json
if isnotebook():
    group1_results = json.load(open("group1.json"))
    group1_results['things']
```

**You can run the files in parallel using**

```python
!ls $HOME/csc-369-student/data/gutenberg/group1
```

```python
!parallel "python Lab1_exercise5.py {} > {/}.json" ::: "$HOME/csc-369-student/data/gutenberg/group1" "$HOME/csc-369-student/data/gutenberg/group2" "$HOME/csc-369-student/data/gutenberg/group3"
            
```

```python slideshow={"slide_type": "subslide"}
import os
def merge():
    index = {}
    r = os.system('parallel "python Lab1_exercise5.py {} > {/}.json" ::: "$HOME/csc-369-student/data/gutenberg/group1" "$HOME/csc-369-student/data/gutenberg/group2" "$HOME/csc-369-student/data/gutenberg/group3"')
    if r == 0:
        for file in ["group1.json","group2.json","group3.json"]:
            # YOUR SOLUTION HERE
            pass
        os.system("rm group1.json group2.json group3.json")
    return index
```

```python
if isnotebook():
    index = merge()
    # You've done it!
```

```python slideshow={"slide_type": "subslide"}
if isnotebook():
    index['things']
```

This solution should match your solution above that was single thread, but now you are a rockstar distributed computing wizard who could process thousands of books on a cluster with nothing other than simple Python and GNU parallel.

```python slideshow={"slide_type": "skip"}
# Don't forget to push!
```
```python
!rm *.json
```
