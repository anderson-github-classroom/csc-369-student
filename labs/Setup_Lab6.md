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
# Setup of Lab 6
<!-- #endregion -->

```python
import pymongo
```

```python
from pymongo import MongoClient
client = MongoClient()
```

```python
db = client["csc-369"]

col = db["daily"]
```

```python
import json
data = json.loads(open('../data/daily.json').read())
```

```python
x = col.insert_many(data)
```

```python
x
```

```python
import pprint
pprint.pprint(list(col.find()))
```

```python

```
