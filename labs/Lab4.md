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
# Lab 4 - Matrix Multiplication in Spark using RDDs

Please review Lab 3 before proceeding. 
<!-- #endregion -->

I've noticed during interactions that some folks are skipping the line below. It is my fault for not explaining it. In Python when you import a file it is never reloaded even if the contents change on disk. If you run the cell below before an import, then it will reload automatically for you.

```python slideshow={"slide_type": "skip"}
%load_ext autoreload
%autoreload 2
```

```python slideshow={"slide_type": "skip"}
# make sure your run the cell above before running this
import Lab4_helper
```

<!-- #region slideshow={"slide_type": "-"} -->
### Matrix Multiplication Reviewed
* Critical to a large number of tasks from graphics and cryptography to graph algorithms and machine learning.
* Computationally intensive. A naive sequential matrix multiplication algorithm has complexity of O(n^3). 
* Algorithms with lower computational complexity exist, but they are not always faster in practice.
* Good candidate for distributed processing
* Every matrix cell is computed using a separate, independent from other cells computation. The computation consumes O(n) input (one matrix row and one matrix column).
* Good candidate for being expressed as a MapReduce computation.
* There are many ways to refresh yourself on matrix muliplication. <a href="https://www.youtube.com/watch?v=kuixY2bCc_0&ab_channel=MathMeeting">Here is one such video.</a>
* <a href="https://en.wikipedia.org/wiki/Matrix_multiplication#Definition">Here is a formal definition</a>
<!-- #endregion -->

### Why Spark for matrix multiplication? 
If you've ever tried to perform matrix multiplication and you've run out of memory, then you know one of the reasons we might want to use Spark. In general, it is faster to work with a library such as numpy when the matrices are reasonable in size. We would only see the performance benefits of a Spark approach at scale.


### Creating our input

Creating the input for testing purposes is easy. In practice, we would be reading from files or a database. Please review the documentation on <a href="https://spark.apache.org/docs/2.1.1/programming-guide.html#parallelized-collections">parallelized collections</a>.

In order to run the examples in this notebook on the course JupyterHub, you will need to grab the spark context for yourself. This object is already available in databricks, so you do NOT need to run the code below.

```python
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .getOrCreate()

sc = spark.sparkContext
```

Let $A$ be a matrix of size $m \times n$ and $B$ be a matrix of size $n \times s$. Then our goal is to create a matrix $R$ of size $m \times s$. 

<!-- #region -->
Let's start with a concrete example that is represented in what seems like a reasonable way. In general, we use two dimensional arrays to represent lists. Things like:
```python
[[1,2,3],[4,5,6]]
```
We will do that here, but we will write each row as a key,value pair such as:
```python
[('A[1,:]',[1,2,3]),
 ('A[2,:]',[4,5,6])]
```
We'll switch to different formats later for reasons that you will notice while doing this first exercise. If you haven't seen ``A[1,:]`` it means this is the first row and all the columns of the A matrix. Below is how we create the RDDs.
<!-- #endregion -->

```python
A = [('A[1,:]',[1, 2, 3]),('A[2,:]',[4, 5,6])]
A_RDD = sc.parallelize(A)

B = [('B[:,1]',[7,9,11]),('B[:,2]',[8,10,12])]
B_RDD = sc.parallelize(B)
```

We can convert these into numpy arrays easily.

```python
import numpy as np
A_mat = np.array(A_RDD.map(lambda v: v[1]).collect())
A_mat
```

```python
B_mat = np.array(B_RDD.map(lambda v: v[1]).collect())
B_mat
```

Let's ask numpy to do our multiplication for us. **Error below is on purpose**. The dot product between two vectors:
<img src="https://wikimedia.org/api/rest_v1/media/math/render/svg/5bd0b488ad92250b4e7c2f8ac92f700f8aefddd5">
So numpy will calculate the dot product of two vectors each time an entry (circle in image below) is needed:
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/e/eb/Matrix_multiplication_diagram_2.svg/440px-Matrix_multiplication_diagram_2.svg.png">

```python
np.dot(A_mat,B_mat)
```

We have already transposed B in our example to make our map reduce task easier. The ``.dot`` function assumes we have not done the transpose. So in order for numpy to do the multiplication for us, we need to transpose the second matrix (note the .T).

```python
np.dot(A_mat,B_mat.T)
```

Let's pick apart how we got the value 64. This is the dot product of row 1 of A and column 2 of B (Hint: when we discuss matrix we start counting at 1). Our goal then is to compute $n \times s$ values: one for each pair (i, k) of rows from matrix A and columns from matrix B.

To do this we'll need to join the two RDDs together. Why is the following empty?

```python
A_RDD.join(B_RDD).collect()
```

It's because none of our keys matched. We need to move to a new key, so the join works as expected. Here is what I did:

```python
# These functions needed because we used a string above (don't worry, later I don't do this).
def get_row(s):
    return int(s.split(",")[0].split("[")[-1])

def get_col(s):
    return int(s.split(",")[1].split("]")[0])
```

```python
A2 = A_RDD.map(lambda kv: ("A x B",[get_row(kv[0]),kv[1]]))
A2.collect()
```

```python
B2 = B_RDD.map(lambda kv: ("A x B",[get_col(kv[0]),kv[1]]))
B2.collect()
```

**Exercise 1:**
Using what I have defined above (A_RDD, B_RDD, A2, B2), the Spark functions (join, map, collect), and the numpy function (np.dot or a loop of your own but why do that...), compute the matrix multiplication of A_RDD and B_RDD.

```python
result = Lab4_helper.exercise_1(A_RDD,B_RDD)
result
```

In case you want to put it back in the same format

```python
R_mat = np.zeros((2,2))
for row_col,value in result:
    row,col = row_col
    R_mat[row-1,col-1] = value
R_mat
```

Alternative format 1:

'Matrix name', 'row number', 'column number', 'value'

```python
A = [['A',1,1,1],
     ['A',1,2,0],
     ['A',2,1,3],
     ['A',2,2,4],
     ['A',3,1,0],
     ['A',3,2,6],
     ['A',4,1,7],
     ['A',4,2,8]
    ]
A_RDD = sc.parallelize(A)

B = [['B',1,1,7],
     ['B',1,2,8],
     ['B',1,3,9],
     ['B',2,1,0],
     ['B',2,2,11],
     ['B',2,3,0]
    ]
B_RDD = sc.parallelize(B)
```

```python
A_mat = np.zeros((4,2))
for name,row,col,val in A:
    A_mat[row-1,col-1]=val
A_mat

B_mat = np.zeros((2,3))
for name,row,col,val in B:
    B_mat[row-1,col-1]=val
A_mat,B_mat
```

```python
np.dot(A_mat,B_mat)
```

<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/e/eb/Matrix_multiplication_diagram_2.svg/440px-Matrix_multiplication_diagram_2.svg.png">

```python
from operator import add
A_rows = A_RDD.map(lambda kv: (kv[2],kv))
B_rows = B_RDD.map(lambda kv: (kv[1],kv))
result = A_rows.join(B_rows).map(lambda entry: ((entry[1][0][1],entry[1][1][2]),entry[1][0][-1] * entry[1][1][-1])).reduceByKey(add).collect()
result
```

```python
result_mat = np.zeros((4,3))
for row_col,val in result:
    row,col = row_col
    result_mat[row-1,col-1] = val
result_mat
```

```python
A = [['A',1,1,1],
     ['A',2,1,3],
     ['A',2,2,4],
     ['A',3,2,6],
     ['A',4,1,7],
     ['A',4,2,8]
    ]
A_RDD = sc.parallelize(A)

B = [['B',1,1,7],
     ['B',1,2,8],
     ['B',1,3,9],
     ['B',2,2,11]
    ]
B_RDD = sc.parallelize(B)
```

```python
from operator import add
A_rows = A_RDD.map(lambda kv: (kv[2],kv))
B_rows = B_RDD.map(lambda kv: (kv[1],kv))
result = A_rows.join(B_rows).map(lambda entry: ((entry[1][0][1],entry[1][1][2]),entry[1][0][-1] * entry[1][1][-1])).reduceByKey(add).collect()
result
```

```python
result_mat = np.zeros((4,3))
for row_col,val in result:
    row,col = row_col
    result_mat[row-1,col-1] = val
result_mat
```

```python

```
