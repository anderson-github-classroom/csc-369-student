from operator import add
import numpy as np

# These functions needed because we used a string above (don't worry, later I don't do this).
def get_row(s):
    return int(s.split(",")[0].split("[")[-1])

def get_col(s):
    return int(s.split(",")[1].split("]")[0])

def exercise_1(A_RDD,B_RDD):
    result = None
    A2 = A_RDD.map(lambda kv: ("A x B",[get_row(kv[0]),kv[1]]))
    B2 = B_RDD.map(lambda kv: ("A x B",[get_col(kv[0]),kv[1]]))

    # Your solution here
    return result
