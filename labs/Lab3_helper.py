from operator import add

def exercise_1_add(a,b):
    res = 0
    # Your solution here
    return res

def exercise_2_load_rdd_all_books(sc,dir):
    lines = None
    return lines

def exercise_3_book_word_counts(sc,dir):
    lines = sc.wholeTextFiles(f"{dir}/*.txt") # read the file into the cluster
    res = None
    return res
