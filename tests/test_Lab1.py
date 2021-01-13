import sys
import os
sys.path.append(".")

# Import the student solutions
import Lab1

book_files = Lab1.book_files

import pathlib
DIR=pathlib.Path(__file__).parent.absolute()

import joblib
answers = joblib.load(str(DIR)+"/answers_Lab1.joblib")

def test_exercise_1():
    assert Lab1.read_line_at_pos(book_files[1],100) == answers['exercise_1']
    
def test_exercise_2():
    index = Lab1.inverted_index(book_files[0])
    assert set(index['things']) == set(answers['exercise_2'])

def test_exercise_3():
    index = Lab1.merged_inverted_index(book_files)
    assert set(index.keys()) == answers['exercise_3']
    
def test_exercise_4():
    lines = Lab1.get_lines(index,'things')
    assert set(lines) == set(answers['exercise_4'])

def test_exercise_5():
    index = Lab1.merge()
    lines = get_lines(index,'things')
    assert set(lines) == answers['exercise_5']
