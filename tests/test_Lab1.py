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
    assert Lab1.read_line_at_pos(book_files[1],90) == answers['exercise_1']
    
def test_exercise_2():
    index = Lab1.inverted_index(book_files[0])
    assert set(index['things']) == set(answers['exercise_2'])

def test_exercise_3():
    index = merged_inverted_index(book_files)
    assert set(index.keys()) == set(answers['exercise_3'].keys())
    
def test_exercise_4():
    lines = get_lines(index,'things')
    assert set(lines) == set(answers['exercise_4'])

def test_exercise_5():
    r = os.system('parallel "python lab1_exercise5.py {} > {/}.json" ::: "../data/gutenberg/group1" "../data/gutenberg/group2" "../data/gutenberg/group3"')
    index = Lab1.merge()
    lines = get_lines(index,'things')
    assert r == 0 && set(lines) == set(get_lines(answers["answer_exercise_5"],'things'))