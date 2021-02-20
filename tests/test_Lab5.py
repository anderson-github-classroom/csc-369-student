import sys
import os
sys.path.append(".")

# Import the student solutions
import Lab5_helper

import pathlib
DIR=pathlib.Path(__file__).parent.absolute()

import joblib 
answers = joblib.load(str(DIR)+"/answers_Lab5.joblib")

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.context import SparkContext

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .getOrCreate()

sc = spark.sparkContext

on_time_df = spark.read.parquet('file:///disk/airline-data-processed/airline-data.parquet')
airlines = spark.read.parquet('file:///disk/airline-data/DOT_airline_codes_table')

def run_exercise_1():
    airline_delay = Lab5_helper.exercise_1(on_time_df).head(10)
    return airline_delay

def run_exercise_2():
    airline_delay = Lab5_helper.exercise_1(on_time_df)
    airline_delay2 = Lab5_helper.exercise_2(airline_delay,airlines)

    return airline_delay2.head(10)

def run_exercise_3(func):
    airline_delay = Lab5_helper.exercise_1(on_time_df)
    airline_delay2 = Lab5_helper.exercise_2(airline_delay,airlines)
    data_for_corr = Lab5_helper.exercise_3(airline_delay2).toPandas()

    return data_for_corr

def test_exercise_1():
    assert set(answers['exercise_1']) == set(run_exercise_1())

def test_exercise_2():
    assert set(answers['exercise_2']) == set(run_exercise_2())

def test_exercise_3():
    assert answers['exercise_3'].equals(run_exercise_3())
