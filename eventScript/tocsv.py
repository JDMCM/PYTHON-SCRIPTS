import pyspark
import math
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import array
from pyspark.sql.functions import split
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql import Window
sc = SparkContext('local')
spark = SparkSession(sc)
input1 = input('Enter file path: ').replace('"', '')
#df contains events and the time at which the events occurs


df = spark.read.text(input1)
df = df.withColumn('index', monotonically_increasing_id())
df = df.filter(df.index != 0)




split = split(df.value, ' ')

df = df.withColumn('p1', split.getItem(1).cast(DoubleType())) \
    .withColumn('p2', split.getItem(2).cast(DoubleType())) \
    .withColumn('time', split.getItem(3).cast(DoubleType()))

if split.__sizeof__() > 5:
    df = df.withColumn('p1x', split.getItem(4).cast(DoubleType())) \
        .withColumn('p1y', split.getItem(5).cast(DoubleType())) \
        .withColumn('p1z', split.getItem(6).cast(DoubleType())) \
        .withColumn('p1vx', split.getItem(7).cast(DoubleType())) \
        .withColumn('p1vy', split.getItem(8).cast(DoubleType())) \
        .withColumn('p1vz', split.getItem(9).cast(DoubleType())) \
        .withColumn('p1r', split.getItem(10).cast(DoubleType())) \
        .withColumn('p2x', split.getItem(11).cast(DoubleType())) \
        .withColumn('p2y', split.getItem(12).cast(DoubleType())) \
        .withColumn('p2z', split.getItem(13).cast(DoubleType())) \
        .withColumn('p2vx', split.getItem(14).cast(DoubleType())) \
        .withColumn('p2vy', split.getItem(15).cast(DoubleType())) \
        .withColumn('p2vz', split.getItem(16).cast(DoubleType())) \
        .withColumn('p2r', split.getItem(17).cast(DoubleType())) 

df = df.drop('value').drop('index')

w = Window.orderBy(lit('A'))
df = df.withColumn('index', row_number().over(w))

input2 = input('Enter CSV File Name: ')
df.toPandas().to_csv(input2)




