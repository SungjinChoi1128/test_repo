# 코딩테스트
# Problem 1

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np

spark = SparkSession.builder \
    .master('local') \
    .appName('homeowrk') \
    .config('spark.executor.memory', '5gb') \
    .config("spark.cores.max", "6") \
    .getOrCreate()

sc = spark.sparkContext

sqlContext = SQLContext(sc)

df = pd.read_parquet('part-r-00000-3ca9e7a9-e187-4ae3-af34-2f9b7152c905.gz.parquet')

sdf = sqlContext.createDataFrame(df)

sdf.printSchema()

from pyspark.sql import functions as F

columnList = ['00000','00001','00002','00003','00004','00005','00006','00007']

## Pivot table ## pivot function에서의 에러는 해결했지만 show fuction에서의 에러는 해결하지 못했음
final_df = sdf.groupBy('zone_id').pivot('income', columnList).agg(F.count('income').alias('count')).na.fill(0)

final_df.show()

## Pandas Pivot
p_table = pd.pivot_table(df, values=['income'],index=['zone_id'], columns=['income'], aggfunc='count', fill_value=0)
p_table.head()


# Problem 2


test = [['A','B','E'],
        ['C','F','G'],
       ['B', 'D', 'H'],
       ['A','B','C']]


def get_longest(root_str, test, row, col):
    #base case
    if row < 0 or row >= len(test) or col >= len(test[0]) or (row, col) in root_str:
        return 0
    if root_str and ord(test[root_str[-1][0]][root_str[-1][1]])+1 != ord(test[row][col]):
        return 0
    # recursice case
    root_str.append((row,col))
    max_length = 1
    for i in range(row -1, row+2):
        for j in range(col -1, col+2):
            max_length = max(max_length, 1 + get_longest(root_str, test, i, j))
    return max_length

def get_longest_alph(test):
    longest = 0
    
    # for all beginning points, find the longest
    for i in range(len(test)):
        for j in range(len(test[0])):
            current_len = get_longest([], test, i, j)
            longest = max(current_len, longest)
    print('Case 1: ',longest)


get_longest_alph(test)
