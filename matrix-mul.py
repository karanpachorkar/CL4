import pyspark

from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("MatrixMultiplication") \
    .getOrCreate()


matrix1 = [
    (0, 0, 2),
    (0, 1, 3),
    (1, 0, 4),
    (1, 1, 5)
]

matrix2 = [
    (0, 0, 6),
    (0, 1, 7),
    (1, 0, 8),
    (1, 1, 9)
]


matrix1_rdd = spark.sparkContext.parallelize(matrix1)
matrix2_rdd = spark.sparkContext.parallelize(matrix2)


result_rdd = matrix1_rdd.flatMap(lambda x: [((x[0], y[1]), x[2] * y[2]) for y in matrix2 if x[1] == y[0]]). \
    reduceByKey(lambda x, y: x + y)



result_df = spark.createDataFrame(result_rdd.map(lambda x: (x[0][0], x[0][1], x[1])), ["row", "col", "result"])


result_df.show()


spark.stop()
