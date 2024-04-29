import pyspark
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("StudentGrades") \
    .getOrCreate()


scores = [
    ("Alice", {"Math": 65, "Science": 87, "English": 90}),
    ("Bob", {"Math": 60, "Science": 65, "English": 55}),
    ("Charlie", {"Math": 50, "Science": 95, "English": 70}),
    ("David", {"Math": 80, "Science": 95, "English": 95}),
    ("Eve", {"Math": 75, "Science": 90, "English": 95})
]


scores_rdd = spark.sparkContext.parallelize(scores)


grading_scheme = {
    "A": (80, 100),
    "B": (60, 79),
    "C": (40, 59),
    "D": (0, 39)
}


def compute_grade(score):
    for grade, (lower_bound, upper_bound) in grading_scheme.items():
        if lower_bound <= score <= upper_bound:
            return grade
    return "F"


grades_rdd = scores_rdd.map(lambda x: (x[0], {subject: compute_grade(score) for subject, score in x[1].items()}))


grades_df = spark.createDataFrame(grades_rdd.flatMap(lambda x: [(x[0], subject, grade) for subject, grade in x[1].items()]), ["Student", "Subject", "Grade"])


grades_df.show()


spark.stop()
