import operator
import pyspark
import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark import RDD, SparkContext, SparkConf

from exercise2.model.review import Review
from exercise2.split_text import split_text
spark: SparkSession = SparkSession.builder \
    .appName("poc") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "localhost") \
    .getOrCreate()



sc: SparkContext = spark.sparkContext
# conf = SparkConf().setAppName("poc") \
#     .setMaster("local[1]")
# conf.set("spark.driver.host", "localhost")
#
# sc = SparkContext(conf=conf)

BASE_PATH = Path().resolve()
BASE_PATH



path = BASE_PATH / "resource" / "reviews_devset_first1000.json" #"reviews_devset.json"
reviews: RDD[Review] = sc.textFile(str(path)).map(json.loads)

el = reviews.collect().pop()
el


# Count number of reviews per category
category_counts = reviews.map(lambda r: (r["category"], 1)) \
        .reduceByKey(operator.add) \
        .collectAsMap()
category_counts

def to_dict(u: list, v: dict):
    for term in u:


def merge_dicts(dict1: dict, dict2: dict):
    for category, doc_cnt in dict2.items():
        if category not in dict1:
            dict1[category] = doc_cnt
        else:
            dict1[category] += doc_cnt
    return dict1

reviews.map(lambda r: (r["category"], set(split_text(r)))) \
        .aggregateByKey(
            {},
            lambda v, u: {category: u[category] + 1 if category in u else 1 for category in v},
            lambda u1, u2: merge_dicts(u1, u2)
        )



