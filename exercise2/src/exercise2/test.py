from pathlib import Path
import operator
import json
import numpy as np
from pyspark.sql import SparkSession
from pyspark import RDD, SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    IDF,
    ChiSqSelector,
    RegexTokenizer,
    HashingTF,
    StopWordsRemover,
    StringIndexer,
    VectorIndexer,
)

from exercise2.model.review import Review
from exercise2.split_text import split_text
from exercise2.task1.util import (
    calculate_chi_squares,
    merge_dicts,
    printable_category,
    calculate_chi_square_per_token,
)


spark: SparkSession = (
    SparkSession.builder.appName("local")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()
)
# spark: SparkSession = SparkSession.builder \
#     .appName("cluster") \
#     .config("spark.executor.instances", 435) \
#     .getOrCreate()
sc: SparkContext = spark.sparkContext

# sc.addPyFile(str(Path().resolve()) + "/exercise2.zip")
# external_files = list(Path().glob("exercise2/**/*.py"))
# for file in external_files:
#     sc.addPyFile(str(file))


p = Path().resolve()
BASE_PATH = p.parent if p.name == "src" else p
BASE_PATH


path = (
    BASE_PATH / "resource" / "reviews_devset.json"
)  # _first1000.json" #"reviews_devset.json"

df = spark.read.json(str(path))
df.head()


tokenizer = RegexTokenizer(
    minTokenLength=2,
    pattern="[\s\d\(\)\[\]{}\.!\?,;:\+=\-_\"'`~#@&\*%€\$§\\\/]",
    inputCol="reviewText",
    outputCol="tokens",
)
stopwords_remover = StopWordsRemover(
    inputCol=tokenizer.getOutputCol(), outputCol="stopWordsFiltered"
)
indexer = StringIndexer(
    inputCol="category",
    outputCol="indexedCategory"
)
hashingTF = HashingTF(inputCol=stopwords_remover.getOutputCol(), outputCol="TF")
idf = IDF(inputCol=hashingTF.getOutputCol(), outputCol="TFIDF")
chiSqSelector = ChiSqSelector(
    numTopFeatures=2000,
    featuresCol=idf.getOutputCol(),
    labelCol=indexer.getOutputCol(),
    outputCol="chiSq",
)


pipeline = Pipeline(
    stages=[
        tokenizer,
        stopwords_remover,
        indexer,
        hashingTF,
        idf,
        chiSqSelector
    ]
)

model = pipeline.fit(df)
tokenized = model.transform(df)

tokenized.head(n=10)
