---
jupyter:
  jupytext:
    cell_metadata_filter: title,-all
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.2
  kernelspec:
    display_name: exercise2
    language: python
    name: exercise2
---

# Setup
Before starting with the tasks of the assignment, initialize spark.

```python
import sys
from pathlib import Path


sys.path.append(str(Path().resolve()))

```


```python
import operator
import json
from pyspark.sql import SparkSession
from pyspark import RDD, SparkContext

from exercise2.model.review import Review
from exercise2.split_text import split_text
from exercise2.task1.util import calculate_chi_squares, merge_dicts, printable_category, calculate_chi_square_per_token


# spark: SparkSession = SparkSession.builder \
#     .appName("local") \
#     .config("spark.driver.host", "localhost") \
#     .config("spark.driver.bindAddress", "localhost") \
#     .getOrCreate()
spark: SparkSession = SparkSession.builder \
    .appName("cluster") \
    .config("spark.executor.instances", 435) \
    .getOrCreate()
sc: SparkContext = spark.sparkContext

sc.addPyFile(str(Path().resolve()) + "/exercise2.zip")
# external_files = list(Path().glob("exercise2/**/*.py"))
# for file in external_files: 
#     sc.addPyFile(str(file))

p = Path().resolve()
BASE_PATH =  p.parent
BASE_PATH
```
# Example 1: RDD
Redo the first assignment, this time utilizing RDDs. 

Start by loading the reviews dataset:

```python
# path = BASE_PATH / "resource" / "reviews_devset.json" #_first1000.json" #"reviews_devset.json"
# path = "hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json"
path = "hdfs:///user/dic24_shared/amazon-reviews/full/reviewscombined.json"
reviews: RDD = sc.textFile(str(path)).map(json.loads)
reviews_cnt = reviews.count()
```


Once the reviews are available, count the documents per category.

```python
category_counts = reviews.map(lambda r: (r["category"], 1)) \
        .reduceByKey(operator.add) \
        .collectAsMap()
```

Split the reviews text into separate tokens, filter them using the stopwords (loaded from disk)
and reduce the produced counts into a map of maps with token on the top level, each token is 
assigned to a map, which contains the review counts for each category.

```python
with open(BASE_PATH / "resource" / "stopwords.txt", "r") as file:
    stopwords = set([line.strip() for line in file.readlines()])

# <term>: {<cat>: cnt, <cat>: cnt, ...}
category_counts_per_token = reviews \
        .flatMap(lambda r: ([(token, r["category"]) for token in set(split_text(r["reviewText"]))])) \
        .filter(lambda r: r[1] not in stopwords) \
        .mapValues(lambda category: {category: 1}) \
        .reduceByKey(lambda dict1, dict2: merge_dicts(dict1, dict2))
```

Now that we have the counts per token and category, we calculate the chi square values and sort
the values to filter for the top 75 tokens per category.

```python
top75_tokens_per_category: RDD = category_counts_per_token \
        .flatMap(lambda cur_category_counts: calculate_chi_square_per_token(cur_category_counts, category_counts, reviews_cnt)) \
        .groupByKey().mapValues(list) \
        .mapValues(lambda val: sorted(val, key=lambda val: val[1])[:-75:-1]) \
        .sortByKey()
```

Prepare the job result by concenating all tokens to a list of all top tokens 
and convert the lists to strings for printing.

```python
top75_tokens_str = "\n".join(
        top75_tokens_per_category.map(lambda el: printable_category(el[0], el[1])).collect())

top_tokens: list[str] = top75_tokens_per_category \
        .flatMap(lambda el: [tup[0] for tup in el[1]]) \
        .distinct() \
        .sortBy(lambda el: el).collect()
top_tokens_str = " ".join(top_tokens)

result = top_tokens_str + '\n' + top75_tokens_str
# result
```

Write the result to a file.

```python
with open(BASE_PATH / "output_rdd.txt", "w") as file:
    file.writelines(result)
```

# Example 2: DataFrames: Spark ML & Pipelines


```python

```

