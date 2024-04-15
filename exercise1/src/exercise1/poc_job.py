from collections.abc import Generator
from typing import Counter
from mrjob.job import MRJob
from mrjob.options import json

from exercise1.definitions import BASE_PATH
from exercise1.model.review import Review


class POCJob(MRJob):

    def mapper(self, key, value: bytearray):

        parsed: Review = json.loads(value)

        # terms = set()
        for term in parsed["reviewText"].split(" "):
            if term in stopwords:
                continue
            else:
                yield term, parsed["category"]
                # terms.add(term)
        #
        # for term in terms:
        #     yield term, parsed["category"]

    def combiner(self, key, values: Generator[str, None, None]):
        """ Only return each combination of term and category once"""
        yield key, next(values)

    def reducer(self, key: str, values: Generator[str, None, None]):
        yield key, Counter(list(values))


if __name__ == "__main__":
    stopwords: set[str] = set(
            open(BASE_PATH / "resource" / "stopwords.txt", "r")
            .readlines())
    POCJob.run()
