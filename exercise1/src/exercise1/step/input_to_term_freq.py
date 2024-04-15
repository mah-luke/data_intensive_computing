from collections.abc import Generator
from mrjob import step
from mrjob.job import MRJob, MRStep
from mrjob.options import json
from typing import Counter

from exercise1.definitions import BASE_PATH
from exercise1.model.review import Review


class InputToTermFreq(MRStep):
    """Do the first step of the Chi Square calculation:
    create the term frequencies for each category.
    """

    stopwords: set[str] = set(
        open(BASE_PATH / "resource" / "stopwords.txt", "r").readlines()
    )

    def __init__(self, **kwargs):
        super().__init__(
            mapper=self.mapper, combiner=self.combiner, reducer=self.reducer, **kwargs
        )

    def mapper(self, key, value: bytearray):
        """Map the input documents to the terms based on category"""
        parsed: Review = json.loads(value)

        for term in parsed["reviewText"].split(" "):
            if term in self.stopwords:
                continue
            else:
                yield ("term", term), parsed["category"]

        yield ("category", parsed["category"]), 1

    def combiner(self, key: tuple[str, str], values: Generator[any, None, None]):
        """Only return each combination of term and category once"""

        if key[0] == "category":
            yield key, sum(list(values))
        else:
            yield key, next(values)

    def reducer(self, key: str, values: Generator[any, None, None]):
        """Count the occurences of each category per term, this results
        in the count of documents containing each term"""

        if key[0] == "category":
            yield key, sum(list(values))

        else:
            counter = Counter(list(values))
            yield key, counter


class Job(MRJob):
    def steps(self):
        return [InputToTermFreq()]


if __name__ == "__main__":
    Job().run()
    # Job().sandbox(str(BASE_PATH / "resource" / "reviews_devset_first100.json")).run()
