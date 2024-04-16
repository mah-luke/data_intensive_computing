from collections.abc import Generator
from mrjob.job import MRJob, MRStep
from mrjob.options import json
from typing import Counter
import logging
import itertools
import sys

from exercise1.model.review import Review

LOG = logging.getLogger()

class InputToTermFreq(MRStep):
    """Do the first step of the Chi Square calculation:
    create the term frequencies for each category.
    """


    def __init__(self, **kwargs):
        super().__init__(
            mapper=self.mapper, combiner=self.combiner, reducer=self.reducer, **kwargs
        )

    def mapper(self, _, value: bytearray):
        """Map the input documents to the terms based on category"""
        parsed: Review = json.loads(value)

        stopwords: set[str] = set(
            open("exercise1/stopwords.txt", "r").readlines()
        )

        terms: set[str] = set()
        for term in parsed["reviewText"].split(" "):
            if term in stopwords:
                continue
            else:
                terms.add(term)
        for term in list(terms):
            yield f"term_{term}", parsed["category"]

        yield "category", parsed["category"]

    def combiner(self, key: str, values: Generator):
        """Only return each combination of term and category once"""

        if key.startswith("category"):
            yield key, (None, list(values))
        else:
            values_list = list(values)
            yield key, values_list
            yield "category", (key.split("_", 1)[1], None)

    def reducer(self, key: str, values: Generator):
        """Count the occurences of each category per term, this results
        in the count of documents containing each term"""

        if key.startswith("category"):
            values_list: list[dict] = list(values)
            print(f"Key: {key}, Values: {values_list}", file=sys.stderr)
            categories: list[str] = []
            terms: list[str] = []

            #TODO: somewhere here is bug which results in empty categories
            for tup in values_list:
                if tup[0] is not None:
                    terms += [tup[0]]
                if tup[1] is not None:
                    categories += tup[1]

            print(f"After concat: {categories}", file=sys.stderr)

            cat_counter: dict[str, int] = dict(Counter(categories))

            print(f"After counting: {cat_counter}", file=sys.stderr)
            print(f"Terms: {terms}", file=sys.stderr)
            yield key, {"categories": cat_counter,
                        "terms": list(set(terms)),
                        "total_documents": sum(cat_counter.values())}
        else:
            counter: dict[str, int] = dict(Counter(list(itertools.chain(*list(values)))))
            print(f"Key: {key}, counter_values: {counter}", file=sys.stderr)
            yield key, counter


class Job(MRJob):
    def steps(self):
        return [InputToTermFreq()]


if __name__ == "__main__":
    Job().run()
