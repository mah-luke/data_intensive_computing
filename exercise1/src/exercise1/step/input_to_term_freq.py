from collections.abc import Generator
from mrjob.job import MRJob, MRStep
from mrjob.options import json
from typing import Counter
import itertools

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

    def mapper(self, _, value: bytearray):
        """Map the input documents to the terms based on category"""
        parsed: Review = json.loads(value)

        terms: set[str] = set()
        for term in parsed["reviewText"].split(" "):
            if term in self.stopwords:
                continue
            else:
                terms.add(term)
        for term in list(terms):
            yield ("term", term), parsed["category"]

        yield ("category",), parsed["category"]

    def combiner(self, key: tuple[str, str], values: Generator):
        """Only return each combination of term and category once"""

        if key[0] == "category":
            yield key, (None, list(values))
        else:
            values_list = list(values)
            yield key, values_list
            yield ("category",), (key[1], None)

    def reducer(self, key: str, values: Generator):
        """Count the occurences of each category per term, this results
        in the count of documents containing each term"""

        if key[0] == "category":
            values_list: list[dict] = list(values)
            categories: list[str] = []
            terms: list[str] = []
            for tup in values_list:
                if tup[0] is not None:
                    terms.append(tup[0])
                elif tup[1] is not None:
                    categories += (tup[1])

            counter: dict[str, int] = Counter(categories)
            yield key, {"categories": counter,
                        "terms": list(set(terms)),
                        "total_documents": sum(counter.values())}
        else:
            counter: dict[str, int] = Counter(list(itertools.chain(*list(values))))
            yield key, counter


class Job(MRJob):
    def steps(self):
        return [InputToTermFreq()]


if __name__ == "__main__":
    Job().run()
