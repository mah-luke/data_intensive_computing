from collections.abc import Generator
from mrjob.conf import json
from mrjob.job import MRStep

from exercise1.model.review import Review


class InputToDocumentCountPerCategory(MRStep):

    def __init__(self, **kwargs):
        super().__init__(
            mapper=self.mapper, combiner=self.combiner, reducer=self.reducer, **kwargs
        )

    def mapper(self, _, value: bytearray):
        parsed: Review = json.loads(value)

        yield parsed["category"], 1

    def combiner(self, key: str, values: Generator[int, None, None]):
        yield key, sum(values)

    def reducer(self, key: str, values: Generator):
        yield key, sum(values)
