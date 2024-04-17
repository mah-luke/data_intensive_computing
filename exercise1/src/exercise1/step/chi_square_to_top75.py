from collections.abc import Generator
from typing import Any
from mrjob.job import MRJob, MRStep


class ChiSquareToTop75(MRStep):
    def __init__(self, **kwargs):
        super().__init__(
            # mapper=self.mapper,
            reducer=self.reducer,
            **kwargs
        )

    # def mapper(self, key, value):
    #     yield key, value
    #
    def reducer(self, key: str, values: Generator[tuple[str, int], Any, Any]):
        sorted_values = sorted(values, key=lambda tup: tup[1], reverse=True)
        yield key, sorted_values[:75]


class Job(MRJob):
    def steps(self):
        return [ChiSquareToTop75()]


if __name__ == "__main__":
    Job().run()
