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

    def reducer(self, key: str, values: Generator[tuple[str, int], Any, Any]):
        """Sort the top 75 terms for each category (key) based on the chi square
        value of the term (value[1]) and return the category as key and the top 75
        tuples of term and chi square value as values.
        returns:
            key:    <category>: str
            value:  <list of sorted top 75 terms>: list[tuple[str, int]]
        """
        sorted_values = sorted(values, key=lambda tup: tup[1], reverse=True)
        yield key, sorted_values[:75]


class Job(MRJob):
    def steps(self):
        return [ChiSquareToTop75()]


if __name__ == "__main__":
    Job().run()
