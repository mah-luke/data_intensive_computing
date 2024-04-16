from functools import reduce
from mrjob.job import MRJob, MRStep


class Top75ToMerged(MRStep):
    def __init__(self, **kwargs):
        super().__init__(mapper=self.mapper, reducer=self.reducer, **kwargs)

    def mapper(self, key, value):
        """
        <category>, [(<term>, <chi_square>), ...]
        """
        yield key, value
        yield "merge", [tup[0] for tup in value]

    def reducer(self, key, values):
        if key == "merge":
            """ """
            merged = reduce(lambda a, b: a + b, values)
            yield "", sorted(merged)

        else:
            yield key, list(values)


class Job(MRJob):
    def steps(self):
        return [Top75ToMerged()]


if __name__ == "__main__":
    Job().run()
