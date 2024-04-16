from mrjob.job import MRStep


class TestStep(MRStep):

    def __init__(self, **kwargs):
        super().__init__(mapper=self.mapper, reducer=self.reducer, **kwargs)

    def mapper(self, key, value):
        stopwords = set(open("exercise1/stopwords.txt", "r").readlines())
        yield "a", "b"
        # yield "a", "b"

    def reducer(self, key, values):

        yield key, list(values)
