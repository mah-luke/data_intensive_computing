from mrjob.job import MRJob, MRStep


class TermFreqToChi(MRStep):
    def __init__(self, **kwargs):
        super().__init__(
            mapper=self.mapper, combiner=self.combiner, reducer=self.reducer, **kwargs
        )

    def mapper(self, key: str, value: dict[str, int]):
        if key[0] == "category":
            yield (key[0],), (key[1], value)
        else:
            yield key, value

    def combiner(self, key, values):
        if key[0] == "category":
            yield key, list(values)
        else:
            yield key, list(values)

    def reducer(self, key, values: any):

        if key[0] == "category":
            yield key, {tup[0]: tup[1] for sublist in values for tup in sublist}

        else:
            values_list = list(values)
            yield key, values_list


class Job(MRJob):
    def steps(self):
        return [TermFreqToChi()]


if __name__ == "__main__":
    Job().run()
