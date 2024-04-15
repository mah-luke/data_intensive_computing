from mrjob.job import MRJob, MRStep


class TermFreqToChi(MRStep):
    def __init__(self, **kwargs):
        super().__init__(
            mapper=self.mapper, combiner=self.combiner, reducer=self.reducer, **kwargs
        )

    def mapper(self, key: tuple[str, str], value):
        if key[0] == "category":
            """"""
            documents_per_cat: dict[str, int] = value["categories"]
            unique_terms: list[str] = value["terms"]
            total_documents: int = value["total_documents"]

            for term in unique_terms:
                """Distribute document per category counts to all terms"""
                yield ("term", term), (
                    None,
                    {
                        "categories": documents_per_cat,
                        "total_documents": total_documents,
                    },
                )
        else:
            """"""
            # ("term", "<term1>"), {"<cat1>": <cnt_docs>, ...}
            yield key, (value, None)

    def combiner(self, key, values):
        if key[0] == "category":
            yield key, list(values)
        else:
            yield key, list(values)

    def reducer(self, key, values):

        if key[0] == "category":
            yield key, list(values)
            # yield key, {tup[0]: tup[1] for sublist in values for tup in sublist}

        else:
            values_list = list(values)
            yield key, values_list


class Job(MRJob):
    def steps(self):
        return [TermFreqToChi()]


if __name__ == "__main__":
    Job().run()
