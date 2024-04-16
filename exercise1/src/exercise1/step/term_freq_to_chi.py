from functools import reduce
from mrjob.job import MRJob, MRStep

from exercise1.chi_squares import calculate_chi_squares
from exercise1.model.category_terms_index import CategoryTermsIndex, ChiCalculation


class TermFreqToChi(MRStep):
    def __init__(self, **kwargs):
        super().__init__(
            mapper=self.mapper, combiner=self.combiner, reducer=self.reducer, **kwargs
        )

    def mapper(self, key: tuple[str, str], value):
        if key[0] == "category":
            """
            ("category",), CategoryTermsIndex
            """
            category_value: CategoryTermsIndex = value

            for term in category_value["terms"]:
                """Distribute document per category counts to all terms"""
                yield ("term", term), {
                    "doc_cnt_per_category": category_value["categories"],
                    "doc_cnt_total": category_value["total_documents"],
                }
        else:
            """
            ("term", <term>), {<category>: <doc_cnt>, ...}
            """
            term_value: dict[str, int] = value
            yield key, {"doc_cnt_term_per_category": term_value}

    def combiner(self, key, values):
        yield key, reduce(lambda a, b: a | b, values)

    def reducer(self, key, values):
        """
        ("term", <term>), [{"doc_cnt_term_per_category": {<category>: <doc_cnt>, ...},
                            "doc_cnt_per_category": {<category>: <doc_cnt>, ...},
                            "doc_cnt_total": cnt}]
        """

        merged_values: ChiCalculation = reduce(lambda a, b: a | b, values)
        doc_cnt_term_all_categories = sum(
            merged_values["doc_cnt_term_per_category"].values()
        )

        for category in merged_values["doc_cnt_term_per_category"]:
            a = merged_values["doc_cnt_term_per_category"][category]
            b = doc_cnt_term_all_categories - a
            c = merged_values["doc_cnt_per_category"][category] - a
            d = merged_values["doc_cnt_total"] - a - b - c

            chi_square = calculate_chi_squares(
                a, b, c, d, merged_values["doc_cnt_total"]
            )
            yield category, (key[1], chi_square)


class Job(MRJob):
    def steps(self):
        return [TermFreqToChi()]


if __name__ == "__main__":
    Job().run()
