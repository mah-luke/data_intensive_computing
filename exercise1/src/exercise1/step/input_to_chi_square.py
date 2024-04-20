from collections.abc import Generator
from mrjob.job import MRStep
from mrjob.options import json
from typing import Any
import logging

from exercise1.chi_squares import calculate_chi_squares
from exercise1.model.review import Review

LOG = logging.getLogger("mrjob")


class InputToChiSquare(MRStep):
    """Calculate the chi squares for each term and category and yield
    category as key and a tuple of term and chi square as value.
    """

    def __init__(self, **kwargs):
        super().__init__(
            mapper_init=self.mapper_init,
            mapper=self.mapper,
            # combiner=self.combiner,
            reducer_init=self.reducer_init,
            reducer=self.reducer,
            **kwargs,
        )

    def mapper_init(self):
        with open("stopwords.txt", "r") as file:
            self.stopwords: set[str] = set(file.readlines())

    def mapper(self, _, value: bytearray):
        """Read the raw input to a dict of type Review, then split
        the reviewText into separate terms and return for each term
        the term as key and a dict of category and 1 as value.

        returns:
            key:    <term>: str
            value:  {<category>: 1}
        """
        parsed: Review = json.loads(value)

        terms = set()
        for term in parsed["reviewText"].split(" "):
            if term in self.stopwords:
                continue
            else:
                terms.add(term)
        for term in terms:
            yield term, {parsed["category"]: 1}

    @staticmethod
    def _merge_dicts(dicts: Generator[dict[str, int], Any, Any]):
        """Merge mutliple dictionaries together to a single dictionary
        by summing up all values of the same key"""
        res: dict[str, int] = dict()
        for dictionary in dicts:
            for category, doc_cnt in dictionary.items():
                if category not in res:
                    res[category] = doc_cnt
                else:
                    res[category] += doc_cnt
        return res

    def combiner(self, key: str, values: Generator[dict[str, int], Any, Any]):
        """When using the combiner worse runtime results were achieved
        hence we didn't use it"""
        yield key, self._merge_dicts(values)

    def reducer_init(self):
        """Initialize each reducer with the dictionary listing
        the count of reviews per category which were calculated
        in the previous job."""
        with open("doc_cnt_cat.json", "r") as file:
            self.doc_cnt_per_cat: dict[str, int] = json.load(file)
        self.doc_cnt_total = sum(self.doc_cnt_per_cat.values())

    def reducer(self, key: str, values: Generator[dict[str, int], Any, Any]):
        """Calculate chi square for each term and category and
        yield the category as key and a tuple of term and chi square as value.
        returns:
            key:    <category>: str
            value:  (<term>, <chi square value>): tuple[str, int]"""
        doc_cnt_term_per_cat: dict[str, int] = self._merge_dicts(values)
        doc_cnt_per_cat = self.doc_cnt_per_cat
        doc_cnt_total = self.doc_cnt_total

        doc_cnt_term_all_cat = sum(doc_cnt_term_per_cat.values())

        for category, doc_cnt_cur_term in doc_cnt_term_per_cat.items():
            a = doc_cnt_cur_term
            b = doc_cnt_term_all_cat - a
            c = doc_cnt_per_cat[category] - a
            d = doc_cnt_total - a - b - c
            chi_squared = calculate_chi_squares(a, b, c, d, doc_cnt_total)

            yield category, (key, chi_squared)
            # yield key, {
            #     "a": a,
            #     "b": b,
            #     "c": c,
            #     "d": d,
            #     "chi_squared": chi_squared,
            #     "doc_cnt_total": doc_cnt_total,
            #     "doc_cnt_term_all_cat": doc_cnt_term_all_cat,
            # }
