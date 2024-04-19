from collections.abc import Generator
from mrjob.job import MRJob, MRStep
from mrjob.options import json
from typing import Any
import logging

from exercise1.chi_squares import calculate_chi_squares
from exercise1.model.review import Review
from exercise1.util import timed

LOG = logging.getLogger("mrjob")


class InputToTermFreq(MRStep):
    """Do the first step of the Chi Square calculation:
    create the term frequencies for each category.
    """

    def __init__(self, **kwargs):
        super().__init__(
            mapper_init=self.mapper_init,
            mapper=self.mapper,
            combiner=self.combiner,
            reducer_init=self.reducer_init,
            reducer=self.reducer,
            **kwargs,
        )

    def mapper_init(self):
        with open("stopwords.txt", "r") as file:
            self.stopwords: set[str] = set(file.readlines())

    def mapper(self, _, value: bytearray):
        parsed: Review = json.loads(value)

        terms = set()
        for term in parsed["reviewText"].split(" "):
            if term in self.stopwords:
                continue
            else:
                terms.add(term)
        # LOG.info(
        #     f"----- mapper: yielding {len(terms)} terms for cat: {parsed['category']}"
        # )
        for term in terms:
            # LOG.info(f"---- mapper: yielding {term}, {parsed['category']}")
            yield term, {parsed["category"]: 1}

    def _merge_dicts(self, dicts: Generator[dict[str, int], Any, Any]):
        res: dict[str, int] = dict()
        # dicts_list = list(dicts)
        for dictionary in dicts:
            for category, doc_cnt in dictionary.items():
                if category not in res:
                    res[category] = doc_cnt
                else:
                    res[category] += doc_cnt
        # LOG.info(f"--- merge_dicts: reduced {len(dicts_list)} to {len(res)}")
        return res

    def combiner(self, key: str, values: Generator[dict[str, int], Any, Any]):
        yield key, self._merge_dicts(values)

    def reducer_init(self):
        # LOG.info("---- reducer_init ----")
        with open("doc_cnt_cat.json", "r") as file:
            self.doc_cnt_per_cat: dict[str, int] = json.load(file)
        self.doc_cnt_total = sum(self.doc_cnt_per_cat.values())

    def reducer(self, key: str, values: Generator[dict[str, int], Any, Any]):
        doc_cnt_term_per_cat: dict[str, int] = self._merge_dicts(values)
        # LOG.info(f"---- reducer ----- {key}, {len(doc_cnt_term_per_cat)}")
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


class Job(MRJob):
    def steps(self):
        return [InputToTermFreq()]


if __name__ == "__main__":
    Job().run()
