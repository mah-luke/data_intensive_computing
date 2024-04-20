from collections.abc import Generator
from mrjob.conf import json
from mrjob.job import MRJob, MRStep
import sys
import logging
from exercise1.model.review import Review

LOG = logging.getLogger(__name__)


class InputToDocumentCountPerCategory(MRStep):
    """Counts the number of reviews per category for review entries of
    type exercise1.model.review.Review

    returns:
        (str, int): Returns for each category a tuple of the category name and
        the amount of reviews in that category."""

    def __init__(self, **kwargs):
        super().__init__(
            mapper=self.mapper, combiner=self.combiner, reducer=self.reducer, **kwargs
        )

    def mapper(self, _, value: bytearray):
        """Read the raw input to a dict of type Review and
        return the category it belongs to.
        returns:
            key:    <category>: str
            value:  1:int
        """
        parsed: Review = json.loads(value)

        yield parsed["category"], 1

    def combiner(self, key: str, values: Generator[int, None, None]):
        """Combine all reviews processed by a mapper to reduce communication
        costs and speedup computation (more mappers than reducers in this case).
        returns:
            key:    <category>: str
            value:  <sum of reviews in this category>: int
        """
        yield key, sum(values)

    def reducer(self, key: str, values: Generator):
        """Combine the document counts for each category (key).
        returns:
            key:    <category>: str
            value:  <count of all reviews in this category>: int
        """
        yield key, sum(values)
