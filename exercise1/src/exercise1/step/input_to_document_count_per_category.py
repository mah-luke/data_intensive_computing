from collections.abc import Generator
from mrjob.conf import json
from mrjob.job import MRJob, MRStep
import sys
import logging
from exercise1.model.review import Review

LOG = logging.getLogger(__name__)


class InputToDocumentCountPerCategory(MRStep):

    def __init__(self, **kwargs):
        super().__init__(
            mapper=self.mapper, combiner=self.combiner, reducer=self.reducer, **kwargs
        )

    def mapper(self, _, value: bytearray):
        # LOG.info(f"------ start mapper ----")
        parsed: Review = json.loads(value)

        yield parsed["category"], 1

    def combiner(self, key: str, values: Generator[int, None, None]):
        # LOG.info(f"------ start combiner -----")
        yield key, sum(values)

    def reducer(self, key: str, values: Generator):
        # LOG.info(f"------ start reducer ------")
        yield key, sum(values)


class Job(MRJob):
    def steps(self):
        return [InputToDocumentCountPerCategory()]


if __name__ == "__main__":
    job = Job()
    job.set_up_logging(quiet=False, verbose=True, stream=sys.stderr)
    job.run()
