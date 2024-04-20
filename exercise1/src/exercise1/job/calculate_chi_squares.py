from mrjob.job import MRJob, log_to_stream
import logging

from exercise1.step.chi_square_to_top75 import ChiSquareToTop75
from exercise1.step.input_to_chi_square import InputToChiSquare

LOG = logging.getLogger("mrjob")


class ChiSquareCalculator(MRJob):
    """Use json created by first job (doc_cnt_cat.json) and
    distribute it to all workers (this file is always small due
    to the constant size of 22 categories).
    Return for each term the top 75 categories based on chi square
    calculation.
    """

    DIRS = ["../../exercise1"]
    FILES = ["../../stopwords.txt", "../../../tmp/doc_cnt_cat.json"]

    @classmethod
    def set_up_logging(cls, quiet=False, verbose=False, stream=None):
        if not quiet:
            log_to_stream(name="mrjob", debug=verbose, stream=stream)
            log_to_stream(name="__main__", debug=verbose, stream=stream)

    def steps(self):
        return [
            InputToChiSquare(),
            ChiSquareToTop75(),
            # Top75ToMerged()
        ]


if __name__ == "__main__":
    ChiSquareCalculator.run()
