from mrjob.job import MRJob, log_to_stream

from exercise1.step.chi_square_to_top75 import ChiSquareToTop75
from exercise1.step.input_to_term_freq import InputToTermFreq

import logging

LOG = logging.getLogger("mrjob")


class ChiSquareCalculator(MRJob):
    """


    """

    DIRS = ["../../exercise1"]
    FILES = ["../../stopwords.txt", "../../../tmp/doc_cnt_cat.json"]

    def set_up_logging(cls, quiet=False, verbose=False, stream=None):
        if not quiet:
            log_to_stream(name="mrjob", debug=verbose, stream=stream)
            log_to_stream(name="__main__", debug=verbose, stream=stream)

    def steps(self):
        return [InputToTermFreq(), ChiSquareToTop75()]


if __name__ == "__main__":
    ChiSquareCalculator.run()
