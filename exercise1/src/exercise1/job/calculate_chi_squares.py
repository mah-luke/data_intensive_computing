from mrjob.job import MRJob, log_to_stream

from exercise1.step.chi_square_to_top75 import ChiSquareToTop75
from exercise1.step.input_to_term_freq import InputToTermFreq
from exercise1.step.term_freq_to_chi import TermFreqToChi
from exercise1.step.top75_to_merged import Top75ToMerged

import logging

LOG = logging.getLogger("mrjob")

class ChiSquareCalculator(MRJob):
    DIRS = ["../../exercise1"]
    FILES = ["../../stopwords.txt", "../../doc_cnt_cat.json"]

    def set_up_logging(cls, quiet=False, verbose=False, stream=None):
        log_to_stream(name="mrjob", debug=verbose, stream=stream)

    def steps(self):
        LOG.warning("start steps")
        return [InputToTermFreq(), ChiSquareToTop75()]

        # return [InputToTermFreq(), TermFreqToChi(), ChiSquareToTop75(), Top75ToMerged()]


if __name__ == "__main__":
    ChiSquareCalculator.run()
