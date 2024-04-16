from mrjob.job import MRJob

from exercise1.step.chi_square_to_top75 import ChiSquareToTop75
from exercise1.step.input_to_term_freq import InputToTermFreq
from exercise1.step.term_freq_to_chi import TermFreqToChi
from exercise1.step.top75_to_merged import Top75ToMerged


class ChiSquareCalculator(MRJob):

    def steps(self):
        return [InputToTermFreq(),
                TermFreqToChi(),
                ChiSquareToTop75(),
                Top75ToMerged()]


if __name__ == "__main__":
    ChiSquareCalculator.run()
