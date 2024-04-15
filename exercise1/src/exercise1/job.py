from mrjob.job import MRJob

from exercise1.step.input_to_term_freq import InputToTermFreq
from exercise1.step.term_freq_to_chi import TermFreqToChi


class ChiSquareCalculator(MRJob):

    def steps(self):
        return [InputToTermFreq(),
                TermFreqToChi()]


if __name__ == "__main__":
    ChiSquareCalculator.run()
