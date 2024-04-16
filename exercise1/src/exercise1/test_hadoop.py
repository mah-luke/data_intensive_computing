from mrjob.job import MRJob
from exercise1.step.test_step import TestStep


class SimpleJob(MRJob):
    DIRS = ["../exercise1"]
    # FILES = ["stopwords.txt"]

    def steps(self):
        return [TestStep()]


if __name__ == "__main__":
    SimpleJob().run()
