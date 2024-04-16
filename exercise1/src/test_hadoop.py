import sys

from mrjob.job import MRJob
from exercise1.definitions import BASE_PATH

class SimpleJob(MRJob):
    DIRS = ["exercise1"]

    def mapper(self, key, value):
        yield "a", None

    def reducer(self, key, values):
        yield "res", BASE_PATH


if __name__ == "__main__":
    SimpleJob().run()
