from exercise1.job import ChiSquareCalculator
from exercise1.test_hadoop import SimpleJob

if __name__ == "__main__":
    # job = SimpleJob()
    job = ChiSquareCalculator()

    with job.make_runner() as runner:
        runner.run()

        for key, value in job.parse_output(runner.cat_output()):
            print(key, value, "\n", end="")
