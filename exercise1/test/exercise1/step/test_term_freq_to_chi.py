from exercise1.definitions import BASE_PATH
from exercise1.job.calculate_chi_squares import ChiSquareCalculator
# from exercise1.step.term_freq_to_chi import Job


# def test_basic_test_term_freq_to_chi():
#     job = Job(args=[str(BASE_PATH / "out" / "after_step1.txt")])
#
#     with job.make_runner() as runner:
#         runner.run()
#         print("out:")
#         [print(line) for line in runner.cat_output()]


def test_full_job():
    job = ChiSquareCalculator(
        args=[str(BASE_PATH / "resource" / "reviews_devset_first1000.json")]
    )

    with job.make_runner() as runner:
        runner.run()


def test_full_job_all():
    job = ChiSquareCalculator(
        args=[str(BASE_PATH / "resource" / "reviews_devset.json")]
    )

    with job.make_runner() as runner:
        runer.run()

