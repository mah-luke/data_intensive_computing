from mrjob.job import BytesIO, MRJob
from exercise1.definitions import BASE_PATH
from exercise1.job import ChiSquareCalculator
from exercise1.step.input_to_term_freq import InputToTermFreq, Job


def test_basic_input_to_term_freq():

    job = Job(
        args=[str(BASE_PATH / "resource" / "reviews_devset_first100.json")]
    )

    with job.make_runner() as runner:
        runner.run()
        print("out:")
        [print(line) for line in runner.cat_output()]



def test_sandbox_input_to_term_freq():
    pass
    # stdin ="""{"reviewerID": "A2VNYWOPJ13AFP", "asin": "0981850006", "reviewerName": 'Amazon Customer "carringt0n"',
    #     "helpful": [6, 7],
    #     "reviewText": "This was a gift for my other husband.  He's making us things from it all the time and we love the food.  Directions are simple, easy to read and interpret, and fun to make.  We all love different kinds of cuisine and Raichlen provides recipes from everywhere along the barbecue trail as he calls it. Get it and just open a page.  Have at it.  You'll love the food and it has provided us with an insight into the culture that produced it. It's all about broadening horizons.  Yum!!",
    #     "overall": 5.0,
    #     "summary": "Delish",
    #     "unixReviewTime": 1259798400,
    #     "reviewTime": "12 3, 2009",
    #     "category": "Patio_Lawn_and_Garde",
    # }"""
    #
    # job = ChiSquareCalculator()
    #
    # job.sandbox(stdin=str(stdin))
    # job.run_mapper(0)
