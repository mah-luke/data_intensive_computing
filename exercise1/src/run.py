import json
import sys
from exercise1.definitions import DOC_CNT_CAT_PATH
from exercise1.job.calculate_chi_squares import ChiSquareCalculator
from exercise1.job.document_count_per_category import DocumentCountPerCategory

if __name__ == "__main__":
    logging_kwargs = {"quiet": False, "verbose": False, "stream": sys.stderr}

    # job = SimpleJob()
    # job = ChiSquareCalculator()
    job = DocumentCountPerCategory()

    # with job.make_runner() as runner:
    #     runner.run()
    #
    #     doc_cnt_cat_out = {
    #         key: val for key, val in job.parse_output(runner.cat_output())
    #     }
    #
    # print(doc_cnt_cat_out)
    # with open(DOC_CNT_CAT_PATH, "w") as file:
    #     json.dump(doc_cnt_cat_out, file)

    # for key, value in job.parse_output(doc_cnt_cat_out):
    #     print(key, value, "\n", end="")

    job_chi_calc = ChiSquareCalculator()
    job_chi_calc.set_up_logging(**logging_kwargs)

    with job_chi_calc.make_runner() as runner:
        runner.run()

        for key, value in job.parse_output(runner.cat_output()):
            print(key, value, "\n", end="")
        top75_terms_per_cat: dict[str, list[tuple[str, int]]] = {
            key: val for key, val in job_chi_calc.parse_output(runner.cat_output())
        }

    best_terms: set[str] = set()
    for tup_list in top75_terms_per_cat.values():
        for tup in tup_list:
            best_terms.add(tup[0])

    best_terms_sorted = sorted(best_terms)
    print(" ".join(best_terms_sorted))

    for key, value in top75_terms_per_cat.items():
        print(f"<{key}>", " ".join([f"{tup[0]}:{tup[1]}" for tup in value]), sep=" ")
