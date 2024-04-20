import json
import sys
from exercise1.definitions import TMP_PATH, BASE_PATH
from exercise1.job.calculate_chi_squares import ChiSquareCalculator
from exercise1.job.document_count_per_category import DocumentCountPerCategory
import logging

logging.basicConfig()
LOG = logging.getLogger(__name__)

if __name__ == "__main__":
    logging_kwargs = {"quiet": False, "verbose": False, "stream": sys.stderr}

    # -------- First Job -------------------------
    job_args = sys.argv.copy()[1:]
    job = DocumentCountPerCategory(args=job_args)
    job.set_up_logging(**logging_kwargs)

    LOG.info(f"Running DocumentCountPerCategory with args: {job_args}")
    with job.make_runner() as runner:
        runner.run()

        doc_cnt_cat_out = {
            key: val for key, val in job.parse_output(runner.cat_output())
        }

    TMP_PATH.mkdir(exist_ok=True)
    with open(TMP_PATH / "doc_cnt_cat.json", "w") as file:
        json.dump(doc_cnt_cat_out, file)

    # -------- Second Job ---------------------
    job_chi_calc_args = sys.argv.copy()[1:]
    job_chi_calc_args.append("-D")
    job_chi_calc_args.append("mapreduce.job.reduces=1000")
    job_chi_calc = ChiSquareCalculator(args=job_chi_calc_args)
    job_chi_calc.set_up_logging(**logging_kwargs)

    with job_chi_calc.make_runner() as runner:
        runner.run()
        top75_terms_per_cat: dict[str, list[tuple[str, int]]] = {
            key: val for key, val in job_chi_calc.parse_output(runner.cat_output())
        }

    # -------- Format Job result & print it -----
    # Final step also implemented as a MapReduce Step, but done here as it has a
    # small and constant size (22 categories * 75 terms) hence it would be overkill using
    # a MapReduce job for that
    best_terms: set[str] = set()
    for tup_list in top75_terms_per_cat.values():
        for tup in tup_list:
            best_terms.add(tup[0])
    best_terms_sorted = sorted(best_terms)

    output: list[str] = []
    best_terms_sorted_formatted = " ".join(best_terms_sorted)
    output.append(best_terms_sorted_formatted)

    for key, value in top75_terms_per_cat.items():
        output.append(f"<{key}> {' '.join([f'{tup[0]}:{tup[1]}' for tup in value])}")

    output_formatted: str = "\n".join(output)
    print(output_formatted)

    # write job result also to a file in folder out
    (BASE_PATH / "out").mkdir(exist_ok=True)
    with open(BASE_PATH / "out" / "job_result.txt", "w") as file:
        file.writelines(output_formatted)
