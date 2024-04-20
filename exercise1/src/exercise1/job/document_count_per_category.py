from mrjob.job import MRJob

from exercise1.step.input_to_document_count_per_category import (
    InputToDocumentCountPerCategory,
)


class DocumentCountPerCategory(MRJob):
    """Calculate the count of reviews per category.
    returns:
        dict[str, int]: The dictionary containing the category as key
            and the count of reviews in this category as value.
    """

    # Make exercise1 (our package) available to all workers
    DIRS = ["../../exercise1"]

    def steps(self):
        return [InputToDocumentCountPerCategory()]


if __name__ == "__main__":
    DocumentCountPerCategory.run()
