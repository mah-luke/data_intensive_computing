from mrjob.job import MRJob

from exercise1.step.input_to_document_count_per_category import (
    InputToDocumentCountPerCategory,
)


class DocumentCountPerCategory(MRJob):
    DIRS = ["../../exercise1"]

    def steps(self):
        return [InputToDocumentCountPerCategory()]


if __name__ == "__main__":
    DocumentCountPerCategory.run()
