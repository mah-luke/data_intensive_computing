from typing import TypedDict


class CategoryTermsIndex(TypedDict):
    categories: dict[str, int]
    terms: list[str]
    total_documents: int


class ChiCalculation(TypedDict):
    doc_cnt_term_per_category: dict[str, int]
    doc_cnt_per_category: dict[str, int]
    doc_cnt_total: int
