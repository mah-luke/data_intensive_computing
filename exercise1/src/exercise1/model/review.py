from typing import TypedDict


class Review(TypedDict):
    reviewerID: str
    asin: str
    reviewerName: str
    helpful: list[int]
    reviewText: str
    overall: float
    summary: str
    unixReviewTime: int
    reviewTime: str
    category: str
