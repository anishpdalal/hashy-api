from typing import List, Optional

from pydantic import BaseModel


class SearchResult(BaseModel):
    doc_name: str
    doc_last_updated: str
    doc_url: str
    score: float
    text: str


class SearchResponse(BaseModel):
    query: str
    count: int
    results: List[SearchResult]
    answer: Optional[str]