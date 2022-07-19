from typing import List, Union

from pydantic import BaseModel


class SearchResult(BaseModel):
    doc_name: str
    doc_last_updated: str
    doc_url: str
    score: float
    text: str


class SearchResponse(BaseModel):
    query_id: str
    query: str
    count: int
    results: List[SearchResult]
    answer: Union[str, None]


class Event(BaseModel):
    query_id: str
    event_type: str
    message: str