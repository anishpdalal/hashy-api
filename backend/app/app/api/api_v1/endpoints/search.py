import os

from fastapi import APIRouter, Depends
import openai
import pinecone
from sentence_transformers import SentenceTransformer
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import (
    current_active_user,
    get_async_session,
)
from app.crud.source import get_sources
from app.models.user import User
from app.schemas.search import SearchResponse

PINECONE_KEY = os.getenv("PINECONE_KEY")
pinecone.init(api_key=PINECONE_KEY, environment="us-west1-gcp")
index = pinecone.Index(index_name="semantic-text-search")
openai.api_key = os.getenv("OPENAI_API_KEY")
search_model = SentenceTransformer("/mnt/bi_encoder")

api_router = APIRouter()


@api_router.get("/search", tags=["search"], response_model=SearchResponse)
async def search(
    query: str,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_session),
    count: int = 10
):
    user_id = str(user.id)
    source_ids = [str(source.id) for source in await get_sources(db, user_id)]
    filter = {"source_id": {"$in": source_ids}}
    query_embedding = search_model.encode([query])
    query_results = index.query(
        queries=[query_embedding.tolist()],
        top_k=count,
        filter=filter,
        include_metadata=True,
        include_values=False
    )
    matches = query_results["results"][0]["matches"]
    results = {
        "query": query,
        "count": len(matches),
        "results": [],
        "answer": None
    }
    for match in matches:
        metadata = match["metadata"]
        score = match["score"]
        doc_type = metadata["doc_type"]
        if doc_type == "zendesk_hc_article_title" and not results["answer"]:
            prompt = "Answer the question based on the context below, and if the question can't be answered based on the context, say \"I don't know\"\n\nContext:\n{0}\n\n---\n\nQuestion: {1}\nAnswer:"
            response = openai.Completion.create(
                engine="text-davinci-002",
                prompt=prompt.format(metadata["text"], query),
                temperature=0,
                max_tokens=100,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0
            )
            results["answer"] = response.choices[0]["text"].strip()
        result = {
            "score": score,
            "doc_name": metadata["doc_name"],
            "doc_last_updated": str(metadata["doc_last_updated"]),
            "doc_url": metadata["doc_url"],
            "text": metadata["text"]
        }
        results["results"].append(result)
    return results