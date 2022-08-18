import modal
from stubs import indexer_stub
from images import nlp_image


@indexer_stub.function(image=nlp_image, secret=modal.ref("pinecone-secret"))
def delete_index():
    import os
    import pinecone

    PINECONE_KEY = os.environ["PINECONE_KEY"]
    pinecone.init(api_key=PINECONE_KEY, environment="us-west1-gcp")
    index = pinecone.Index(index_name="semantic-text-search")
    index.delete(delete_all=True, namespace="note_generation")

    

if __name__ == "__main__":
    with indexer_stub.run():
        delete_index()