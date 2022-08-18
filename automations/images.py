import modal

db_image = modal.DebianSlim().apt_install(["libpq-dev"]).pip_install(["psycopg2"])
integrations_image = modal.DebianSlim().pip_install(["requests", "pinecone-client", "openai"])
web_scraper_image = modal.DebianSlim().pip_install(["lxml", "beautifulsoup4", "requests", "pysbd"])
nlp_image = modal.DebianSlim().pip_install(["protobuf==3.20.1", "sentence-transformers", "torch", "pinecone-client"])