import modal

note_generator_stub = modal.Stub("note-generator")
indexer_stub = modal.Stub("indexer")

volume = modal.SharedVolume().persist("indexer-vol")


if modal.is_local():
    import os
    from dotenv import load_dotenv
    load_dotenv()
    pg_secret = modal.Secret(
        {
            "PGHOST": os.environ["PGHOST"],
            "PGPORT": os.environ["PGPORT"],
            "PGDATABASE": os.environ["PGDATABASE"],
            "PGUSER": os.environ["PGUSER"],
            "PGPASSWORD": os.environ["PGPASSWORD"],
        }
    )
    hubspot_secret = modal.Secret(
        {
            "HUBSPOT_CLIENT_ID": os.environ["HUBSPOT_CLIENT_ID"],
            "HUBSPOT_SECRET": os.environ["HUBSPOT_SECRET"],
            "HUBSPOT_REDIRECT_URI": os.environ["HUBSPOT_REDIRECT_URI"],
        }
    )
    note_generator_stub["pg_secret"] = pg_secret
    note_generator_stub["hubspot_secret"] = hubspot_secret
    indexer_stub["pg_secret"] = pg_secret
    indexer_stub["hubspot_secret"] = hubspot_secret