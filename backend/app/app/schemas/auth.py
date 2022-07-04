from pydantic import BaseModel

class ZendeskAuth(BaseModel):
    subdomain: str