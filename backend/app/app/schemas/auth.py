from pydantic import BaseModel


class HubSpotAuth(BaseModel):
    subdomain: str

class ZendeskAuth(BaseModel):
    subdomain: str