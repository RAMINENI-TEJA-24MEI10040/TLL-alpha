from pydantic import BaseModel
from typing import Optional, Dict, Any

class DiscoverRequest(BaseModel):
    spec_source: str
    base_url: Optional[str] = None
    jwt_tokens: Optional[Dict[str, str]] = None
