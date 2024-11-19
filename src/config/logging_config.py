from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field, HttpUrl

class URLDocument(BaseModel):
    """MongoDB document model for URLs"""
    
    url: HttpUrl
    url_hash: str
    normalized_url: str
    domain: str
    source_url: Optional[HttpUrl]
    depth: int = 0
    keywords: List[str] = Field(default_factory=list)
    title: Optional[str]
    description: Optional[str]
    status_code: Optional[int]
    content_type: Optional[str]
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    last_checked: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = True
    error_count: int = 0
    metadata: dict = Field(default_factory=dict)

    class Config:
        arbitrary_types_allowed = True

class CrawlStats(BaseModel):
    """Model for crawl statistics"""
    
    start_time: datetime
    end_time: Optional[datetime]
    urls_processed: int = 0
    urls_failed: int = 0
    new_urls_found: int = 0
    duplicate_urls: int = 0
    total_requests: int = 0
    average_response_time: float = 0.0
    errors: List[str] = Field(default_factory=list)
    keywords: List[str] = Field(default_factory=list)

class ProxyStats(BaseModel):
    """Model for proxy statistics"""
    
    proxy_address: str
    success_count: int = 0
    failure_count: int = 0
    average_response_time: float = 0.0
    last_used: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = True
    error_messages: List[str] = Field(default_factory=list)
