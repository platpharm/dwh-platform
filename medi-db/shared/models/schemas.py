from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

class HealthResponse(BaseModel):
    status: str
    service: str
    dependencies: Dict[str, bool] = {}

class CrawlRequest(BaseModel):
    keywords: List[str]

class CrawlResponse(BaseModel):
    success: bool
    count: int
    message: str

class TrendData(BaseModel):
    keyword: str
    source: str
    date: datetime
    value: float
    related_keywords: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None

class PreprocessResponse(BaseModel):
    success: bool
    processed_count: int
    message: str

class TrendProductMapping(BaseModel):
    product_id: int
    product_name: str
    keyword: str
    keyword_source: str
    trend_score: float = 0.0
    match_score: float = 0.0
    timestamp: datetime = Field(default_factory=datetime.utcnow)

