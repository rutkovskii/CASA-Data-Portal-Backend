from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class NcFileDTO(BaseModel):
    id: Optional[int] = None
    s3_path: str
    date_time: datetime
    product: str
    event_id: Optional[int] = None
    reference_file_id: Optional[int] = None


class IndividualRefFileDTO(BaseModel):
    id: Optional[int] = None
    s3_path: str
    date_time: datetime
    product: str
    event_id: Optional[int] = None
    nc_file_id: Optional[int] = None

class EventRefFileDTO(BaseModel):
    id: Optional[int] = None
    s3_path: str
    product: str
    event_id: int

