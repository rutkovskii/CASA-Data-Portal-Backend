from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel


class LastUploadedDateDTO(BaseModel):
    id: Optional[int] = None
    product: str
    date: date


class FailedUploadDTO(BaseModel):
    id: Optional[int] = None
    remote_path: str
    product: str
    date_dir: str
    last_error: str
    last_attempt: Optional[datetime] = None
