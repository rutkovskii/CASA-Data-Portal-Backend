from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel
from ..models.enums import EventStatus


class NoaaRecordDTO(BaseModel):
    id: Optional[int] = None
    file_year: int
    last_modified: date


class NoaaEventDTO(BaseModel):
    id: Optional[int] = None
    noaa_record_id: int
    event_id: int
    noaa_product: str
    date_time_start: datetime
    date_time_end: datetime
    status: EventStatus = EventStatus.UNMAPPED

    # Location fields
    begin_lat: Optional[float] = None
    begin_lon: Optional[float] = None
    end_lat: Optional[float] = None
    end_lon: Optional[float] = None
    county: str = "UNKNOWN"
    begin_city: Optional[str] = None
    end_city: Optional[str] = None

    # Impact fields
    magnitude: Optional[str] = None
    damage_property: int = 0
    damage_crops: int = 0
    deaths_direct: int = 0
    deaths_indirect: int = 0
    injuries_direct: int = 0
    injuries_indirect: int = 0

    # Description fields
    event_narrative: Optional[str] = None
    episode_narrative: Optional[str] = None
