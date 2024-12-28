# Python standard library imports
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Optional, List
import asyncio
import os
import traceback
from pprint import pprint

# Third-party imports
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Local imports
from database.models import NoaaRecord, NoaaEvent, EventStatus
from shared.logger_config import setup_logger


# Logging setup
logger = setup_logger("checker", "db_tools")

# Get database URL
DATABASE_URL = os.getenv("SQLALCHEMY_DATABASE_URL")
logger.debug(f"Using DATABASE_URL: {DATABASE_URL}")

# Engine and session setup
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@asynccontextmanager
async def session_scope():
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            logger.error("Session rollback due to error.")
            logger.error(traceback.format_exc())
            raise
        finally:
            await session.close()


class NoaaRecordDTO(BaseModel):
    id: Optional[int] = None
    file_year: int
    last_modified: date


class NoaaEventDTO(BaseModel):
    id: Optional[int] = None
    noaa_record_id: int
    event_id: str
    product: str
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


async def test_connection():
    async with session_scope() as s:
        result = await s.execute(text("SELECT version();"))
        row = result.fetchone()
        version = row[0] if row else "Unknown"
        logger.info(f"Connected to PostgreSQL: {version}")


async def post_noaa_record(noaa_record: NoaaRecordDTO):
    """Create and return a new NOAA record"""
    async with session_scope() as s:
        db_noaa_record = NoaaRecord(
            file_year=noaa_record.file_year, last_modified=noaa_record.last_modified
        )
        s.add(db_noaa_record)
        await s.commit()
        await s.refresh(db_noaa_record)  # Refresh to get the generated ID
        return db_noaa_record


async def post_noaa_records(noaa_records: List[NoaaRecordDTO]):
    async with session_scope() as s:
        db_noaa_records = [
            NoaaRecord(
                file_year=noaa_record.file_year, last_modified=noaa_record.last_modified
            )
            for noaa_record in noaa_records
        ]
        s.add_all(db_noaa_records)
        await s.commit()


async def get_noaa_record(file_year: int):
    async with session_scope() as s:
        result = await s.execute(
            text("SELECT * FROM noaa_records WHERE file_year = :file_year"),
            {"file_year": file_year},
        )
        return result.fetchone()


async def get_noaa_records():
    async with session_scope() as s:
        result = await s.execute(text("SELECT * FROM noaa_records"))
        return result.fetchall()


async def delete_noaa_record(file_year: int):
    async with session_scope() as s:
        await s.execute(
            text("DELETE FROM noaa_records WHERE file_year = :file_year"),
            {"file_year": file_year},
        )
        await s.commit()


async def put_noaa_record(noaa_record: NoaaRecordDTO):
    async with session_scope() as s:
        await s.execute(
            text(
                "UPDATE noaa_records SET last_modified = :last_modified WHERE file_year = :file_year"
            ),
            {
                "file_year": noaa_record.file_year,
                "last_modified": noaa_record.last_modified,
            },
        )
        await s.commit()


async def post_noaa_event(event: NoaaEventDTO):
    async with session_scope() as s:
        db_event = NoaaEvent(
            noaa_record_id=event.noaa_record_id,
            event_id=event.event_id,
            product=event.product,
            date_time_start=event.date_time_start,
            date_time_end=event.date_time_end,
            status=event.status,
        )
        s.add(db_event)
        await s.commit()
        return db_event


async def post_noaa_events(events: List[NoaaEventDTO]):
    async with session_scope() as s:
        db_events = [
            NoaaEvent(
                noaa_record_id=event.noaa_record_id,
                event_id=event.event_id,
                product=event.product,
                date_time_start=event.date_time_start,
                date_time_end=event.date_time_end,
                status=event.status,
                # Location fields
                begin_lat=event.begin_lat,
                begin_lon=event.begin_lon,
                end_lat=event.end_lat,
                end_lon=event.end_lon,
                county=event.county,
                begin_city=event.begin_city,
                end_city=event.end_city,
                # Impact fields
                magnitude=event.magnitude,
                damage_property=event.damage_property,
                damage_crops=event.damage_crops,
                deaths_direct=event.deaths_direct,
                deaths_indirect=event.deaths_indirect,
                injuries_direct=event.injuries_direct,
                injuries_indirect=event.injuries_indirect,
                # Description fields
                event_narrative=event.event_narrative,
                episode_narrative=event.episode_narrative,
            )
            for event in events
        ]
        s.add_all(db_events)
        await s.commit()
        return db_events


# clean noaa_records table and noaa_events table
async def clean_noaa_tables():
    """Clean both noaa_events and noaa_records tables in the correct order"""
    async with session_scope() as s:
        # Delete events first to remove foreign key references
        await s.execute(text("DELETE FROM noaa_events"))
        # Then delete records
        await s.execute(text("DELETE FROM noaa_records"))
        await s.commit()


async def print_noaa_event_columns():
    """Print the column names of the noaa_events table"""
    async with session_scope() as s:
        result = await s.execute(text("SELECT * FROM noaa_events LIMIT 0"))
        pprint([desc for desc in result.keys()])


if __name__ == "__main__":
    # asyncio.run(test_connection())
    # Print columns of noaa_events table
    asyncio.run(print_noaa_event_columns())
    # asyncio.run(clean_noaa_tables())

