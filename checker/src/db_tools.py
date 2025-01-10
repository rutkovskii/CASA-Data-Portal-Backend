# Python standard library imports
from contextlib import asynccontextmanager
from typing import List
import asyncio
import os
import traceback
from pprint import pprint

# Third-party imports
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

if __name__ == "__main__":
    from database.models import NoaaRecord, NoaaEvent
    from database.schemas import NoaaRecordDTO, NoaaEventDTO
    from shared.logger_config import setup_logger
else:
    from src.database.models import NoaaRecord, NoaaEvent
    from src.database.schemas import NoaaRecordDTO, NoaaEventDTO
    from src.shared.logger_config import setup_logger


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


async def get_noaa_record(file_year: int):
    """Get a NOAA record by file year"""
    async with session_scope() as s:
        result = await s.execute(
            text("SELECT * FROM noaa_records WHERE file_year = :file_year"),
            {"file_year": file_year},
        )
        return result.fetchone()


async def get_noaa_records():
    """Get all NOAA records"""
    async with session_scope() as s:
        result = await s.execute(text("SELECT * FROM noaa_records"))
        return result.fetchall()


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
            noaa_product=event.noaa_product,
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
                noaa_product=event.noaa_product,
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


async def get_unique_products():
    """Get the unique products in the noaa_events table"""
    async with session_scope() as s:
        result = await s.execute(text("SELECT DISTINCT noaa_product FROM noaa_events"))
        return result.scalars().all()


if __name__ == "__main__":
    # asyncio.run(test_connection())
    # Print columns of noaa_events table
    # asyncio.run(print_noaa_event_columns())
    # asyncio.run(clean_noaa_tables())
    asyncio.run(clean_noaa_tables())
