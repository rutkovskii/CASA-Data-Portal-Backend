# Python standard library imports
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional
import asyncio
import os
import traceback

# Third-party imports
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, joinedload

# Local imports
from src.database.models import NoaaEvent, NcFile, EventStatus
from src.database.schemas import NoaaEventDTO
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


async def get_events_to_map(start_date: Optional[datetime] = None):
    """Events that have not been mapped"""
    async with session_scope() as s:
        query = select(NoaaEvent).where(NoaaEvent.status == EventStatus.UNMAPPED)
        if start_date:
            query = query.where(NoaaEvent.date_time_start >= start_date)
        result = await s.execute(query)
        return result.scalars().all()


async def get_matching_nc_files(
    product: str, date_time_start: datetime, date_time_end: datetime
):
    """Get the matching nc files for the given product and date_time between start and end times"""
    async with session_scope() as s:
        result = await s.execute(
            select(NcFile)
            .options(joinedload(NcFile.ref_file))  # Preload ref_file relationship
            .where(
                NcFile.product == product,
                NcFile.date_time >= date_time_start,
                NcFile.date_time <= date_time_end,
            )
        )
        return result.scalars().all()


async def get_current_event(session: AsyncSession, id_event: int) -> NoaaEvent:
    """
    Get the current event from the database using the provided session
    
    Args:
        session: SQLAlchemy async session
        event_id: NOAA event ID to query
        
    Returns:
        NoaaEvent: The current event from the database
    """
    result = await session.execute(
        select(NoaaEvent).where(NoaaEvent.id == id_event)
    )
    return result.scalar_one()


if __name__ == "__main__":
    pass
