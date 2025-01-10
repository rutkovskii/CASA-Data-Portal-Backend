# Python standard library imports
from contextlib import asynccontextmanager
from typing import Optional
import asyncio
import os
import traceback
from datetime import datetime
# Third-party imports
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Local imports
if __name__ == "__main__":
    from shared.logger_config import setup_logger
    from database.schemas import NcFileDTO, IndividualRefFileDTO, EventRefFileDTO
    from database.models import EventStatus, NoaaEvent, EventRefFile
else:
    from src.shared.logger_config import setup_logger
    from src.database.schemas import NcFileDTO, IndividualRefFileDTO, EventRefFileDTO
    from src.database.models import EventStatus, NoaaEvent, EventRefFile


# Logging setup
logger = setup_logger("kerchunker", "db_tools")

# Get database URL
DATABASE_URL = os.getenv("SQLALCHEMY_DATABASE_URL")
logger.debug(f"Using DATABASE_URL: {DATABASE_URL}")

# Engine and session setup
engine = create_async_engine(DATABASE_URL, echo=False, pool_size=100, max_overflow=200)
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


async def clean_table(table_name: str):
    async with session_scope() as s:
        await s.execute(text(f"DELETE FROM {table_name}"))
        await s.commit()


async def upsert_nc_file(logger, nc_file: NcFileDTO):
    """Insert or update an NC file record based on s3_path."""
    try:
        async with session_scope() as s:
            query = text("""
                INSERT INTO nc_files (s3_path, date_time, product, event_id, reference_file_id)
                VALUES (:s3_path, :date_time, :product, :event_id, :reference_file_id)
                ON CONFLICT (s3_path) 
                DO UPDATE SET 
                    date_time = EXCLUDED.date_time,
                    product = EXCLUDED.product,
                    event_id = EXCLUDED.event_id,
                    reference_file_id = EXCLUDED.reference_file_id
                RETURNING *
            """)
            await s.execute(
                query,
                {
                    "s3_path": nc_file.s3_path,
                    "date_time": nc_file.date_time,
                    "product": nc_file.product,
                    "event_id": nc_file.event_id,
                    "reference_file_id": nc_file.reference_file_id,
                },
            )
    except Exception as e:
        logger.error(f"Failed to upsert NC file {nc_file.s3_path}: {e}")
        raise e


async def upsert_individual_ref_file_in_session(
    logger, session: AsyncSession, individual_ref_file: IndividualRefFileDTO
):
    """
    Upsert an IndividualRefFile using the *existing* transaction/session.
    """
    try:
        query = text("""
            INSERT INTO individual_reference_files (s3_path, date_time, product, event_id, nc_file_id)
            VALUES (:s3_path, :date_time, :product, :event_id, :nc_file_id)
            ON CONFLICT (s3_path) 
            DO UPDATE SET 
                date_time = EXCLUDED.date_time,
                product = EXCLUDED.product,
                event_id = EXCLUDED.event_id,
                nc_file_id = EXCLUDED.nc_file_id
            RETURNING id, s3_path, date_time, product, event_id, nc_file_id
        """)
        result = await session.execute(
            query,
            {
                "s3_path": individual_ref_file.s3_path,
                "date_time": individual_ref_file.date_time,
                "product": individual_ref_file.product,
                "event_id": individual_ref_file.event_id,
                "nc_file_id": individual_ref_file.nc_file_id,
            },
        )
        row = result.fetchone()
        return row  # row will have .id, .s3_path, etc.
    except Exception as e:
        logger.error(
            f"Failed to upsert IndividualRefFile {individual_ref_file.s3_path}: {e}"
        )
        raise e


async def upsert_nc_file_in_session(logger, session: AsyncSession, nc_file: NcFileDTO):
    """
    Upsert an NcFile using the *existing* transaction/session.
    Returns the upserted record.
    """
    try:
        query = text("""
            INSERT INTO nc_files (s3_path, date_time, product, event_id, reference_file_id)
            VALUES (:s3_path, :date_time, :product, :event_id, :reference_file_id)
            ON CONFLICT (s3_path) 
            DO UPDATE SET 
                date_time = EXCLUDED.date_time,
                product = EXCLUDED.product,
                event_id = EXCLUDED.event_id,
                reference_file_id = EXCLUDED.reference_file_id
            RETURNING *
        """)
        result = await session.execute(
            query,
            {
                "s3_path": nc_file.s3_path,
                "date_time": nc_file.date_time,
                "product": nc_file.product,
                "event_id": nc_file.event_id,
                "reference_file_id": nc_file.reference_file_id,
            },
        )
        return result.fetchone()
    except Exception as e:
        logger.error(f"Failed to upsert NcFile {nc_file.s3_path}: {e}")
        raise e
    

async def get_event(session: AsyncSession, event_id: int):
    result = await session.execute(text(f"SELECT * FROM noaa_events WHERE id = {event_id}"))
    return result.fetchone()


# async def update_event(session: AsyncSession, event_id: int, event_ref_file_id: int):
#     await session.execute(text(f"UPDATE noaa_events SET event_ref_file_id = {event_ref_file_id} WHERE id = {event_id}"))


async def get_events_for_event_ref_files(start_date: Optional[datetime] = None):
    async with session_scope() as s:
        query = select(NoaaEvent).outerjoin(
            EventRefFile, EventRefFile.event_id == NoaaEvent.id  # Join condition
        ).where(
            NoaaEvent.status == EventStatus.MAPPED,
            EventRefFile.id.is_(None)  # Filter for no matching EventRefFile
        )
        if start_date:
            query = query.where(NoaaEvent.date_time_start >= start_date)

        result = await s.execute(query)
        return result.scalars().all()


async def get_individual_ref_files(session: AsyncSession, event_id: int):
    result = await session.execute(text(f"SELECT * FROM individual_reference_files WHERE event_id = {event_id}"))
    return result.fetchall()


async def upsert_event_ref_file(session: AsyncSession, event_ref_file: EventRefFileDTO):
    query = text("""
        INSERT INTO event_reference_files (s3_path, product, event_id) 
        VALUES (:s3_path, :product, :event_id)
        ON CONFLICT (s3_path) 
        DO UPDATE SET 
            product = EXCLUDED.product,
            event_id = EXCLUDED.event_id
        RETURNING *
    """)
    result = await session.execute(
        query,
        {
            "s3_path": event_ref_file.s3_path,
            "product": event_ref_file.product,
            "event_id": event_ref_file.event_id,
        },
    )
    return result.fetchone()



if __name__ == "__main__":
    # asyncio.run(test_connection())
    # asyncio.run(clean_table("last_uploaded_date"))
    asyncio.run(clean_table("nc_files"))
