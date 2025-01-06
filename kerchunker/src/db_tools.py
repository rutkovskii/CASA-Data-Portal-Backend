# Python standard library imports
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Optional
import asyncio
import os
import traceback

# Third-party imports
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Local imports
if __name__ == "__main__":
    from database.models import NcFile, IndividualRefFile
    from shared.logger_config import setup_logger
else:
    from src.database.models import NcFile, IndividualRefFile
    from src.shared.logger_config import setup_logger


# Logging setup
logger = setup_logger("uploader", "db_tools")

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


async def test_connection():
    async with session_scope() as s:
        result = await s.execute(text("SELECT version();"))
        row = result.fetchone()
        version = row[0] if row else "Unknown"
        logger.info(f"Connected to PostgreSQL: {version}")


async def clean_table(table_name: str):
    async with session_scope() as s:
        await s.execute(text(f"DELETE FROM {table_name}"))
        await s.commit()


class NcFileDTO(BaseModel):
    id: Optional[int] = None
    s3_path: str
    date_time: datetime
    product: str
    event_id: Optional[int] = None
    reference_file_id: Optional[int] = None


async def post_nc_file(nc_file: NcFileDTO):
    async with session_scope() as s:
        db_nc_file = NcFile(
            s3_path=nc_file.s3_path,
            date_time=nc_file.date_time,
            product=nc_file.product,
            event_id=nc_file.event_id,
            reference_file_id=nc_file.reference_file_id,
        )
        s.add(db_nc_file)
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


async def update_nc_file_with_ref(logger, nc_file_id, ref_file_id):
    """Update the NcFile with the reference_file_id."""
    try:
        async with session_scope() as s:
            query = text("""
                UPDATE nc_files
                SET reference_file_id = :ref_file_id
                WHERE id = :nc_file_id
            """)
            await s.execute(
                query, {"ref_file_id": ref_file_id, "nc_file_id": nc_file_id}
            )
            logger.info(
                f"Updated NcFile ID {nc_file_id} with reference_file_id {ref_file_id}"
            )
    except Exception as e:
        logger.error(
            f"Failed to update NcFile ID {nc_file_id} with reference_file_id {ref_file_id}: {e}"
        )
        raise


class IndividualRefFileDTO(BaseModel):
    id: Optional[int] = None
    s3_path: str
    date_time: datetime
    product: str
    event_id: Optional[int] = None
    nc_file_id: Optional[int] = None


async def upsert_individual_ref_file(individual_ref_file: IndividualRefFileDTO):
    """Insert or update an Individual Reference File record based on s3_path."""
    try:
        async with session_scope() as s:
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
            result = await s.execute(
                query,
                {
                    "s3_path": individual_ref_file.s3_path,
                    "date_time": individual_ref_file.date_time,
                    "product": individual_ref_file.product,
                    "event_id": individual_ref_file.event_id,
                    "nc_file_id": individual_ref_file.nc_file_id,
                },
            )
            return result.fetchone()  # Return the upserted row for verification/logging
    except Exception as e:
        logger.error(
            f"Failed to upsert Individual Reference File {individual_ref_file.s3_path}: {e}"
        )
        raise


async def get_nc_file_by_s3_path(s3_path: str):
    async with session_scope() as session:
        result = await session.execute(
            text("SELECT * FROM nc_files WHERE s3_path = :s3_path"),
            {"s3_path": s3_path},
        )
        return result.fetchone()  # Returns the first matching NcFile or None


async def _upsert_individual_ref_file_in_session(
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


async def _upsert_nc_file_in_session(logger, session: AsyncSession, nc_file: NcFileDTO):
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


if __name__ == "__main__":
    # asyncio.run(test_connection())
    # asyncio.run(clean_table("last_uploaded_date"))
    asyncio.run(clean_table("nc_files"))
