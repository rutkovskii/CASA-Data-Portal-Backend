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
    from database.models import LastUploadedDate, FailedUpload, NcFile
    from shared.logger_config import setup_logger
else:
    from src.database.models import LastUploadedDate, FailedUpload, NcFile
    from src.shared.logger_config import setup_logger


# Logging setup
logger = setup_logger("uploader", "db_tools")

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


class LastUploadedDateDTO(BaseModel):
    id: Optional[int] = None
    product: str
    date: date


async def test_connection():
    async with session_scope() as s:
        result = await s.execute(text("SELECT version();"))
        row = result.fetchone()
        version = row[0] if row else "Unknown"
        logger.info(f"Connected to PostgreSQL: {version}")


async def get_last_uploaded_date(product: str):
    async with session_scope() as s:
        result = await s.execute(
            text("SELECT * FROM last_uploaded_date WHERE product = :product"),
            {"product": product},
        )
        return result.fetchone()


async def get_all_last_uploaded_dates():
    async with session_scope() as s:
        result = await s.execute(text("SELECT * FROM last_uploaded_date"))
        return result.fetchall()


async def post_last_uploaded_date(last_uploaded_date: LastUploadedDateDTO):
    """Create and return a new NOAA record"""
    async with session_scope() as s:
        db_last_uploaded_date = LastUploadedDate(
            product=last_uploaded_date.product, date=last_uploaded_date.date
        )
        s.add(db_last_uploaded_date)
        await s.commit()
        await s.refresh(db_last_uploaded_date)  # Refresh to get the generated ID
        return db_last_uploaded_date


# put last uploaded date
async def put_last_uploaded_date(last_uploaded_date: LastUploadedDateDTO):
    async with session_scope() as s:
        await s.execute(
            text("UPDATE last_uploaded_date SET date = :date WHERE product = :product"),
            {"date": last_uploaded_date.date, "product": last_uploaded_date.product},
        )
        await s.commit()


# delete last uploaded date
async def delete_last_uploaded_date(product: str):
    async with session_scope() as s:
        await s.execute(
            text("DELETE FROM last_uploaded_date WHERE product = :product"),
            {"product": product},
        )
        await s.commit()


async def upsert_last_uploaded_date(last_uploaded_date: LastUploadedDateDTO):
    """Insert or update the last uploaded date for a product."""
    async with session_scope() as s:
        # Use ON CONFLICT to handle UPSERT behavior
        query = text(
            """
            INSERT INTO last_uploaded_date (product, date)
            VALUES (:product, :date)
            ON CONFLICT (product)
            DO UPDATE SET date = EXCLUDED.date
            """
        )
        await s.execute(
            query,
            {"product": last_uploaded_date.product, "date": last_uploaded_date.date},
        )
        await s.commit()


# clearn table
async def clean_table(table_name: str):
    async with session_scope() as s:
        await s.execute(text(f"DELETE FROM {table_name}"))
        await s.commit()


async def clean_all_tables():
    async with session_scope() as s:
        # Delete events first to remove foreign key references
        await s.execute(text("DELETE FROM noaa_events"))
        # Then delete records
        await s.execute(text("DELETE FROM noaa_records"))
        # Then delete last uploaded date
        await s.execute(text("DELETE FROM last_uploaded_date"))
        await s.commit()


class FailedUploadDTO(BaseModel):
    id: Optional[int] = None
    remote_path: str
    product: str
    date_dir: str
    last_error: str
    last_attempt: Optional[datetime] = None


async def log_failed_upload(failed_upload: FailedUploadDTO):
    """Log a failed upload attempt to the database"""
    async with session_scope() as s:
        query = text("""
            INSERT INTO failed_uploads (remote_path, product, date_dir, last_error, last_attempt)
            VALUES (:remote_path, :product, :date_dir, :last_error, NOW())
            ON CONFLICT (remote_path) 
            DO UPDATE SET 
                last_error = :last_error,
                last_attempt = NOW()
            RETURNING *
        """)
        await s.execute(
            query,
            {
                "remote_path": failed_upload.remote_path,
                "product": failed_upload.product,
                "date_dir": failed_upload.date_dir,
                "last_error": failed_upload.last_error,
            },
        )
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


if __name__ == "__main__":
    # asyncio.run(test_connection())
    # get all last uploaded dates
    print(asyncio.run(get_all_last_uploaded_dates()))
    # asyncio.run(clean_table("last_uploaded_date"))
