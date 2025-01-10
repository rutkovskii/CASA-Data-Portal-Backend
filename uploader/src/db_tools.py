# Python standard library imports
from contextlib import asynccontextmanager
import asyncio
import os
import traceback

# Third-party imports
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Local imports
if __name__ == "__main__":
    from database.models import LastUploadedDate, NcFile
    from database.schemas import LastUploadedDateDTO, FailedUploadDTO, NcFileDTO
    from shared.logger_config import setup_logger
else:
    from src.database.models import LastUploadedDate, NcFile
    from src.database.schemas import LastUploadedDateDTO, FailedUploadDTO, NcFileDTO
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


# clean table
async def clean_table(table_name: str):
    async with session_scope() as s:
        await s.execute(text(f"DELETE FROM {table_name}"))
        await s.commit()


if __name__ == "__main__":
    # asyncio.run(clean_table("last_uploaded_date"))
    pass
