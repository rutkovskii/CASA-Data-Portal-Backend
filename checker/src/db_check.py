import asyncio
import os
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

# Get database URL from environment variables
DATABASE_URL = os.getenv(
    "SQLALCHEMY_DATABASE_URL",
    "postgresql+asyncpg://admin:root@localhost:5432/casa_data_portal",
)


async def check_database():
    print(f"Using DATABASE_URL: {DATABASE_URL}")  # Debugging
    engine = create_async_engine(DATABASE_URL)

    try:
        async with engine.connect() as conn:
            # Check version
            result = await conn.execute(text("SELECT version();"))
            row = result.fetchone()
            version = row[0] if row else "Unknown"
            print(f"Connected to PostgreSQL: {version}")

            # Check if noo_records table exists
            result = await conn.execute(
                text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'noo_records'
                );
            """)
            )
            row = result.fetchone()
            table_exists = row[0] if row else False

            if table_exists:
                print("noo_records table exists")
            else:
                print("noo_records table does not exist")

    except Exception as e:
        import traceback

        traceback.print_exc()
        print(f"Error connecting to PostgreSQL: {e}")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(check_database())
