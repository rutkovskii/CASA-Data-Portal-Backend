import asyncio
from database import engine
from sqlalchemy import text


async def check_db():
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            await result.fetchone()
            print("Database connection successful!")
            return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(check_db())
    exit(0 if success else 1)
