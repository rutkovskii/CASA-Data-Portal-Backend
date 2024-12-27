import asyncio
import sys
from database import init_db

async def create_tables():
    try:
        print("Initializing database tables...")
        await init_db()
        print("Database tables created successfully!")
    except Exception as e:
        print(f"Error creating tables: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(create_tables())