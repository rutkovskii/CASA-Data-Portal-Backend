from src.shared.S3Manager import S3Manager
from config.config import Config
import asyncio

s3_manager = S3Manager(endpoint=Config.ENDPOINT, anonymous=True)
asyncio.run(s3_manager.initialize())

asyncio.run(s3_manager.list_files(Config.S3_BUCKET_NAME, folder="hail/20241228"))

asyncio.run(s3_manager.close())
