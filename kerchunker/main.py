# Steps:
# 1. Connect to OSN S3 using manager or with s3fs
# 2. Get all files in the bucket
# 3. For each file, create an item in the database in NcFile table (as if uploader did it)
# 4. For each file, generate a reference file and create an item in the database in NcReferenceFile table
# - create a temp directory
# - using the file list, iterate over each file and generate a reference file
# - save the reference file to the temp directory
# - create an item in the database in NcReferenceFile table
# - upload the reference file to the S3 bucket
# - delete the temp directory

import asyncio
import time

# import context manager
from contextlib import asynccontextmanager

from src.tools import list_files
from src.netcdf_tools import process_nc_files
from src.kerchunk_tools import process_reference_files
from src.config.config import Config
from src.shared.logger_config import setup_logger


# Variables
PROCESS_NC_FILES = False

logger = setup_logger("kerchunker", "main", append=False)


# make timer wrapper
@asynccontextmanager
async def timer(name):
    start = time.time()
    yield
    end = time.time()
    # Format the time to 2 decimal places
    formatted_time = "{:.2f}".format(end - start)
    logger.info(f"{name} took {formatted_time} seconds")


async def main():
    files = list_files(logger, Config.S3_BUCKET_NAME, "rainfall/**/*.nc")

    logger.info(f"Total files to process: {len(files)}")  # 237643
    if PROCESS_NC_FILES:
        await process_nc_files(logger, files)

    async with timer("process_reference_files"):
        await process_reference_files(logger, files)


if __name__ == "__main__":
    asyncio.run(main())
