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


# TODO: 
# - Check if dimensions are appropriate in combined file: 
#   Dimensions:   (datetime: 205, time: 1, z0: 1, y0: 366, x0: 350)
# Coordinates:
#   * datetime  (datetime) datetime64[ns] 2kB 2018-02-24T09:45:01 ... 2018-02-2...
#   * time      (time) datetime64[ns] 8B 2018-02-24T12:35:31
#   * x0        (x0) float32 1kB -97.99 -97.99 -97.98 ... -96.25 -96.25 -96.24
#   * y0        (y0) float32 1kB 31.77 31.78 31.78 31.79 ... 33.58 33.59 33.6 33.6
#   * z0        (z0) float32 4B 0.5

import asyncio
import time
from datetime import datetime

from contextlib import asynccontextmanager

from src.tools import list_files
from src.db_tools import get_events_for_event_ref_files
from src.netcdf_tools import process_nc_files
from src.kerchunk_ind_refs import process_reference_files
from src.kerchunk_event_refs import process_event_ref_files
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


async def generate_individual_ref_files():
    files = list_files(logger, Config.S3_BUCKET_NAME, "rainfall/**/*.nc")

    logger.info(f"Total files to process: {len(files)}")  # 237643
    if PROCESS_NC_FILES:
        await process_nc_files(logger, files)

    async with timer("process_reference_files"):
        await process_reference_files(logger, files)


async def generate_event_ref_files():
    events = await get_events_for_event_ref_files(start_date=datetime(2018, 1, 1))
    logger.info(f"Total events to process: {len(events)}")
    async with timer("process_event_ref_files"):
        await process_event_ref_files(logger, events)

if __name__ == "__main__":
    # asyncio.run(generate_individual_ref_files())
    asyncio.run(generate_event_ref_files())