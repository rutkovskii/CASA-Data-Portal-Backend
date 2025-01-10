import os
import asyncio
import ujson
import numpy as np
from typing import List, Tuple
from pprint import pprint

from kerchunk.combine import MultiZarrToZarr

from src.tools import create_s3fs_client
from src.config.config import Config
from src.db_tools import (
    get_individual_ref_files,
    upsert_event_ref_file,
    session_scope,
    get_event,
)
from src.database.schemas import EventRefFileDTO
from src.shared.tools import parse_file_datetime_infer_simple
from src.shared.noaa_product_to_product import event_to_product_map


def fn_to_datetime(index, fs, var, fn):
    """
    Kerchunk coordinate function: attempts to parse datetime from filename
    using parse_file_datetime_infer_simple().
    """
    if not fn:
        raise ValueError("Filename is None or empty.")

    dt = parse_file_datetime_infer_simple(fn)
    if dt is None:
        raise ValueError(f"Could not parse datetime from {fn}")

    return np.datetime64(dt, 'ns')


def _sync_generate_combined_json_reference(s3fs_client, s3_rf_paths, event_id):
    """
    Use Kerchunk's `NetCDF3ToZarr` method to create a `Kerchunk` index from a NetCDF file.

    temp_dir example: /tmp/kerchunk_temp/rainfall/20240101/
    """
    
    ind_rf_paths = []
    for rf_path in s3_rf_paths:
        ind_rf_paths.append(os.path.join(Config.S3_URL_TEMPLATE, rf_path))

    print(len(ind_rf_paths))

    ind_rfs = []
    for rf_path in s3_rf_paths:
        with s3fs_client.open(os.path.join(Config.S3_BUCKET_NAME, rf_path), "rb") as f:
            ind_rfs.append(ujson.load(f))

    print(len(ind_rfs))

    # Get the filename from the input file path
    mzz = MultiZarrToZarr(
        path=ind_rf_paths,
        indicts=ind_rfs,
        coo_map={'datetime': fn_to_datetime},
        coo_dtypes={'datetime': np.dtype('M8[ns]')},
        concat_dims=['datetime'],
        identical_dims=["y0", "x0", "z0"],
        inline_threshold=100,
    )
        
    combined_reference = mzz.translate()

    filename_json = f"{event_id}.json"
    ref_filepath = os.path.join("event_ref_files/noaa/", filename_json)

    # Upload to S3
    path = os.path.join(Config.S3_BUCKET_NAME, ref_filepath)
    with s3fs_client.open(path, "wb") as f:
        f.write(ujson.dumps(combined_reference).encode())

    # logger.info(f"Generated reference file for {s3_filepath}")

    return (ref_filepath, event_id)


async def batch_handle_event_ref_files(
    logger,
    items: List[Tuple[str, int]],  # list of (ref_filepath, event_id)
    batch_size: int = 100
):
    """
    Batch process event reference files in smaller chunks to manage database load.
    """
    total = len(items)
    for start in range(0, total, batch_size):
        end = start + batch_size
        batch = items[start:end]

        async with session_scope() as session:
            for ref_filepath, event_id in batch:
                try:
                    # Get event and determine product
                    event = await get_event(session, event_id)
                    product = event_to_product_map[event.noaa_product][0]

                    # Create and upsert event_ref_file
                    event_ref_file_dto = EventRefFileDTO(
                        s3_path=ref_filepath,
                        product=product,
                        event_id=event_id
                    )
                    event_ref_file = await upsert_event_ref_file(session, event_ref_file_dto)

                    # Update event with reference
                    # await update_event(session, event_id, event_ref_file.id)

                    # event_ref_file.event_id = event_id
                    # session.add(event_ref_file)
                    # await session.commit()

                    
                    logger.info(f"Processed event {event_id} -> {ref_filepath}")

                except Exception as e:
                    logger.error(f"Failed to process event {event_id}: {str(e)}")

async def process_event_ref_files(logger, events: list, batch_size: int = 100):
    """Process multiple events to generate and store combined reference files."""
    semaphore = asyncio.Semaphore(os.cpu_count() or 4)
    s3fs_client = create_s3fs_client()

    # Phase 1: Generate combined references
    async def bounded_generate_combined_ref_file(event):
        async with semaphore:
            async with session_scope() as s:
                individual_ref_files = await get_individual_ref_files(s, event.id)

            logger.info(f"Found {len(individual_ref_files)} individual reference files for event {event.id}")
            
            # Run the sync function in a thread pool
            return await asyncio.to_thread(
                _sync_generate_combined_json_reference,
                s3fs_client,
                [rf.s3_path for rf in individual_ref_files],
                event.id
            )

    combined_ref_file_tasks = [
        bounded_generate_combined_ref_file(event) 
        for event in events
    ]
    items = await asyncio.gather(*combined_ref_file_tasks)

    # Phase 2: Batch process database updates
    await batch_handle_event_ref_files(logger, items, batch_size=batch_size)






# get events
# for event in events:
    # from event get individual ref files
    # (Create tasks) based on the individual ref files, generate a combined ref file + upload
# Gather tasks and run
# Bulk upsert the combined ref files to the database
