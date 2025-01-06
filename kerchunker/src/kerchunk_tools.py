import os
import asyncio
import traceback
import ujson
from functools import partial
from kerchunk.netCDF3 import NetCDF3ToZarr
from typing import List, Tuple


from src.tools import create_s3fs_client
from src.shared.tools import parse_file_datetime
from src.config.config import Config
from src.db_tools import (
    IndividualRefFileDTO,
    _upsert_nc_file_in_session,
    _upsert_individual_ref_file_in_session,
    session_scope,
    NcFileDTO,
    IndividualRefFileDTO,
)

# TODO: logging
# TODOL Build a Single Multi-Row INSERT/UPSERT
# For truly massive batches, you could build an SQL statement that inserts/updates
# all reference files in one go (using multiple VALUES rows), then one statement
# for all NcFiles. That would further reduce the number of queries from 2n to 2
# or each batch.
# However, this is more complex: you’d have to gather the newly inserted reference
# IDs, then map them back to the NcFiles. Typically you’d need a temporary table or
# some advanced Postgres features (like WITH ... INSERT ... RETURNING ...) to handle
# the cross-references.

# TODO: Reference file creation via Dask (THE MAIN BOTTLENECK is the Kerchunk library)


def _sync_generate_json_reference(s3fs_client, s3_filepath):
    """
    Use Kerchunk's `NetCDF3ToZarr` method to create a `Kerchunk` index from a NetCDF file.

    temp_dir example: /tmp/kerchunk_temp/rainfall/20240101/
    """
    # Get the filename from the input file path
    chunks = NetCDF3ToZarr(
        os.path.join(Config.S3_URL_TEMPLATE, s3_filepath), inline_threshold=100
    )
    reference = chunks.translate()

    s3_dir = os.path.dirname(s3_filepath)
    filename = os.path.basename(s3_filepath)
    filename_without_extension = filename.removesuffix(".nc")
    filename_json = f"{filename_without_extension}.json"

    # s3_key example: # ref_files/rainfall/20240101/20240101_000000.json
    ref_filepath = os.path.join("ref_files", s3_dir, filename_json)

    # TODO: Do we need this?
    # u_path = os.path.join(Config.S3_URL_TEMPLATE, ref_filepath)
    # reference["templates"] = {"u": u_path}

    # Upload to S3
    path = os.path.join(Config.S3_BUCKET_NAME, ref_filepath)
    with s3fs_client.open(path, "wb") as f:
        f.write(ujson.dumps(reference).encode())

    logger.info(f"Generated reference file for {s3_filepath}")

    return (ref_filepath, s3_filepath)


async def generate_json_reference(s3fs_client, s3_filepath):
    """
    Async wrapper that calls the synchronous Kerchunk + S3 write logic
    in a worker thread using asyncio.to_thread.
    """
    func = partial(_sync_generate_json_reference, s3fs_client, s3_filepath)
    return await asyncio.to_thread(func)


async def batch_upsert_refs(
    logger,
    items: List[Tuple[str, str]],  # list of (ref_path, nc_path)
    batch_size: int = 100,
):
    """
    Batch upsert logic that creates bidirectional relationships between
    nc_files and individual_reference_files tables.
    """
    total = len(items)
    for start in range(0, total, batch_size):
        end = start + batch_size
        batch = items[start:end]

        async with session_scope() as session:
            for ref_path, nc_path in batch:
                try:
                    product = nc_path.split("/")[0]
                    date_time = parse_file_datetime(os.path.basename(nc_path), product)

                    # 1. First, upsert the NcFile without reference_file_id
                    nc_dto = NcFileDTO(
                        s3_path=nc_path,
                        date_time=date_time,
                        product=product,
                        reference_file_id=None,  # We'll update this later
                    )
                    nc_result = await _upsert_nc_file_in_session(
                        logger, session, nc_dto
                    )
                    if not nc_result or not nc_result.id:
                        logger.warning(f"No nc_file ID returned for {nc_path}")
                        continue

                    nc_file_id = nc_result.id

                    # 2. Then, upsert reference file with nc_file_id
                    ref_dto = IndividualRefFileDTO(
                        s3_path=ref_path,
                        date_time=date_time,
                        product=product,
                        nc_file_id=nc_file_id,  # Link to nc_file
                    )
                    ref_result = await _upsert_individual_ref_file_in_session(
                        logger, session, ref_dto
                    )
                    if not ref_result or not ref_result.id:
                        logger.warning(f"No ref_file ID returned for {ref_path}")
                        continue

                    # 3. Finally, update the nc_file with reference_file_id
                    nc_dto.reference_file_id = ref_result.id
                    await _upsert_nc_file_in_session(logger, session, nc_dto)

                    logger.info(f"Processed {nc_path} -> {ref_path}")

                except Exception as e:
                    traceback_str = traceback.format_exc()
                    logger.error(
                        f"Failed to process file {nc_path} -> {ref_path}: {e}\n{traceback_str}"
                    )


async def process_reference_files(logger, files: list[str]):
    # Group files by product and date
    s3fs_client = create_s3fs_client()

    # Phase 1: Generate individual references
    semaphore = asyncio.Semaphore(os.cpu_count() or 4)  # Limit to CPU count

    async def bounded_generate_reference(nc_path):
        async with semaphore:
            return await generate_json_reference(s3fs_client, nc_path)

    reference_tasks = [bounded_generate_reference(nc_path) for nc_path in files]
    items = await asyncio.gather(*reference_tasks)

    # Phase 2: Add references to the database
    await batch_upsert_refs(logger, items, batch_size=200)
