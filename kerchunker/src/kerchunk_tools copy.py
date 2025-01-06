import os
import re
import shutil
import datetime
import asyncio
import traceback
import numpy as np
import ujson

from kerchunk.netCDF3 import NetCDF3ToZarr

from src.tools import create_s3fs_client
from src.config.config import Config
from src.db_tools import (
    IndividualRefFileDTO,
    upsert_individual_ref_file,
    get_nc_file_by_s3_path,
    update_nc_file_with_ref,
)
from src.shared.tools import parse_file_datetime
from src.shared.S3Manager import S3Manager, get_transfer_config


semaphore = asyncio.Semaphore(50)


async def get_s3_manager():
    s3_manager = S3Manager(
        endpoint=Config.S3_ENDPOINT_URL,
        access_key=Config.S3_ACCESS_KEY,
        secret_key=Config.S3_SECRET_KEY,
    )
    await s3_manager.initialize()
    return s3_manager


def fn_to_datetime(index, fs, var, fn):
    """
    Extract datetime from filename in the format 'YYYYMMDD_HHMMSS.nc'
    """
    if fn is None:
        raise ValueError("Filename (fn) is None")

    match = re.search(r"(\d{8})_(\d{6})", fn)
    if not match:
        raise ValueError(f"Filename {fn} does not match expected pattern")

    date_str, time_str = match.groups()
    dt_str = f"{date_str}{time_str}"
    dt = datetime.datetime.strptime(dt_str, "%Y%m%d%H%M%S")

    return np.datetime64(dt, "ns")


def generate_json_reference(s3fs_client, s3_filepath, temp_dir: str):
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

    # /tmp/kerchunk_temp/rainfall/20240101/20240101_000000.json
    temp_ref_filepath = os.path.join(temp_dir, filename_json)

    # s3_key example: # ref_files/rainfall/20240101/20240101_000000.json
    ref_filepath = os.path.join("ref_files", s3_dir, filename_json)

    # TODO: Do we need this?
    # u_path = os.path.join(Config.S3_URL_TEMPLATE, ref_filepath)
    # reference["templates"] = {"u": u_path}

    # Upload to S3
    path = os.path.join(Config.S3_BUCKET_NAME, ref_filepath)
    with s3fs_client.open(path, "wb") as f:
        f.write(ujson.dumps(reference).encode())

    return temp_ref_filepath, ref_filepath

    # Local
    # with open(ref_filepath, "wb") as f:
    #     f.write(ujson.dumps(reference).encode())
    # print(f"Kerchunk reference file generated: {temp_ref_filepath}")
    # return temp_ref_filepath, ref_filepath


async def upload_reference_file(
    s3_manager, logger, temp_ref_filepath: str, ref_filepath: str
):
    retry_attempts = 3
    for attempt in range(retry_attempts):
        try:
            await s3_manager.upload_file_async(
                temp_ref_filepath,
                Config.S3_BUCKET_NAME,
                ref_filepath,  # key
                transfer_config=get_transfer_config(),
            )
            break
        except Exception as e:
            if attempt == retry_attempts - 1:  # If last attempt
                error_msg = f"{str(e)}\n{traceback.format_exc()}"
                logger.error(
                    f"Failed to upload reference file {temp_ref_filepath} to {ref_filepath} after {retry_attempts} attempts: {error_msg}"
                )


async def add_ref_to_IndividualRefFile(logger, ref_filepath, nc_filepath):
    """
    Add the reference file to the IndividualRefFile table and link it to the corresponding NcFile.
    """
    try:
        # Get product from the filepath: <bucket>/<product>/<date>/<filename>.nc
        product = nc_filepath.split("/")[0]
        date_time = parse_file_datetime(os.path.basename(nc_filepath), product)

        # Fetch the corresponding NcFile based on the s3_path
        nc_file = await get_nc_file_by_s3_path(nc_filepath)
        if nc_file is None:
            logger.warning(f"No NcFile found for {nc_filepath}")
            return  # Early return if no NcFile found

        individual_ref_file_dto = IndividualRefFileDTO(
            s3_path=ref_filepath,
            date_time=date_time,
            product=product,
            nc_file_id=nc_file.id,  # Link to NcFile if it exists
        )

        # Upsert the reference file and update the NcFile with the reference_file_id
        ref_file = await upsert_individual_ref_file(individual_ref_file_dto)
        if ref_file and ref_file.id:
            await update_nc_file_with_ref(logger, nc_file.id, ref_file.id)

    except Exception as e:
        logger.error(f"Failed to add reference file to IndividualRefFile table: {e}")
        raise e


async def process_reference_file(
    s3fs_client, s3_manager, logger, nc_filepath: str, temp_dir: str
):
    temp_ref_filepath, ref_filepath = generate_json_reference(
        s3fs_client, nc_filepath, temp_dir
    )
    # await upload_reference_file(s3_manager, logger, temp_ref_filepath, ref_filepath)
    await add_ref_to_IndividualRefFile(logger, ref_filepath, nc_filepath)


async def limited_process_reference_file(
    s3fs_client, s3_manager, logger, file, temp_dir
):
    async with semaphore:
        return await process_reference_file(
            s3fs_client, s3_manager, logger, file, temp_dir
        )


async def process_reference_files(logger, files: list[str]):
    # Group files by product and date
    grouped_files = {}
    for file in files:
        file = file.removeprefix(Config.S3_BUCKET_NAME).removeprefix("/")

        parts = file.split("/")
        product = parts[0]  # Assuming the product is the first part of the path
        date_dir = (
            parts[1]
            if not product == "single_radar"
            else os.path.join(parts[1], parts[2])
        )
        key = (product, date_dir)
        if key not in grouped_files:
            grouped_files[key] = []
        grouped_files[key].append(file)

    for (product, date_dir), file_group in grouped_files.items():
        temp_dir = f"/tmp/kerchunk_temp/{product}/{date_dir}"
        os.makedirs(temp_dir, exist_ok=True)

        s3fs_client = create_s3fs_client()
        s3_manager = await get_s3_manager()

        # Create a list of tasks for concurrent execution
        tasks = [
            limited_process_reference_file(
                s3fs_client, s3_manager, logger, file, temp_dir
            )
            for file in file_group
        ]
        # Run tasks concurrently
        await asyncio.gather(*tasks)

        # Clean up the temp directory for this product/date
        shutil.rmtree(temp_dir)
