import os
import socket
import traceback
import asyncio
from datetime import datetime, timedelta

import pysftp

from src.uploader_tools import (
    get_product_base_path,
    create_temp_dir,
    cleanup_temp_dir,
    list_of_radars,
    calculate_days_in_range,
)
from src.db_tools import (
    LastUploadedDateDTO,
    FailedUploadDTO,
    NcFileDTO,
    upsert_last_uploaded_date,
    log_failed_upload,
    post_nc_file,
)
from src.shared.S3Manager import S3Manager, get_transfer_config
from src.shared.tools import add_time_dimension, parse_file_datetime
from src.config.config import Config
from src.shared.logger_config import setup_logger

# Initialize logger
logger = setup_logger("uploader", "uploader", append=False)


def get_mgh5_connection():
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    return pysftp.Connection(
        host=Config.MGH5_HOST,
        username=Config.MGH5_K_USER,
        password=Config.MGH5_K_PASSWORD,
        cnopts=cnopts,
    )


async def get_s3_manager():
    s3_manager = S3Manager(
        endpoint=Config.S3_ENDPOINT_URL,
        access_key=Config.S3_ACCESS_KEY,
        secret_key=Config.S3_SECRET_KEY,
    )
    await s3_manager.initialize()
    return s3_manager


def get_radar_dirs(base_path):
    with get_mgh5_connection() as sftp:
        return [rd for rd in sftp.listdir(base_path) if rd in list_of_radars]


def get_relevant_dates(base_path, days_to_check):
    with get_mgh5_connection() as sftp:
        date_dirs = sorted(sftp.listdir(base_path))
        relevant_dates = [
            date_dir
            for date_dir in date_dirs
            if date_dir in [d.strftime("%Y%m%d") for d in days_to_check]
        ]
        return relevant_dates


def get_files(base_path, date_dir):
    with get_mgh5_connection() as sftp:
        return [
            f
            for f in sftp.listdir(os.path.join(base_path, date_dir))
            if f.endswith((".nc", ".nc.gz"))
        ]


async def upload_file(
    s3_manager, remote_file_path, temp_file_path, s3_prefix, filename, date_dir, product
):
    """
    Uploads a file to S3.
    Adds time dimension to the file if it is a gzipped netcdf file.
    In case of failure, logs the error and the file pathto the database.
    """
    retry_attempts = 3
    temp_file = None
    for attempt in range(retry_attempts):
        try:
            if not temp_file:
                temp_file = add_time_dimension(temp_file_path)
            s3_key = os.path.join(s3_prefix, filename.replace(".gz", ""))
            await s3_manager.upload_file_async(
                temp_file,
                Config.S3_BUCKET_NAME,
                s3_key,
                transfer_config=get_transfer_config(),
            )

            nc_file = NcFileDTO(
                s3_path=s3_key,
                date_time=parse_file_datetime(filename, product),
                product=product,
            )
            await post_nc_file(nc_file)

            logger.info(f"Successfully uploaded {filename} to {s3_key}")
            break
        except Exception as e:
            if attempt == retry_attempts - 1:  # If last attempt
                error_msg = f"{str(e)}\n{traceback.format_exc()}"
                logger.error(
                    f"Failed to upload {filename} after {retry_attempts} attempts. Error uploading {temp_file_path}: {error_msg}"
                )

                await log_failed_upload(
                    FailedUploadDTO(
                        remote_path=remote_file_path,
                        product=product,
                        date_dir=date_dir,
                        last_error=error_msg,
                    )
                )


def download_file(sftp, base_path, temp_dir, date_dir, filename, max_retries=3):
    """
    Handle the download of a single file with SFTP connection management.

    Args:
        sftp: SFTP connection object or None
        base_path: Base path on remote server
        temp_dir: Local temporary directory
        date_dir: Date directory name
        filename: File to download
        max_retries: Maximum number of retry attempts

    Returns:
        tuple: (temp_file_path, sftp_connection)
    """
    remote_file_path = os.path.join(base_path, date_dir, filename)
    temp_file_path = os.path.join(temp_dir, filename)

    for attempt in range(max_retries):
        try:
            if not sftp:
                sftp = get_mgh5_connection()

            sftp.get(remote_file_path, temp_file_path)
            return temp_file_path, sftp

        except socket.error:
            if sftp:
                try:
                    sftp.close()
                except Exception:
                    pass
            sftp = None

            if attempt == max_retries - 1:
                logger.error(
                    f"Failed to download {filename} after {max_retries} attempts"
                )
                raise

    raise Exception(f"Max retries reached for {filename}")


async def process_files(base_path, temp_dir, s3_root, date_dir, files, product):
    tasks = []
    s3_manager = await get_s3_manager()
    sftp = None

    try:
        for filename in files:
            try:
                temp_file_path, sftp = download_file(
                    sftp, base_path, temp_dir, date_dir, filename
                )
                tasks.append(
                    asyncio.create_task(
                        upload_file(
                            s3_manager,
                            os.path.join(base_path, date_dir, filename),
                            temp_file_path,
                            f"{s3_root}/{date_dir}",
                            filename,
                            date_dir,
                            product,
                        )
                    )
                )
            except Exception as e:
                logger.error(f"Error processing file {filename}: {e}")

        if tasks:
            await asyncio.gather(*tasks)

    finally:
        if sftp:
            sftp.close()
        await s3_manager.close()


async def process_date_dir(base_path, temp_dir, s3_root, product, date_dir):
    files = get_files(base_path, date_dir)
    file_count = len(files)
    logger.info(
        f"Scanning directory {base_path}/{date_dir} containing {file_count} files"
    )

    temp_date_dir = os.path.join(temp_dir, date_dir)
    os.makedirs(temp_date_dir, exist_ok=True)

    await process_files(base_path, temp_date_dir, s3_root, date_dir, files, product)

    # Convert date_dir string (YYYYMMDD) to date object before passing to DTO
    date_obj = datetime.strptime(date_dir, "%Y%m%d").date()
    await upsert_last_uploaded_date(LastUploadedDateDTO(product=product, date=date_obj))
    logger.info(f"Last uploaded date for {product} set to {date_obj}")

    cleanup_temp_dir(temp_date_dir)


async def process_directory(base_path, temp_dir, s3_root, days_to_check, product):
    relevant_dates = get_relevant_dates(base_path, days_to_check)

    if not relevant_dates:
        logger.info(f"No relevant dates found in {base_path}")
        return

    for date_dir in relevant_dates:
        await process_date_dir(base_path, temp_dir, s3_root, product, date_dir)

    cleanup_temp_dir(temp_dir)
    logger.info(f"Cleaned up temp directory {temp_dir}")


async def copy_mgh5_to_osn(
    begindatetime=datetime.now().date() - timedelta(days=1),
    enddatetime=datetime.now().date(),
    product_types=None,
):
    """
    Copies data from MGH5 to OSN bucket maintaining the same directory structure, but with product name.
    Uses multiple upload workers for better throughput.
    TODO: Add maintainance and speed up the process
    TODO: Improvement can be obtained if MGH5 can support multiple connectoins.
    """

    if product_types is None:
        product_types = ["hail", "rainfall", "singleradar"]

    temp_dir = create_temp_dir()

    for product in product_types:
        base_path = get_product_base_path(product)
        logger.info(f"Processing product: {product} from {base_path}")

        days_to_check = calculate_days_in_range(begindatetime, enddatetime)
        logger.info(f"# of days to check: {len(days_to_check)}")

        if len(days_to_check) == 0:
            logger.info(f"No days to check for {product}")
            continue

        if product == "singleradar":
            radar_dirs = get_radar_dirs(base_path)
            for radar_dir in radar_dirs:
                radar_path = os.path.join(base_path, radar_dir)
                prefix = os.path.join(product, radar_dir)
                await process_directory(
                    base_path=radar_path,
                    temp_dir=os.path.join(temp_dir, prefix),
                    s3_root=prefix,
                    days_to_check=days_to_check,
                    product=product,
                )
        else:
            await process_directory(
                base_path=base_path,
                temp_dir=os.path.join(temp_dir, product),
                s3_root=product,
                days_to_check=days_to_check,
                product=product,
            )

    cleanup_temp_dir(temp_dir)


if __name__ == "__main__":
    try:
        # start = datetime(2018, 1, 1).date()
        start = datetime(2018, 5, 12).date()
        end = datetime(2018, 12, 31).date()
        asyncio.run(copy_mgh5_to_osn(start, end))
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}\n{traceback.format_exc()}")
        raise
