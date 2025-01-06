from src.shared.tools import parse_file_datetime
from src.db_tools import NcFileDTO, upsert_nc_file
from src.config.config import Config


async def add_nc_to_NcFile(logger, file):
    """
    Add NC file to the NcFile table.
    """
    try:
        product = file.split("/")[0]
        filename = file.split("/")[-1]
        date_time = parse_file_datetime(filename, product)
        await upsert_nc_file(
            logger, NcFileDTO(s3_path=file, product=product, date_time=date_time)
        )
        # logger.info(f"Successfully added NC file '{file}' to the database.")
    except Exception as e:
        logger.error(f"Failed to process file '{file}': {str(e)}")


async def process_nc_files(logger, files):
    """
    Add all NC files to the NcFile table with batch processing.
    """
    for file in files:
        await add_nc_to_NcFile(logger, file)
    logger.info(f"Processed {len(files)} files")
