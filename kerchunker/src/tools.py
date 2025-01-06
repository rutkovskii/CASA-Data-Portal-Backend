import s3fs
from src.config.config import Config


def create_s3fs_client():
    return s3fs.S3FileSystem(
        endpoint_url=Config.S3_ENDPOINT_URL,
        key=Config.S3_ACCESS_KEY,
        secret=Config.S3_SECRET_KEY,
        default_fill_cache=True,
        default_cache_type="first",
    )


def list_files(logger, bucket_name, pattern):
    """List all files in the bucket that match the pattern"""
    s3fs_client = create_s3fs_client()
    files = s3fs_client.glob(f"{bucket_name}/{pattern}")
    # remove the bucket name from the filepath
    files = [file.replace(f"{bucket_name}/", "") for file in files]
    logger.info(
        f"Listed {len(files)} files in bucket '{bucket_name}' with pattern '{pattern}'"
    )
    return files
