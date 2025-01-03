import os
import aioboto3
from botocore import UNSIGNED
from botocore.config import Config as BotocoreConfig
from src.s3_tools.S3Tools import add_time_dimension
import aiofiles
import shutil


class S3Manager:
    """Manages S3 operations including file uploads, downloads, and bucket management."""

    def __init__(self, endpoint, access_key=None, secret_key=None, anonymous=False):
        self.endpoint = endpoint
        self.session = aioboto3.Session()
        self.access_key = access_key
        self.secret_key = secret_key
        self.anonymous = anonymous
        self.client = None

    async def initialize(self):
        """Initialize the S3 client asynchronously using async context manager"""
        if self.anonymous:
            self.client = await self.session.client(
                service_name="s3",
                endpoint_url=self.endpoint,
                config=BotocoreConfig(signature_version=UNSIGNED),
            ).__aenter__()
        else:
            self.client = await self.session.client(
                service_name="s3",
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=BotocoreConfig(max_pool_connections=10),
            ).__aenter__()

    async def close(self):
        """Close the S3 client properly"""
        if self.client:
            await self.client.__aexit__(None, None, None)

    async def create_bucket(self, bucket_name):
        await self.client.create_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} created")

    async def upload_dir(self, bucket_name, directory):
        for root, dirs, files in os.walk(directory):
            for file in files:
                await self.client.upload_file(
                    os.path.join(root, file), bucket_name, f"{root}/{file}"
                )
                print(f"File {file} uploaded to bucket {bucket_name}")

    async def upload_dir_time_dim(self, bucket_name, directory):
        # Create a temporary directory for processed files
        temp_dir = directory + "_temp"
        os.makedirs(temp_dir, exist_ok=True)

        try:
            for root, dirs, files in os.walk(directory):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    temp_file = add_time_dimension(local_file_path, output_dir=temp_dir)
                    s3_file_path = os.path.join(root, os.path.splitext(file)[0] + ".nc")

                    async with aiofiles.open(temp_file, "rb") as file_obj:
                        await self.client.upload_fileobj(
                            file_obj, bucket_name, s3_file_path
                        )
                    print(
                        f"File {file} uploaded to bucket {bucket_name} as {s3_file_path}"
                    )
        finally:
            # Clean up the entire temporary directory at once
            shutil.rmtree(temp_dir)

    # async def upload_dir_time_dim(self, bucket_name, directory):
    #     for root, dirs, files in os.walk(directory):
    #         for file in files:
    #             local_file_path = os.path.join(root, file)
    #             temp_file = add_time_dimension(local_file_path)
    #             s3_file_path = os.path.join(root, file)
    #             await self.client.upload_file(temp_file, bucket_name, s3_file_path)
    #             os.remove(temp_file)
    #             print(f"File {file} uploaded to bucket {bucket_name}")

    async def list_files(self, bucket_name, folder=None):
        paginator = self.client.get_paginator("list_objects_v2")
        if folder:
            async for page in paginator.paginate(Bucket=bucket_name, Prefix=folder):
                for obj in page.get("Contents", []):
                    print(obj.get("Key"))
        else:
            async for page in paginator.paginate(Bucket=bucket_name):
                for obj in page.get("Contents", []):
                    print(obj.get("Key"))

    async def download_file_async(self, bucket_name, file_name, local_folder=None):
        if local_folder:
            os.makedirs(local_folder, exist_ok=True)
        else:
            local_folder = ""

        async with aioboto3.client("s3", endpoint_url=self.endpoint) as s3:
            await s3.download_file(
                bucket_name, file_name, f"{local_folder}/{os.path.basename(file_name)}"
            )

    async def delete_folder(self, bucket_name, folder):
        response = await self.client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
        for obj in response.get("Contents", []):
            await self.client.delete_object(Bucket=bucket_name, Key=obj.get("Key"))
            print(f'File {obj.get("Key")} deleted')

        response = await self.client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
        if not response.get("Contents"):
            await self.client.delete_object(Bucket=bucket_name, Key=folder)
            print(f"Folder {folder} deleted")

    async def generate_presigned_url(self, bucket_name, object_name, expiration=3600):
        try:
            response = await self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": object_name},
                ExpiresIn=expiration,
            )
        except Exception as e:
            print(e)
            return None

        return response

    async def upload_file_async(self, file_path, bucket, key, transfer_config=None):
        """Upload a file to S3 asynchronously using streaming upload"""
        async with aiofiles.open(file_path, "rb") as file_obj:
            await self.client.upload_fileobj(
                file_obj, bucket, key, Config=transfer_config
            )

    async def list_dirs(self, bucket_name, folder=None):
        paginator = self.client.get_paginator("list_objects_v2")
        dirs = set()

        if folder:
            async for page in paginator.paginate(Bucket=bucket_name, Prefix=folder):
                for obj in page.get("Contents", []):
                    s3_dir = os.path.dirname(obj.get("Key"))
                    dirs.add(s3_dir)
        else:
            async for page in paginator.paginate(Bucket=bucket_name):
                for obj in page.get("Contents", []):
                    s3_dir = os.path.dirname(obj.get("Key"))
                    dirs.add(s3_dir)

        dirs = list(dirs)
        dirs.sort()
        print(dirs)

        return dirs


# Example usage:
# s3_manager = S3Manager(endpoint='https://example.com', access_key='your-access-key', secret_key='your-secret-key')
# s3_manager.create_bucket('your-bucket-name')
# s3_manager.upload_dir('your-bucket-name', 'your-directory')
# s3_manager.set_bucket_policy_public('your-bucket-name')
# s3_manager.list_files('your-bucket-name', 'your-folder')
# s3_manager.download_file('your-bucket-name', 'your-file-name', 'your-local-folder')
