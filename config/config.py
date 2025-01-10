import os


class Config:
    S3_ENDPOINT_URL = "https://mghp.osn.xsede.org"
    S3_ACCESS_KEY = "XWYY7XK2I46OGR9RJDUF"
    S3_SECRET_KEY = "dH/Y37rzH4x6AtXsE9EUxL24jwsc9U"
    S3_BUCKET_NAME = "opencloudtestbed-bucket01"

    MGH5_HOST = "mgh5.casa.umass.edu"
    MGH5_USER = "arutkovskii"
    MGH5_PASSWORD = "radar2$bling"

    MGH5_K_USER = "kmclaughlin"
    MGH5_K_PASSWORD = "Jt6u!m8X7"

    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

    REFERENCES_DIR = os.path.join(ROOT_DIR, "references")
    COMBINED_REFS_DIR = os.path.join(REFERENCES_DIR, "combined")
    IMAGES_DIR = os.path.join(ROOT_DIR, "images")

    S3_URL_TEMPLATE = os.path.join(S3_ENDPOINT_URL, S3_BUCKET_NAME)

    # Create references directory if it doesn't exist
    # os.makedirs(REFERENCES_DIR, exist_ok=True)

    # https://mghp.osn.xsede.org/opencloudtestbed-bucket01/rainfall/20180512/20180512_235922.nc
