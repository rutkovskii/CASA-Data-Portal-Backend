import os
import xarray as xr
from datetime import datetime
import gzip
import io


def parse_file_datetime(filename, product=None):
    """
    Extract datetime from filename based on product type.
    If product is None, assumes format 'YYYYMMDD_HHMMSS.nc' or 'YYYYMMDD_HHMMSS.nc.gz'

    Args:
        filename (str): Name of the file
        product (str, optional): Product type ('hail', 'rainfall', or 'singleradar')
    """
    try:
        if product == "hail":
            return datetime.strptime(filename, "COMPOSITE_%Y%m%d-%H%M%S.nc")
        elif product == "rainfall":
            return datetime.strptime(filename, "%Y%m%d_%H%M%S.nc.gz")
        elif product == "singleradar":
            filename_split = filename.split(".")
            return datetime.strptime(filename_split[1], "tx-%Y%m%d-%H%M%S")
        else:
            # Default format for generic files
            base_name = os.path.basename(filename)
            base_name = base_name.replace(".nc.gz", "").replace(".nc", "")
            date_str, time_str = base_name.split("_")
            dt_str = f"{date_str}{time_str}"
            return datetime.strptime(dt_str, "%Y%m%d%H%M%S")
    except ValueError:
        return None


def open_gzipped_netcdf(gz_path):
    with gzip.open(gz_path, "rb") as f:
        return xr.open_dataset(io.BytesIO(f.read()))


def unzip_inplace(gz_path):
    with gzip.open(gz_path, "rb") as f:
        unzipped_path = gz_path.replace(".gz", "")
        with open(unzipped_path, "wb") as f_out:
            f_out.write(f.read())
        os.remove(gz_path)
    return unzipped_path


def add_time_dimension(input_file):
    """
    Add a time dimension to a NetCDF file based on its filename.
    Handles both .nc and .nc.gz files.
    """
    # Handle gzipped files
    if input_file.endswith(".gz"):
        input_file = unzip_inplace(input_file)

    ds = xr.open_dataset(input_file)

    # Extract time from filename
    time_value = parse_file_datetime(input_file)

    # Create a time coordinate
    ds = ds.expand_dims(time=[time_value])

    # Convert ds to netcdf file
    ds.to_netcdf(input_file)

    return input_file
