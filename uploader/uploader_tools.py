import os
from datetime import timedelta
import shutil

list_of_radars = [
    "XMDL",
    "XJCO",
    "XMSQ",
    "XFTW",
    "XUNT",
]


def calculate_days_in_range(begin_date, end_date):
    """
    Calculates the number of days in the range of begindatetime and enddatetime
    """
    # Initialize list of days to check radar data for
    days_to_check = []
    # If begin and end date are not the same, there are multiple days in the date range
    if begin_date != end_date:
        # Calculate all days within date range
        datediff = end_date - begin_date
        # Add all days to list of days to check
        for i in range(datediff.days + 1):
            days_to_check.append(begin_date + timedelta(i))
        print(days_to_check)

    else:  # If begin and end date are the same
        days_to_check.append(begin_date)
        print(days_to_check)

    return days_to_check


def get_product_base_path(product):
    """
    Returns the base path for a given product type on MGH5

    Args:
        product (str): Product type ('hail', 'rainfall', or 'singleradar')

    Returns:
        str: Base path for the product
    """
    paths = {
        "hail": "/mnt/data/hydro",
        "rainfall": "/mnt/data/qpe",
        "singleradar": "/mnt/data/moments",
    }
    return paths.get(product)


def create_temp_dir():
    """
    Creates a temporary directory for file transfers

    Returns:
        str: Path to temporary directory
    """
    temp_dir = "/tmp/mgh5_transfer"
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir


def cleanup_temp_file(filepath):
    """
    Safely removes a temporary file

    Args:
        filepath (str): Path to file to remove
    """
    try:
        if os.path.exists(filepath):
            os.remove(filepath)
    except OSError as e:
        print(f"Error removing temporary file {filepath}: {e}")


def cleanup_temp_dir(dirpath):
    """
    Safely removes a temporary directory and all its contents

    Args:
        dirpath (str): Path to directory to remove
    """
    try:
        if os.path.exists(dirpath):
            shutil.rmtree(dirpath)  # This removes directory and all its contents
    except OSError as e:
        print(f"Error removing temporary directory {dirpath}: {e}")


def is_valid_product(product):
    """
    Checks if a product type is valid

    Args:
        product (str): Product type to check

    Returns:
        bool: True if product is valid, False otherwise
    """
    valid_products = ["hail", "rainfall", "singleradar"]
    return product in valid_products
