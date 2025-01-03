"""Helper functions for processing NOAA weather event data"""

import os
from datetime import datetime
from typing import Dict

from src.db_tools import NoaaEventDTO
from database.models import EventStatus


def process_damage(damage_str: str) -> int:
    """Convert NOAA damage string to integer value in dollars

    Args:
        damage_str: Damage string (e.g., "10.00K", "1.5M")

    Returns:
        int: Damage amount in dollars

    Examples:
        >>> process_damage("10.00K")
        10000
        >>> process_damage("1.5M")
        1500000
        >>> process_damage("")
        0
    """
    if not damage_str or damage_str == "0.00K":
        return 0

    try:
        number = float(damage_str[:-1])
        multiplier = {"K": 1000, "M": 1000000, "B": 1000000000}.get(damage_str[-1], 1)
        return int(number * multiplier)
    except (ValueError, IndexError):
        return 0


def parse_datetime(date_str: str) -> datetime:
    """Parse NOAA datetime string to Python datetime

    Args:
        date_str: NOAA format date string (e.g., "09-MAY-19 15:54:00")

    Returns:
        datetime: Parsed datetime object

    Examples:
        >>> parse_datetime("09-MAY-19 15:54:00")
        datetime(2019, 5, 9, 15, 54)
    """
    return datetime.strptime(date_str, "%d-%b-%y %H:%M:%S")


def extract_event_from_row(row: Dict, record_id: int) -> NoaaEventDTO:
    """Extract event data from CSV row and create DTO

    Args:
        row: Dictionary containing CSV row data
        record_id: ID of parent NoaaRecord

    Returns:
        NoaaEventDTO: Data transfer object with extracted event data
    """
    # Process damage amounts
    damage_property = process_damage(row["DAMAGE_PROPERTY"])
    damage_crops = process_damage(row["DAMAGE_CROPS"])

    # Process magnitude
    magnitude = row["MAGNITUDE"] if row["MAGNITUDE"] else row["TOR_F_SCALE"]

    return NoaaEventDTO(
        noaa_record_id=record_id,
        event_id=row["EVENT_ID"],
        product=row["EVENT_TYPE"],
        date_time_start=parse_datetime(row["BEGIN_DATE_TIME"]),
        date_time_end=parse_datetime(row["END_DATE_TIME"]),
        status=EventStatus.UNMAPPED,
        # Location fields
        begin_lat=float(row["BEGIN_LAT"]) if row["BEGIN_LAT"] else None,
        begin_lon=float(row["BEGIN_LON"]) if row["BEGIN_LON"] else None,
        end_lat=float(row["END_LAT"]) if row["END_LAT"] else None,
        end_lon=float(row["END_LON"]) if row["END_LON"] else None,
        county=row.get("CZ_NAME", "UNKNOWN").strip() or "UNKNOWN",
        begin_city=row["BEGIN_LOCATION"],
        end_city=row["END_LOCATION"],
        # Impact fields
        magnitude=str(magnitude) if magnitude else None,
        damage_property=damage_property,
        damage_crops=damage_crops,
        deaths_direct=int(row["DEATHS_DIRECT"]),
        deaths_indirect=int(row["DEATHS_INDIRECT"]),
        injuries_direct=int(row["INJURIES_DIRECT"]),
        injuries_indirect=int(row["INJURIES_INDIRECT"]),
        # Description fields
        event_narrative=row["EVENT_NARRATIVE"],
        episode_narrative=row["EPISODE_NARRATIVE"],
    )


# Example of received data from NOAA
links_example = [
    "StormEvents_details-ftp_v1.0_d2016_c20220719.csv.gz",
    "StormEvents_details-ftp_v1.0_d2017_c20230317.csv.gz",
    "StormEvents_details-ftp_v1.0_d2018_c20240716.csv.gz",
    "StormEvents_details-ftp_v1.0_d2019_c20240117.csv.gz",
    "StormEvents_details-ftp_v1.0_d2020_c20240620.csv.gz",
    "StormEvents_details-ftp_v1.0_d2021_c20240716.csv.gz",
    "StormEvents_details-ftp_v1.0_d2022_c20241121.csv.gz",
    "StormEvents_details-ftp_v1.0_d2023_c20241216.csv.gz",
    "StormEvents_details-ftp_v1.0_d2024_c20241216.csv.gz",
]

# iterate through static folder, and get all .csv files
links_example_2 = [file for file in os.listdir("static") if file.endswith(".csv")]
