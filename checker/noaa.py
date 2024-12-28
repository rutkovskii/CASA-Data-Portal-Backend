# Python standard library imports
import asyncio
import csv
import datetime as dt
import gzip
import os
from pprint import pprint
from typing import Dict, List, Tuple
import traceback

# Third-party imports
import requests
from bs4 import BeautifulSoup
import aiohttp
import aiofiles

# Local imports
from db_tools import (
    get_noaa_records,
    post_noaa_record,
    post_noaa_events,
    NoaaEventDTO,
    NoaaRecordDTO,
)
from events_magnitudes import (
    countieslist,
    storm_types_sorted,
)
from noaa_tools import extract_event_from_row
from database.models import EventStatus
from shared.logger_config import setup_logger

# Initialize logger with service and component names
logger = setup_logger("checker", "noaa")

# Years for which we want data (2016 through current year)
years = [str(i) for i in range(2016, dt.date.today().year + 1)]

# Flag to use local files instead of fetching from NOAA
USE_LOCAL_FILES = False

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


def parse_noaa_csv(content: str, record_id: int) -> List[NoaaEventDTO]:
    """Parse NOAA CSV content and return list of events

    Args:
        content: CSV file content as string
        record_id: ID of parent NoaaRecord

    Returns:
        List[NoaaEventDTO]: List of parsed events
    """
    events = []
    reader = csv.DictReader(content.splitlines())

    for row in reader:
        if (
            row["STATE"] == "TEXAS"
            and row["CZ_NAME"] in countieslist
            and row["EVENT_TYPE"] in storm_types_sorted
        ):
            event = extract_event_from_row(row, record_id)
            events.append(event)

    return events


async def process_noaa_file(file_name: str, record_id: int) -> List[NoaaEventDTO]:
    """Process a single NOAA file and return list of event data"""
    logger.info(f"Processing NOAA file: {file_name}")

    if USE_LOCAL_FILES:
        file_path = os.path.join("static", file_name)
        if file_name.endswith(".csv.gz"):
            async with aiofiles.open(file_path, "rb") as f:
                content = await f.read()
                content = gzip.decompress(content).decode()
        else:
            async with aiofiles.open(file_path, "r") as f:
                content = await f.read()
    else:
        url = f"https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/{file_name}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                content = await response.read()
                content = gzip.decompress(content).decode()

    return parse_noaa_csv(content, record_id)


async def get_noaa_links(url: str) -> List[str]:
    """Get list of NOAA file links from URL"""
    logger.info(f"Fetching NOAA links from {url}")
    try:
        if USE_LOCAL_FILES:
            links = [f for f in os.listdir("static") if f.endswith((".csv", ".csv.gz"))]
            logger.info(f"Successfully retrieved {len(links)} local files")
            return links
        else:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=1) as response:
                    content = await response.text()
                    soup = BeautifulSoup(content, features="html.parser")
                    links = [link.get("href") for link in soup.find_all("a")]
                    logger.info(f"Successfully retrieved {len(links)} links")
                    return links
    except asyncio.TimeoutError:
        logger.error(f"Timeout error for {url}")
        return []
    except Exception as e:
        logger.error(f"Error getting NOAA links: {str(e)}")
        return []


def filter_year_files(links: List[str]) -> Dict[str, str]:
    """Filter links to only include relevant year files and return dict of year:filename"""
    logger.info("Filtering files by year")
    filtered = {}
    if USE_LOCAL_FILES:
        for link in links:
            if link and (link.endswith(".csv") or link.endswith(".csv.gz")):
                for year in years:
                    if f"{year}_" in link:
                        filtered[year] = link
                        break
    else:
        for link in links:
            if link and link.endswith(".csv.gz"):
                for year in years:
                    if f"StormEvents_details-ftp_v1.0_d{year}" in link:
                        filtered[year] = link
                        break
    return filtered


def extract_file_dates(year_files: Dict[str, str]) -> Dict[str, dt.date]:
    """Extract year and modified date from filenames"""
    logger.info("Extracting dates from filenames")
    file_dates = {}
    for year, file in year_files.items():
        parts = file.split("_")
        date_str = parts[4].split(".")[0][1:]  # Extract date after 'c'
        file_dates[year] = dt.datetime.strptime(date_str, "%Y%m%d").date()
    return file_dates


async def process_new_files(file_dates: Dict[str, dt.date], year_files: Dict[str, str]):
    """Process and insert new files when database is empty"""
    logger.info("Starting to process new files")
    for year, modified_date in file_dates.items():
        logger.info(f"Processing file for year {year}")
        noaa_record = NoaaRecordDTO(file_year=int(year), last_modified=modified_date)
        db_record = await post_noaa_record(noaa_record)
        logger.info(f"Added NOAA record for year {year} with ID {db_record.id}")

        events = await process_noaa_file(year_files[year], db_record.id)
        logger.info(f"Processed {len(events)} events for year {year}")

        await post_noaa_events(events)
        logger.info(
            f"Successfully added {len(events)} events to database for year {year}"
        )
    logger.info("Completed processing new files")


async def check_noaa():
    """Check for new or modified NOAA files and process their events"""
    logger.info("Starting NOAA file check")
    url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"

    try:
        links = await get_noaa_links(url)
        logger.debug(f"Found {len(links)} links")
        year_files = filter_year_files(links)
        file_dates = extract_file_dates(year_files)
        logger.info(f"File dates: {file_dates}")

        # Get existing records from database
        logger.info("Fetching existing records from database")
        db_files = await get_noaa_records()
        logger.info(f"Found {len(db_files)} existing records in database")

        # If database is empty, insert all files
        if not db_files:
            logger.info("No existing records found - processing all files")
            await process_new_files(file_dates, year_files)

            # TODO: Trigger mapper.py (it can just extracted unmapped events/modified events)

        # Handle modified files
        elif any(
            str(record.file_year) in file_dates
            and file_dates[str(record.file_year)] != record.last_modified
            for record in db_files
        ):
            logger.info("Found modified files - processing updates")
            # TODO: Handle modified files
            # 1. Identify which files have different last_modified dates
            # 2. For each modified file:
            #    a. Update the NOAA record with new last_modified date
            #    b. Process the new file's events
            #    c. Compare with existing events in database
            #    d. For events that differ:
            #       - Update event data
            #       - Set status to EventStatus.MODIFIED
            #    e. For new events:
            #       - Add with status EventStatus.UNMAPPED
            #    f. Keep unchanged events as is
            # 3. Prepare dictionary of modified events for mapper:
            #    {event_id: [product, date_time_start, date_time_end], ...}
            # 4. Trigger mapper.py with this dictionary
            pass

        else:
            logger.info("No new or modified files found")
            # No differences found between database records and parsed files
            # Continue without any changes
            pass

    except Exception as e:
        logger.error(f"Error processing NOAA files: {str(e)}")
        logger.error(traceback.format_exc())
        return []


if __name__ == "__main__":
    asyncio.run(check_noaa())
