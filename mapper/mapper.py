"""
Q: At what point should I create individual reference files?
A1: Standalone function that iterates over the S3 bucket and creates reference files for nc files.
A2: Integrate a functionality into dataset creation. When a request is made to create a dataset that covers this period,
generate reference files individually, and then combine them into a single dataset.

- A1 is better because it would require less logic to track files and reference files, as well as
  it would preprocess files in advance.

STEPS to map events:
1. Connect to the database
2. Get unmapped events
3. Based on the product, datetime start and end, get the corresponding filepaths for nc and reference files
4. Add mapping to the database
Should it be event to list of filepaths or event to many ids of reference files?

STEPS to modify events:
- Delete the ids of the reference files and nc_file from the noaa_events table corresponding to the event
- Delete event_reference_file
- Re-run the mapping process

Then Kerchunk would need to create event reference files.
"""

# So there are Single Radar product, and therefore for an event we may need to have one combined_ref_file
# for (eg) rainfall and another one for single radar

# TODO:
# 1. Verify if it makes sense to rework the logic of the mapper to be more like the kerchunker ind ref files
# 2. Remove start_date from map_events function
# 2. Single Radar/Radars in range  (maybe should not preprocess but to just generate on the go)
# 3. "MODIFIED" logic
# 4. Another question I have, what if there are 2 overallaping events, then the netcdf would have a conflict? ???????????

from datetime import timedelta, datetime

import asyncio

from src.db_tools import (
    get_events_to_map,
    get_matching_nc_files,
    get_current_event,
    session_scope,
)
from src.shared.noaa_product_to_product import event_to_product_map
from src.database.models import EventStatus

from src.shared.logger_config import setup_logger

logger = setup_logger("mapper", "mapper", append=False)


async def map_events(start_date: datetime):
    """
    Map events to the database
    """
    events_to_map = await get_events_to_map(start_date)

    for event in events_to_map:
        # Get the fields for the event
        product = event_to_product_map[event.noaa_product][0]
        date_time_start = event.date_time_start - timedelta(minutes=15)
        date_time_end = event.date_time_end + timedelta(minutes=15)

        matching_nc_files = await get_matching_nc_files(
            product, date_time_start, date_time_end
        )

        if len(matching_nc_files) == 0:
            logger.warning(
                f"No matching nc files found for event: {event.id}, {event.noaa_product}, {event.date_time_start}, {event.date_time_end}"
            )
            continue

        logger.info(f"Processing event_id: {event.event_id}")
        logger.info(f"Found {len(matching_nc_files)} matching NC files")

        async with session_scope() as s:
            try:
                # Get current event in this session
                current_event = await get_current_event(s, event.id)

                for nc_file in matching_nc_files:
                    logger.info(
                        f"Updating NC file {nc_file.id} with event_id {event.id}"
                    )
                    nc_file.event_id = event.id
                    s.add(nc_file)

                    logger.info(f"Updating ref file for NC file {nc_file.id}")
                    nc_file.ref_file.event_id = event.id
                    s.add(nc_file.ref_file)

                current_event.status = EventStatus.MAPPED
                s.add(current_event)

                await s.commit()
                logger.info(
                    f"Successfully processed all files for event_id {event.id}"
                )

            except Exception as e:
                logger.error(f"Error processing event_id {event.id}: {str(e)}")
                await s.rollback()
                raise


async def main():
    await map_events(start_date=datetime(2018, 1, 1))

if __name__ == "__main__":
    asyncio.run(main())
