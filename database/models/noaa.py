from datetime import date, datetime
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import String, Integer, Float, DateTime, Date, ForeignKey, Enum

from .base import Base
from .enums import EventStatus

class NoaaRecord(Base):
    __tablename__ = "noaa_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    file_year: Mapped[int] = mapped_column(Integer, nullable=False)
    last_modified: Mapped[date] = mapped_column(Date)
    events: Mapped[list["NoaaEvent"]] = relationship(back_populates="noaa_record")

    def __repr__(self):
        return (
            f"NoaaFile(id={self.id}, "
            f"file_year={self.file_year}, "
            f"last_modified={self.last_modified})"
        )


class NoaaEvent(Base):
    """Represents a NOAA weather event record

    Attributes:
        id (int): Primary key
        noaa_record_id (int): Foreign key to parent NoaaRecord
        event_id (str): NOAA's unique event identifier
        noaa_product (str): Type of weather event (e.g., "Tornado", "Flash Flood")
        date_time_start (DateTime): Event start time
        date_time_end (DateTime): Event end time
        status (EventStatus): Processing status of the event

        # Location fields
        begin_lat (float): Starting latitude of event
        begin_lon (float): Starting longitude of event
        end_lat (float): Ending latitude of event
        end_lon (float): Ending longitude of event
        county (str): County where event occurred
        begin_city (str): City/location where event began
        end_city (str): City/location where event ended

        # Impact fields
        magnitude (str): Event magnitude (e.g., EF scale for tornadoes, hail size)
        damage_property (int): Property damage in dollars
        damage_crops (int): Crop damage in dollars
        deaths_direct (int): Direct deaths caused by event
        deaths_indirect (int): Indirect deaths related to event
        injuries_direct (int): Direct injuries caused by event
        injuries_indirect (int): Indirect injuries related to event

        # Description fields
        event_narrative (str): Detailed description of the specific event
        episode_narrative (str): Description of the overall weather episode
    """

    __tablename__ = "noaa_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    noaa_record_id: Mapped[int] = mapped_column(ForeignKey("noaa_records.id"))

    # Core fields
    event_id: Mapped[int] = mapped_column(Integer, nullable=False)
    noaa_product: Mapped[str] = mapped_column(String)
    date_time_start: Mapped[DateTime] = mapped_column(DateTime)
    date_time_end: Mapped[DateTime] = mapped_column(DateTime)
    status: Mapped[EventStatus] = mapped_column(Enum(EventStatus))

    # Location fields
    begin_lat: Mapped[Optional[Float]] = mapped_column(Float, nullable=True)
    begin_lon: Mapped[Optional[Float]] = mapped_column(Float, nullable=True)
    end_lat: Mapped[Optional[Float]] = mapped_column(Float, nullable=True)
    end_lon: Mapped[Optional[Float]] = mapped_column(Float, nullable=True)
    county: Mapped[str] = mapped_column(String)
    begin_city: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    end_city: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Impact fields
    magnitude: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    damage_property: Mapped[int] = mapped_column(Integer, default=0)
    damage_crops: Mapped[int] = mapped_column(Integer, default=0)
    deaths_direct: Mapped[int] = mapped_column(Integer, default=0)
    deaths_indirect: Mapped[int] = mapped_column(Integer, default=0)
    injuries_direct: Mapped[int] = mapped_column(Integer, default=0)
    injuries_indirect: Mapped[int] = mapped_column(Integer, default=0)

    # Description fields
    event_narrative: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    episode_narrative: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Foreign keys
    # event_ref_file_id: Mapped[Optional[int]] = mapped_column(
    #     ForeignKey("event_reference_files.id"),
    #     nullable=True
    # )

    # Relationships
    noaa_record: Mapped[NoaaRecord] = relationship(back_populates="events")
    nc_files: Mapped[list["NcFile"]] = relationship(back_populates="event")
    ref_files: Mapped[list["IndividualRefFile"]] = relationship(back_populates="event")
    event_ref_file: Mapped[Optional["EventRefFile"]] = relationship(
        back_populates="event",
        uselist=False
    )

    def __repr__(self):
        return (
            f"NoaaEvent(id={self.id}, "
            f"event_id={self.event_id}, "
            f"noaa_product={self.noaa_product}, "
            f"status={self.status}),"
            f"begin_lat={self.begin_lat}, "
            f"begin_lon={self.begin_lon}, "
            f"end_lat={self.end_lat}, "
            f"end_lon={self.end_lon}, "
            f"county={self.county}, "
            f"begin_city={self.begin_city}, "
            f"end_city={self.end_city}"
            f"magnitude={self.magnitude}, "
            f"damage_property={self.damage_property}, "
            f"damage_crops={self.damage_crops}, "
            f"deaths_direct={self.deaths_direct}, "
            f"deaths_indirect={self.deaths_indirect}, "
            f"injuries_direct={self.injuries_direct}, "
            f"injuries_indirect={self.injuries_indirect}, "
            f"event_narrative={self.event_narrative}, "
            f"episode_narrative={self.episode_narrative}, "
            f"noaa_record_id={self.noaa_record_id}"
            f"noaa_record={self.noaa_record}"
            f"nc_files={self.nc_files}"
            f"ref_files={self.ref_files}"
            f"event_ref_file={self.event_ref_file}"
        )
