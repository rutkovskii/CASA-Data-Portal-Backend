from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import (
    String,
    Integer,
    Float,
    DateTime,
    Date,
    ForeignKey,
    Enum,
)
import enum
from datetime import date, datetime
from typing import Optional


# Base class for SQLAlchemy 2.0
class Base(DeclarativeBase):
    pass


class EventStatus(enum.Enum):
    UNMAPPED = "unmapped"
    MAPPED = "mapped"
    MODIFIED = "modified"


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
        product (str): Type of weather event (e.g., "Tornado", "Flash Flood")
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
    event_id: Mapped[str] = mapped_column(String, nullable=False)
    product: Mapped[str] = mapped_column(String)
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

    # Relationship to parent record
    noaa_record: Mapped[NoaaRecord] = relationship(back_populates="events")

    # Add these relationships
    nc_files: Mapped[list["NcFile"]] = relationship(back_populates="event")
    ref_files: Mapped[list["IndividualRefFile"]] = relationship(back_populates="event")
    combined_refs: Mapped[list["CombinedRefFile"]] = relationship(
        back_populates="event"
    )

    def __repr__(self):
        return (
            f"NoaaEvent(id={self.id}, "
            f"event_id={self.event_id}, "
            f"product={self.product}, "
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
            f"combined_refs={self.combined_refs}"
        )


class LastUploadedDate(Base):
    __tablename__ = "last_uploaded_date"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    product: Mapped[str] = mapped_column(String, unique=True)
    date: Mapped[date] = mapped_column(Date)

    def __repr__(self):
        return (
            f"LastUploadedDate(id={self.id}, product={self.product}, date={self.date})"
        )


class FailedUpload(Base):
    __tablename__ = "failed_uploads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    remote_path: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    product: Mapped[str] = mapped_column(String, nullable=False)
    date_dir: Mapped[str] = mapped_column(String, nullable=False)
    last_error: Mapped[str] = mapped_column(String, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime, default=datetime.utcnow)
    last_attempt: Mapped[DateTime] = mapped_column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"FailedUpload(remote_path={self.remote_path})"


class NcFile(Base):
    __tablename__ = "nc_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    s3_path: Mapped[str] = mapped_column(String, nullable=False)
    date_time: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    product: Mapped[str] = mapped_column(String, nullable=False)

    # Make sure event_id is nullable in the database
    event_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("noaa_events.id"), nullable=True
    )
    reference_file_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("individual_reference_files.id"), nullable=True
    )

    # Relationships
    event: Mapped[Optional["NoaaEvent"]] = relationship(back_populates="nc_files")
    ref_file: Mapped[Optional["IndividualRefFile"]] = relationship(
        back_populates="nc_files"
    )
    combined_refs: Mapped[Optional[list["CombinedRefFile"]]] = relationship(
        secondary="combined_ref_2_nc_files", back_populates="nc_files"
    )


class IndividualRefFile(Base):
    __tablename__ = "individual_reference_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    s3_path: Mapped[str] = mapped_column(String, nullable=False)
    date_time: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    product: Mapped[str] = mapped_column(String, nullable=False)

    # Foreign keys
    event_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("noaa_events.id"), nullable=True
    )

    # Relationships
    event: Mapped[Optional["NoaaEvent"]] = relationship(back_populates="ref_files")
    nc_files: Mapped[list["NcFile"]] = relationship(back_populates="ref_file")
    combined_refs: Mapped[Optional[list["CombinedRefFile"]]] = relationship(
        secondary="combined_ref_2_individual_refs", back_populates="ref_files"
    )


class CombinedRefFile(Base):
    __tablename__ = "combined_reference_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    s3_path: Mapped[str] = mapped_column(String, nullable=False)
    date_time: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    product: Mapped[str] = mapped_column(String, nullable=False)

    # Foreign keys
    event_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("noaa_events.id"), nullable=True
    )

    # Relationships
    event: Mapped[Optional["NoaaEvent"]] = relationship(back_populates="combined_refs")
    nc_files: Mapped[list["NcFile"]] = relationship(
        secondary="combined_ref_2_nc_files", back_populates="combined_refs"
    )
    ref_files: Mapped[list["IndividualRefFile"]] = relationship(
        secondary="combined_ref_2_individual_refs", back_populates="combined_refs"
    )


# Association tables for many-to-many relationships
class CombinedRefNcFiles(Base):
    __tablename__ = "combined_ref_2_nc_files"

    combined_ref_id: Mapped[int] = mapped_column(
        ForeignKey("combined_reference_files.id"), primary_key=True
    )
    nc_file_id: Mapped[int] = mapped_column(ForeignKey("nc_files.id"), primary_key=True)


class CombinedRefIndividualFiles(Base):
    __tablename__ = "combined_ref_2_individual_refs"

    combined_ref_id: Mapped[int] = mapped_column(
        ForeignKey("combined_reference_files.id"), primary_key=True
    )
    reference_file_id: Mapped[int] = mapped_column(
        ForeignKey("individual_reference_files.id"), primary_key=True
    )
