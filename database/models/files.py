from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import String, Integer, DateTime, ForeignKey

from .base import Base
from .noaa import NoaaEvent

class NcFile(Base):
    __tablename__ = "nc_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    s3_path: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    date_time: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    product: Mapped[str] = mapped_column(String, nullable=False)

    # Foreign keys
    event_id: Mapped[Optional[int]] = mapped_column(ForeignKey("noaa_events.id"), nullable=True)
    ref_file_id: Mapped[Optional[int]] = mapped_column(ForeignKey("individual_reference_files.id"), nullable=True, unique=True)
    event_ref_file_id: Mapped[Optional[int]] = mapped_column(ForeignKey("event_reference_files.id"), nullable=True)

    # Relationships
    event: Mapped[Optional["NoaaEvent"]] = relationship(back_populates="nc_files")
    ref_file: Mapped[Optional["IndividualRefFile"]] = relationship(back_populates="nc_file", uselist=False)
    event_ref_file: Mapped[Optional["EventRefFile"]] = relationship(
        "EventRefFile",
        back_populates="nc_files",
    )


class IndividualRefFile(Base):
    __tablename__ = "individual_reference_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    s3_path: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    date_time: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    product: Mapped[str] = mapped_column(String, nullable=False)

    # Foreign keys
    event_id: Mapped[Optional[int]] = mapped_column(ForeignKey("noaa_events.id"), nullable=True)
    event_ref_file_id: Mapped[Optional[int]] = mapped_column(ForeignKey("event_reference_files.id"), nullable=True)

    # Relationships
    event: Mapped[Optional["NoaaEvent"]] = relationship(back_populates="ref_files")
    nc_file: Mapped[Optional["NcFile"]] = relationship(back_populates="ref_file", uselist=False)
    event_ref_file: Mapped[Optional["EventRefFile"]] = relationship(
        "EventRefFile",
        back_populates="individual_ref_files",
        uselist=False
    )


class EventRefFile(Base):
    __tablename__ = "event_reference_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    s3_path: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    product: Mapped[str] = mapped_column(String, nullable=False)

    # Foreign keys
    event_id: Mapped[int] = mapped_column(ForeignKey("noaa_events.id"), unique=True)

    # Relationships
    event: Mapped[Optional["NoaaEvent"]] = relationship(
        "NoaaEvent",
        back_populates="event_ref_file",
        uselist=False
    )
    nc_files: Mapped[list["NcFile"]] = relationship(
        "NcFile",
        back_populates="event_ref_file",
    )
    individual_ref_files: Mapped[list["IndividualRefFile"]] = relationship(
        "IndividualRefFile",
        back_populates="event_ref_file",
    )


class DatasetRefFile(Base):
    __tablename__ = "dataset_reference_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    s3_path: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    product: Mapped[str] = mapped_column(String, nullable=False)

    # Foreign keys
    # TODO:Must relate to a query, therefore need to create query table and one to one relationship
