from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import String, Integer, Float, DateTime, Date, Time, Boolean, ForeignKey, Enum
import enum
from datetime import date

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
    #TODO: Add all necessary fields from noaa_events table
    __tablename__ = "noaa_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    noaa_record_id: Mapped[int] = mapped_column(ForeignKey("noaa_records.id"))
    
    # Core fields (we can add more as needed)
    event_id: Mapped[str] = mapped_column(String, nullable=False)
    product: Mapped[str] = mapped_column(String)
    date_time_start: Mapped[DateTime] = mapped_column(DateTime)
    date_time_end: Mapped[DateTime] = mapped_column(DateTime)
    status: Mapped[EventStatus] = mapped_column(Enum(EventStatus))
    
    # Relationship to parent record
    noaa_record: Mapped[NoaaRecord] = relationship(back_populates="events")

    def __repr__(self):
        return (
            f"NoaaEvent(id={self.id}, "
            f"event_id={self.event_id}, "
            f"product={self.product}, "
            f"status={self.status})"
        )
