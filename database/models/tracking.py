from sqlalchemy import Column, Integer, String, Date

from .base import Base


class LastUploadedDate(Base):
    __tablename__ = "last_uploaded_date"

    id = Column(Integer, primary_key=True)
    product = Column(String, nullable=False)
    date = Column(Date, nullable=False)


class FailedUpload(Base):
    __tablename__ = "failed_uploads"

    id = Column(Integer, primary_key=True)
    remote_path = Column(String, unique=True, nullable=False)
    product = Column(String, nullable=False)
    date_dir = Column(String, nullable=False)
    last_error = Column(String)
    last_attempt = Column(Date)
