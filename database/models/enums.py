import enum


class EventStatus(enum.Enum):
    UNMAPPED = "unmapped"
    MAPPED = "mapped"
    MODIFIED = "modified"
