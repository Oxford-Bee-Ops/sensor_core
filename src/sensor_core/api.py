####################################################################################################
# Bee Ops API
#
# File define constants used on interfaces between components in the Bee Ops system.
####################################################################################################
from datetime import datetime
from enum import Enum
from typing import Final, Literal, Optional
from zoneinfo import ZoneInfo


############################################################
# Data record ID fields
############################################################
class RECORD_ID(Enum):
    VERSION = "version_id"
    DATA_TYPE_ID = "data_type_id"
    DEVICE_ID = "device_id"
    SENSOR_INDEX = "sensor_index"
    TIMESTAMP = "logged_time"
    END_TIME = "end_time"
    OFFSET = "primary_offset_index"
    SECONDARY_OFFSET = "secondary_offset_index"
    SUFFIX = "file_suffix"
    INCREMENT = "increment"
    NAME = "device_name"  # Not used programmatically, but helpful for users


REQD_RECORD_ID_FIELDS = [
    RECORD_ID.VERSION.value,
    RECORD_ID.DATA_TYPE_ID.value,
    RECORD_ID.DEVICE_ID.value,
    RECORD_ID.SENSOR_INDEX.value,
    RECORD_ID.TIMESTAMP.value,
]
ALL_RECORD_ID_FIELDS = [*REQD_RECORD_ID_FIELDS, 
                        RECORD_ID.END_TIME.value, 
                        RECORD_ID.OFFSET.value, 
                        RECORD_ID.SECONDARY_OFFSET.value, 
                        RECORD_ID.SUFFIX.value,
                        RECORD_ID.INCREMENT.value,
                        RECORD_ID.NAME.value]


############################################################
# Installation types
#
# Used in DUA & BCLI
############################################################
INSTALL_TYPE_SENSOR = "rpi_sensor"
INSTALL_TYPE_ETL = "etl"
INSTALL_TYPES = [INSTALL_TYPE_SENSOR, INSTALL_TYPE_ETL]

############################################################
# Sensor types
############################################################
SENSOR_TYPES = Literal['ENV', 'MIC', 'CAMERA', 'SYS']

############################################################
# Datastream types
############################################################
FILE_FORMATS = Literal[
    "df", # Dataframe; can be saved as CSV
    "log", # Jog (dict)
    "jpg", 
    "png", 
    "mp4", 
    "h264", 
    "wav", 
    "yaml"]

############################################################
# Tags used in logs sent from sensors to the ETL
############################################################
RAISE_WARN_TAG = "RAISE_WARNING#V1"
TELEM_TAG = "TELEM#V1: "

############################################################
# Datastream and DataProcessor configuration
############################################################
ON: Final[str] = "ON"
OFF: Final[str] = "OFF"
OPTIMISED: Final[str] = "OPTIMISED"

############################################################
# Datastream & Sensor status updates
############################################################
DS_STARTED = "STARTED"
DS_STOPPED = "STOPPED"

############################################################
# Datetime formats used in the system
#
# All times are in UTC.
#
# The format used for timestamps in the system is "%Y%m%dT%H%M%S%3f"
# (but the %3f directive is not supported by datetime.strptime).
# Nonetheless we only want milliseconds not microseconds in the filenames.
############################################################
STRFTIME = "%Y%m%dT%H%M%S%f"
PADDED_TIME_LEN = len("20210101T010101000000")

def utc_now() -> datetime:
    """Return the current time in UTC."""
    return datetime.now(ZoneInfo("UTC"))


def utc_to_iso_str(t: Optional[datetime | float] = None) -> str:
    """Return the current time in UTC as a formatted string."""
    if t is None:
        t = utc_now()
    elif isinstance(t, float):
        t = datetime.fromtimestamp(t, tz=ZoneInfo("UTC"))
    return t.isoformat(timespec="milliseconds")


def utc_to_fname_str(t: Optional[datetime | float] = None) -> str:
    """Return the current time in UTC as a string formatted for use in filenames."""
    if t is None:
        t = utc_now()
    elif isinstance(t, float):
        t = datetime.fromtimestamp(t, tz=ZoneInfo("UTC"))
    timestamp = t.strftime(STRFTIME)
    return timestamp[:-3]


def utc_from_str(t: str) -> datetime:
    """Convert a string timestamp formatted according to a datetime object."""
    # strptime doesn't support just milliseconds, so pad the string with 3 zeros
    t += "0" * (PADDED_TIME_LEN - len(t))

    naive_dt = datetime.strptime(t, STRFTIME)
    # Convert to UTC timezone
    utc_dt = naive_dt.replace(tzinfo=ZoneInfo("UTC"))
    return utc_dt


def str_to_iso(t: str) -> str:
    """Convert a string timestamp to an ISO 8601 formatted string."""
    dt = utc_from_str(t)
    return dt.isoformat(timespec="milliseconds")

