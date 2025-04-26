# filepath: sensor_core/__init__.py

# Re-export specific classes and functions
# Dynamically fetch the version from the package metadata
import importlib.metadata

from . import api, configuration
from .data_processor import DataProcessor
from .device_config_object_defs import (
    DeviceCfg,
)
from .dp_config_object_defs import (
    DataProcessorCfg,
    SensorCfg,
    Stream,
)
from .sensor import Sensor
from .sensor_core import SensorCore

try:
    __version__ = importlib.metadata.version("sensor-core")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"

# Optionally, define an explicit __all__ to control what gets imported with "from sensor_core import *"
__all__ = [
    "configuration",
    "DataProcessor",
    "DataProcessorCfg",
    "DeviceCfg",
    "Sensor",
    "SensorCfg",
    "SensorCore",
    "Stream",
    "api",
]