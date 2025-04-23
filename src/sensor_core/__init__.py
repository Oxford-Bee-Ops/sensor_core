# filepath: sensor_core/__init__.py

# Re-export specific classes and functions
# Dynamically fetch the version from the package metadata
import importlib.metadata

from . import api, configuration
from .config_objects import (
    DeviceCfg,
)
from .data_processor import DataProcessor
from .sensor import Sensor
from .sensor_core import SensorCore
from .dp_tree_node_types import (
    SensorCfg,
    DataProcessorCfg,
) 

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
    "api",
    "configuration"
]