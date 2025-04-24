from dataclasses import dataclass
from typing import Optional

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.utils import file_naming

logger = root_cfg.setup_logger("sensor_core")

@dataclass
class Stream:
    """Defines the format and fields present in a datastream coming from a DPtreeNode."""
    # Used to identify the type & purpose of data in file names, etc.
    # In combination with the index, this will be unique to a given sensor.
    # In combination with the device_id & sensor_index this must be globally unique.
    type_id: str
    # Idenfier for the output stream.
    index: int
    # The type of data being produced by this output stream.
    format: api.FILE_FORMATS
    # The human-readable name of the output stream.
    fields: Optional[list[str]] = None
    # The cloud storage container to which the data is archived.
    # This is required for all types uploading files, other than output_format="CSV".
    # "CSV" data is uploaded to the DeviceCfg.cc_for_journals container.
    cloud_container: Optional[str] = None

    # Human-understandable description of the data in the stream
    description: Optional[str] = ""

    def get_data_id(self, sensor_index: int) -> str:
        """
        Returns the unique identifier for this node.  Used in filenaming and other data management.

        Returns:
            The unique identifier for this node.
        """
        return file_naming.create_data_id(root_cfg.my_device_id, sensor_index, self.type_id, self.index)

@dataclass
class DPtreeNodeCfg:
    """Defines the configuration for a node in the DPtree.
    SensorCfg & DataProcessorCfg inherit from this class.
    """
    outputs: list[Stream]
    
    # Human-meaningful description of the node.
    description: str

    # Some sources support saving of sample raw recordings to the archive.
    # This string is interpreted by the Sensor or DataProcessor to determine the frequency of 
    # raw data sampling. The format of this string is specific to the Sensor or DataProcessor.
    # The default implementation interprets this string as a float sampling probability (0.0-1.0)
    sample_probability: Optional[str] = None
    # If sampling is enabled, a sample_container must be specified and exist in the cloud storage.
    sample_container: Optional[str] = None


@dataclass
class SensorCfg(DPtreeNodeCfg):
    """Defines the configuration for a concrete Sensor class implementation.
    Can be subclassed to add additional configuration parameters specific to the Sensor class.

    Parameters:
    ----------
    sensor_type: str
        One of the sensor types defined in api.SENSOR_TYPES.

    sensor_index: int
        The index of the sensor in the list of sensors.
        Must be unique in combination with the sensor_type.
        Used, for example, where a device has 4 audio sensors.

    sensor_class_ref: str
        The fully qualified class name of the sensor.
        This must be interpretable as a Class by the Python interpreter.

    sensor_model_description: str
        A human-readable description of the sensor model.
    """
    sensor_index: int = 0
    sensor_type: api.SENSOR_TYPES = 'NOT_SET'
    sensor_model: str = root_cfg.FAILED_TO_LOAD


@dataclass
class DataProcessorCfg(DPtreeNodeCfg):
    """Defines the configuration for a concrete DataProcessor class implementation.
    Can be subclassed to add additional configuration parameters specific to the DataProcessor class."""
    