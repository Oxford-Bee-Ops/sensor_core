from dataclasses import dataclass, field
from typing import Optional

from sensor_core import api
from sensor_core.config_objects import DataProcessorCfg, Datastream, SensorCfg

#############################################################################################################
# Define the DatastreamType IDs
#############################################################################################################
EXAMPLE_DF_DS_TYPE_ID = "DUMMD"
EXAMPLE_LOG_DS_TYPE_ID = "DUMML"
EXAMPLE_FILE_DS_TYPE_ID = "DUMMF"


#############################################################################################################
# Define the SensorCfg object for the ExampleSensor
#
# We've added a_custom_field to demonstrate passing custom configuration to a concrete subclass of Sensor.
#############################################################################################################
@dataclass
class ExampleSensorCfg(SensorCfg):
    # The type of sensor.
    sensor_type: api.SENSOR_TYPES = "SYS"
    # Sensor index
    sensor_index: int = 1
    # The fully qualified class name of the sensor.
    # This must be interpretable as a Class by the Python
    sensor_class_ref: str = "example.my_sensor_example.ExampleSensor"
    # A human-readable description of the sensor model.
    sensor_model_description: str = "Dummy sensor for testing purposes"
    # An example of a custom field used to pass configuration to the ExampleSensor class.
    a_custom_field: str = "default_value"



#############################################################################################################
# Define the DERIVED DatastreamCfg objects
#############################################################################################################
@dataclass
class ExampleDfDsCfg(Datastream):
    type_id: str = EXAMPLE_DF_DS_TYPE_ID
    input_format: api.FILE_FORMATS = "csv"
    input_fields: list[str] = field(
        default_factory=lambda: ["pixel_count_transformed"])
    output_format: api.FILE_FORMATS = "csv"
    output_fields: list[str] = field(
        default_factory=lambda: ["pixel_count_transformed"])
    description: str = "Example df datastream for testing. "


EXAMPLE_DF_DATASTREAM_TYPE = ExampleDfDsCfg()

#############################################################################################################
# Define the DataProcessorCfg objects for the ExampleSensor
#############################################################################################################
@dataclass
class ExampleFileProcessorCfg(DataProcessorCfg):
    dp_class_ref: str = "example.my_processor_example.ExampleProcessor"
    dp_description: str = "Dummy file processor for testing"
    input_format: api.FILE_FORMATS = "jpg"
    output_format: api.FILE_FORMATS = "df"
    output_fields: Optional[list[str]] = field(
        default_factory=lambda: ["pixel_count"]
    )
    derived_datastreams: Optional[list[Datastream]] = field(
        default_factory=lambda: [ExampleDfDsCfg()]) #type: ignore


EXAMPLE_FILE_PROCESSOR = ExampleFileProcessorCfg()

#############################################################################################################
# Define the PRIMARY DatastreamCfg objects
#############################################################################################################

@dataclass
class ExampleFileDsCfg(Datastream):
    type_id: str = EXAMPLE_FILE_DS_TYPE_ID
    input_format: api.FILE_FORMATS = "jpg"
    input_fields: list[str] = field(default_factory=lambda: ["pixel_count"])
    output_format: api.FILE_FORMATS = "csv"
    output_fields: list[str] = field(default_factory=lambda: ["pixel_count"])
    description: str = "Example file datastream for testing. "
    sample_probability: str = str(0.1)
    sample_container: str = "sensor-core-upload"
    edge_processors: list[DataProcessorCfg] = field(
        default_factory=lambda: [EXAMPLE_FILE_PROCESSOR])


EXAMPLE_FILE_DS_TYPE = ExampleFileDsCfg()


@dataclass
class ExampleLogDsCfg(Datastream):
    type_id: str = EXAMPLE_LOG_DS_TYPE_ID
    input_format: api.FILE_FORMATS = "log"
    input_fields: list[str] = field(default_factory=lambda: ["temperature"])
    output_format: api.FILE_FORMATS = "csv"
    output_fields: list[str] = field(default_factory=lambda: ["temperature"])
    description: str = "Example log datastream for testing. "
    # No edge processors for this datastream type


EXAMPLE_LOG_DS_TYPE = ExampleLogDsCfg()
