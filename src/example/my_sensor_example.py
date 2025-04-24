from time import sleep

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.dp_config_object_defs import SensorCfg, Stream
from sensor_core.sensor import Sensor
from sensor_core.utils import file_naming

logger = root_cfg.setup_logger("sensor_core")

EXAMPLE_LOG_DS_TYPE_ID = "DUMML"
EXAMPLE_FILE_DS_TYPE_ID = "DUMMF"

EXAMPLE_FILE_STREAM_INDEX = 0
EXAMPLE_LOG_STREAM_INDEX = 1

#############################################################################################################
# Define the SensorCfg object for the ExampleSensor
#
# We've added a_custom_field to demonstrate passing custom configuration to a concrete subclass of Sensor.
#############################################################################################################
EXAMPLE_SENSOR_CFG = SensorCfg(
    # The type of sensor.
    sensor_type = "ENV",
    # Sensor index
    sensor_index = 1,
    sensor_model="ExampleSensor",
    # A human-readable description of the sensor model.
    description = "Dummy sensor for testing purposes",
    # The list of data output streams from the sensor.
    outputs=[
        Stream(EXAMPLE_FILE_DS_TYPE_ID, EXAMPLE_FILE_STREAM_INDEX, "jpg", 
               cloud_container="sensor-core-upload"),
        Stream(EXAMPLE_LOG_DS_TYPE_ID, EXAMPLE_LOG_STREAM_INDEX, "log", ["temperature"]),
    ],
    sample_container="sensor-core-upload",
    sample_probability="1.0",
)

#############################################################################################################
# Define the ExampleSensor as a concrete implementation of the Sensor class
#
# A concrete Sensor class must implement the run() method.
#############################################################################################################
class ExampleSensor(Sensor):
    def __init__(self, config: SensorCfg) -> None:
        super().__init__(config)

    def run(self) -> None:
        """The run method is called when the Sensor is started."""

        # Main sensor loop
        # All sensor implementations must check for stop_requested to allow the sensor to be stopped cleanly
        while not self.stop_requested:
            self.log(stream_index=EXAMPLE_LOG_STREAM_INDEX,
                     sensor_data={"temperature": 25.0})
            fname = file_naming.get_temporary_filename("jpg")
            # Generate a random image file
            with open(fname, "w") as f:
                f.write("This is a dummy image file")
            self.save_recording(stream_index=EXAMPLE_FILE_STREAM_INDEX, 
                                temporary_file=fname, 
                                start_time=api.utc_now())

            # Sensors should not sleep for more than ~180s so that the stop_requested flag can be checked
            # and the sensor shut down cleanly in a reasonable time frame.
            if root_cfg.TEST_MODE == root_cfg.MODE.TEST:
                # In test mode, sleep for 0.1s to allow the test to run quickly
                sleep(0.1)
            else:
                sleep(10)
