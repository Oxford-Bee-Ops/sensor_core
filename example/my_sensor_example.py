from time import sleep
from typing import Optional

from example.my_config_object_defs import EXAMPLE_FILE_DS_TYPE, EXAMPLE_LOG_DS_TYPE
from sensor_core import api
from sensor_core.config_objects import SensorDsCfg
from sensor_core.datastream import Datastream
from sensor_core.sensor import Sensor
from sensor_core.utils import file_naming, utils

logger = utils.setup_logger("sensor_core")


#############################################################################################################
# Define the ExampleSensor as a concrete implementation of the Sensor class
#
# A concrete Sensor class must implement the run() method.
#############################################################################################################
class ExampleSensor(Sensor):
    def __init__(self, sds_config: SensorDsCfg) -> None:
        super().__init__(sds_config)

    def run(self) -> None:
        """The run method is called when the Sensor is started."""

        # Get the Datastream objects for this sensor so we can log / save data to them
        self.example_log_ds: Optional[Datastream] = self.get_datastream(
            ds_type_id=EXAMPLE_LOG_DS_TYPE.ds_type_id, sensor_index=1
        )
        self.example_file_ds: Optional[Datastream] = self.get_datastream(
            ds_type_id=EXAMPLE_FILE_DS_TYPE.ds_type_id, sensor_index=1
        )

        assert self.example_log_ds is not None
        assert self.example_file_ds is not None

        # Main sensor loop
        # All sensor implementations must check for stop_requested to allow the sensor to be stopped cleanly
        while not self.stop_requested:
            self.example_log_ds.log({"temperature": 25.0})
            fname = file_naming.get_temporary_filename("jpg")
            # Generate a random image file
            with open(fname, "w") as f:
                f.write("This is a dummy image file")
            self.example_file_ds.save_recording(fname, api.utc_now())

            # Sensors should not sleep for more than ~180s so that the stop_requested flag can be checked
            # and the sensor shut down cleanly in a reasonable time frame.
            sleep(0.1)
