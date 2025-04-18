from time import sleep

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.config_objects import SensorDsCfg
from sensor_core.sensor import Sensor
from sensor_core.utils import file_naming

logger = root_cfg.setup_logger("sensor_core")


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
        self.example_log_ds = self.get_datastreams(format="log", expected=1)[0]
        self.example_file_ds = self.get_datastreams(format="jpg", expected=1)[0]

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
            if root_cfg.TEST_MODE == root_cfg.MODE.TEST:
                # In test mode, sleep for 0.1s to allow the test to run quickly
                sleep(0.1)
            else:
                sleep(10)
