from time import sleep

from sensor_core import configuration as root_cfg
from sensor_core.sensor_core import SensorCore
from sensor_core.utils import utils

logger = utils.setup_logger("sensor_core")
root_cfg.TEST_MODE = False

def run_sensor_core_standalone():

        # Standard flow
        # We reset cfg.my_device_id to override the computers mac_address
        # This is a test device defined in BeeOps.cfg to have a DummySensor.
        root_cfg.update_my_device_id("d01111111111")

        sc = SensorCore()
        sc.configure("example.my_fleet_config.Inventory")
        sc.start()
        sc.status()
        while True:
            sleep(120)
            sc.status()

if __name__ == "__main__":
    run_sensor_core_standalone()