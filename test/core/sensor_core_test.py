from time import sleep

import pytest
from sensor_core import configuration as root_cfg
from sensor_core.sensor_core import SensorCore
from sensor_core.utils import utils

from example import my_fleet_config

logger = utils.setup_logger("sensor_core")
root_cfg.TEST_MODE = True

class Test_SensorFactory:
    @pytest.mark.quick
    def test_SensorCore_status(self) -> None:
        sc = SensorCore()
        sc.configure(my_fleet_config.MyInventory())
        message = sc.status()
        logger.info(message)
        assert message is not None

    @pytest.mark.quick
    def test_SensorCore_cycle(self) -> None:
        # Standard flow
        # We reset cfg.my_device_id to override the computers mac_address
        # This is a test device defined in BeeOps.cfg to have a DummySensor.
        root_cfg.update_my_device_id("d01111111111")

        sc = SensorCore()
        sc.configure(my_fleet_config.MyInventory())
        sc.start()
        sleep(2)
        sc.status()
        # This should be rejected because the sensor is already running
        #with pytest.raises(Exception):
        #    sc.configure("example.my_fleet_config.Inventory")
        sc.stop()
        sc.status()

        # Start again
        sc.start()
        sc.stop()
