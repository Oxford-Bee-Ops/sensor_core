from time import sleep

import pytest
from sensor_core import DeviceCfg, SensorCore
from sensor_core import configuration as root_cfg
from sensor_core.sensors import device_recipes
from sensor_core.utils import utils

logger = utils.setup_logger("sensor_core")

root_cfg.TEST_MODE = True

INVENTORY: list[DeviceCfg] = [
    DeviceCfg(
        name="Alex",
        device_id="d01111111111",  # This is the DUMMY MAC address for windows
        notes="Testing trap camera device",
        sensor_ds_list=device_recipes.trap_cam_device,
    ),
]

class Test_trap_cam_device:

    @pytest.mark.quick
    def test_trap_cam_device(self):
        
        sc = SensorCore()
        sc.configure(INVENTORY)

        # Run for 30s to get some data and let the processors run
        # Should have 1 video after 18s (180s at 10x test speed)
        sc.start()
        sleep(30)
        sc.stop()
