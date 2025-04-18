from time import sleep

import pytest
from example.my_device_types import experiment1_double_camera_device
from sensor_core import DeviceCfg, SensorCore
from sensor_core import configuration as root_cfg
from sensor_core.utils.sc_test_emulator import ScEmulator

logger = root_cfg.setup_logger("sensor_core")

root_cfg.TEST_MODE = root_cfg.MODE.TEST

INVENTORY: list[DeviceCfg] = [
    DeviceCfg(
        name="Alex",
        device_id="d01111111111",  # This is the DUMMY MAC address for windows
        notes="Testing example camera device",
        sensor_ds_list=experiment1_double_camera_device,
    ),
]

class Test_example_device:

    @pytest.mark.quick
    def test_example_device(self):

        with ScEmulator.get_instance() as th:
            # Limit the SensorCore to 1 recording so we can easily validate the results
            #th.set_recording_cap(1)

            # Configure SensorCore with the trap camera device
            sc = SensorCore()
            sc.configure(INVENTORY)
            sc.start()
            sleep(10)
            sc.stop()

            # We should have identified bees in the video and save the info to the EXITCAM datastream
            th.assert_records("sensor-core-fair", 
                            {"V3_DUMM*": 6})
            th.assert_records("sensor-core-journals", 
                            {"*": 3})