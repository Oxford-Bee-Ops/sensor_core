import pytest

from sensor_core import device_manager as dm


class Test_device_manager:
    @pytest.mark.quick
    def test_device_manager(self) -> None:
        dm.DeviceManager()