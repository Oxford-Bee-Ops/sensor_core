
import pytest

from sensor_core import configuration as root_cfg
from sensor_core.utils import utils

logger = utils.setup_logger("sensor_core")
root_cfg.TEST_MODE = True

class Test_configuration:
    @pytest.mark.parametrize(
        "test_input,expected",
        [
            ("('d01111111111','name')", "DUMMY"),
        ],
    )
    @pytest.mark.quick
    def test_get_field(self, test_input: str, expected: str) -> None:
        _, key = eval(test_input)
        assert root_cfg.my_device.get_field(key) == expected

    @pytest.mark.quick
    def test_display_cfg(self) -> None:
        assert root_cfg.my_device.display() != ""
