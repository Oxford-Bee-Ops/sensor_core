import os

import pandas as pd
import pytest
import logging
from sensor_core import configuration as root_cfg
from sensor_core import bcli

logger = root_cfg.setup_logger("sensor_core", logging.DEBUG)

class Test_bcli:
    @pytest.mark.quick
    def test_bcli(self) -> None:
        bcli.main()
