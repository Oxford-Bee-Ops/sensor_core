import logging
import sys

import pytest
from sensor_core import configuration as root_cfg
from sensor_core.sensors import processor_video_aruco
from sensor_core.utils import utils

logger = utils.setup_logger("rpi")


class Test_video_aruco_processor:
    logger.setLevel(level=logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    @pytest.mark.quick
    def test_aruco_processor_basic(self):
        file = (
            root_cfg.CODE_DIR / "sensor_core" 
            / "test"
            / "sensors"
            / "resources"
            / "5fps_4X4 5Mm 30Cm 20250107 154846.mp4"
        )

        # Run the processor
        processor = processor_video_aruco.VideoArucoProcessor()
        processor.process_video_file(source_file=file)
