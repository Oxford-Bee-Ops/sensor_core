from unittest.mock import patch

import pandas as pd
import pytest

from sensor_core.sensors.sensor_audio import AudioSensor as ac
from sensor_core.utils import utils

logger = utils.setup_logger("rpi")


class Test_AudioSensor:
    @pytest.mark.parametrize(
        "test_input,expected",
        [
            ("(3,0,3,'2024-02-02T12:01:00.000')", "(start_recording, 120)"),
            ("(4,0,3,'2024-02-02T12:01:00.000')", "(29 * 60, 0)"),
            ("(4,0,3,'2024-02-02T12:29:59.000')", "(1, 0)"),
            ("(4,4,3,'2024-02-02T12:29:59.000')", "(start_recording, 1)"),
            ("(4,4,4,'2024-02-02T12:00:00.000')", "(start_recording, av_rec_seconds)"),  # in-hive port
            ("(4,4,4,'2024-02-02T12:59:59.000')", "(1, 0)"),  # in-hive port
            ("(4,4,4,'2024-02-02T12:06:00.000')", "(max_sleep, 0)"),  # in-hive port
            ("(4,0,3,'2024-02-02T12:29:59.000')", "(1, 0)"),
            ("(4,0,4,'2024-02-02T12:30:00.000')", "(start_recording, av_rec_seconds)"),
            ("(4,0,4,'2024-02-02T12:31:00.000')", "(start_recording, 120)"),
            ("(4,0,1,'2024-02-02T12:01:00.000')", "(start_recording, 120)"),
            ("(4,0,1,'2024-02-02T12:29:00.000')", "(0, 60)"),
            ("(4,0,1,'2024-02-02T12:31:00.000')", "(29 * 60, 0)"),
        ],
    )
    @pytest.mark.quick
    def test_ok_to_record(self, test_input, expected):
        max_sleep = 1800
        start_recording = 0 # noqa
        av_rec_seconds = 180
        num_devices, in_hive_port, port, timestamp = eval(test_input)
        expected_sleep_for, expected_record_for = eval(expected)
        timestamp = pd.to_datetime(timestamp)
        # If utils.pause_recording() returns True, we should always return the max_sleep value
        with patch("common.utils.pause_recording", return_value=True):
            sleep_for, record_for = ac.ok_to_record(
                num_devices, in_hive_port, port, timestamp, av_rec_seconds
            )
            assert sleep_for == max_sleep
        # If it's False, we should return the "expected" value
        with patch("common.utils.pause_recording", return_value=False):
            sleep_for, record_for = ac.ok_to_record(
                num_devices, in_hive_port, port, timestamp, av_rec_seconds
            )
            assert sleep_for == expected_sleep_for
            assert record_for == expected_record_for
