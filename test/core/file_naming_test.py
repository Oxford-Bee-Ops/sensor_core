from datetime import datetime, timedelta

import pytest
from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.utils import file_naming, utils

from example.my_config_object_defs import ExampleSensorCfg

logger = utils.setup_logger("sensor_core")
root_cfg.TEST_MODE = True

class Test_datastream:
    @pytest.mark.quick
    def test_file_naming(self) -> None:
        from sensor_core.datastream import Datastream

        from example.my_config_object_defs import EXAMPLE_FILE_DS_TYPE

        datastream = Datastream(
            datastream_config=EXAMPLE_FILE_DS_TYPE, 
            device_id="d01111111111", 
            sensor_index=1,
            sensor_config=ExampleSensorCfg(),
        )
        # datastream.start()
        fname = file_naming.get_record_filename(
            root_cfg.EDGE_PROCESSING_DIR,
            ds_id=datastream.ds_id,
            suffix=datastream.ds_config.raw_format,
            start_time=api.utc_now() - timedelta(hours=1),
            end_time=api.utc_now(),
        )
        print(fname)
        fields = file_naming.parse_record_filename(fname)
        assert fields[api.RECORD_ID.DS_TYPE_ID.value] == EXAMPLE_FILE_DS_TYPE.ds_type_id
        assert fields[api.RECORD_ID.DEVICE_ID.value] == "d01111111111"
        assert fields[api.RECORD_ID.SENSOR_INDEX.value] == 1
        assert isinstance(fields[api.RECORD_ID.TIMESTAMP.value], datetime)
        assert (
            isinstance(fields[api.RECORD_ID.END_TIME.value], datetime)
            or fields[api.RECORD_ID.END_TIME.value] is None
        )
        assert fields[api.RECORD_ID.SUFFIX.value] == EXAMPLE_FILE_DS_TYPE.raw_format

        fname = file_naming.get_record_filename(
            root_cfg.EDGE_PROCESSING_DIR,
            ds_id=datastream.ds_id,
            suffix=datastream.ds_config.raw_format,
            start_time=api.utc_now() - timedelta(hours=1),
            end_time=api.utc_now(),
            frame_number=2,
            arbitrary_index=4,
        )
        print(fname)
        fields = file_naming.parse_record_filename(fname)
        assert fields[api.RECORD_ID.DEVICE_ID.value] == "d01111111111"
        assert fields[api.RECORD_ID.SENSOR_INDEX.value] == 1
        assert fields[api.RECORD_ID.DS_TYPE_ID.value] == EXAMPLE_FILE_DS_TYPE.ds_type_id
        assert isinstance(fields[api.RECORD_ID.TIMESTAMP.value], datetime)
        assert (
            isinstance(fields[api.RECORD_ID.END_TIME.value], datetime)
            or fields[api.RECORD_ID.END_TIME.value] is None
        )
        assert fields[api.RECORD_ID.SUFFIX.value] == EXAMPLE_FILE_DS_TYPE.raw_format
        assert fields[api.RECORD_ID.OFFSET.value] == 2
        assert fields[api.RECORD_ID.SECONDARY_OFFSET.value] == 4
        # datastream.stop()
