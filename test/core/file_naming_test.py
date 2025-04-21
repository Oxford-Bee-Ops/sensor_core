from datetime import datetime, timedelta

import pytest
from example.my_config_object_defs import ExampleSensorCfg
from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.utils import file_naming

logger = root_cfg.setup_logger("sensor_core")
root_cfg.TEST_MODE = root_cfg.MODE.TEST

class Test_datastream:

    @pytest.mark.quick
    def test_file_naming(self) -> None:
        from example.my_config_object_defs import EXAMPLE_FILE_DS_TYPE
        from sensor_core.dp_engine import DPengine

        datastream = DPengine(
            datastream_config=EXAMPLE_FILE_DS_TYPE, 
            device_id="d01111111111", 
            sensor_index=1,
            dp_tree=ExampleSensorCfg(),
        )
        # datastream.start()
        fname = file_naming.get_record_filename(
            root_cfg.EDGE_PROCESSING_DIR,
            ds_id=datastream.ds_id,
            suffix=datastream.ds_config.input_format,
            start_time=api.utc_now() - timedelta(hours=1),
            end_time=api.utc_now(),
        )
        print(fname)
        fields = file_naming.parse_record_filename(fname)
        assert fields[api.RECORD_ID.DATA_TYPE_ID.value] == EXAMPLE_FILE_DS_TYPE.type_id
        assert fields[api.RECORD_ID.DEVICE_ID.value] == "d01111111111"
        assert fields[api.RECORD_ID.SENSOR_INDEX.value] == 1
        assert isinstance(fields[api.RECORD_ID.TIMESTAMP.value], datetime)
        assert (
            isinstance(fields[api.RECORD_ID.END_TIME.value], datetime)
            or fields[api.RECORD_ID.END_TIME.value] is None
        )
        assert fields[api.RECORD_ID.SUFFIX.value] == EXAMPLE_FILE_DS_TYPE.input_format

        fname = file_naming.get_record_filename(
            root_cfg.EDGE_PROCESSING_DIR,
            ds_id=datastream.ds_id,
            suffix=datastream.ds_config.input_format,
            start_time=api.utc_now() - timedelta(hours=1),
            end_time=api.utc_now(),
            frame_number=2,
            arbitrary_index=4,
        )
        print(fname)
        fields = file_naming.parse_record_filename(fname)
        assert fields[api.RECORD_ID.DEVICE_ID.value] == "d01111111111"
        assert fields[api.RECORD_ID.SENSOR_INDEX.value] == 1
        assert fields[api.RECORD_ID.DATA_TYPE_ID.value] == EXAMPLE_FILE_DS_TYPE.type_id
        assert isinstance(fields[api.RECORD_ID.TIMESTAMP.value], datetime)
        assert (
            isinstance(fields[api.RECORD_ID.END_TIME.value], datetime)
            or fields[api.RECORD_ID.END_TIME.value] is None
        )
        assert fields[api.RECORD_ID.SUFFIX.value] == EXAMPLE_FILE_DS_TYPE.input_format
        assert fields[api.RECORD_ID.OFFSET.value] == 2
        assert fields[api.RECORD_ID.SECONDARY_OFFSET.value] == 4
        # datastream.stop()

    @pytest.mark.quick
    def test_id_parsing(self) -> None:
        device_id = "d01111111111"
        type_id = "test"
        sensor_id = 1
        node_index = 3
        output = file_naming.parse_data_id(
            ds_id=file_naming.create_data_id(
                device_id=device_id, type_id=type_id, sensor_id=sensor_id
            )
        )
        assert output[0] == device_id
        assert output[1] == type_id
        assert output[2] == sensor_id

        output = file_naming.parse_data_id(
            ds_id=file_naming.create_data_id(
                device_id=device_id, 
                type_id=type_id, 
                sensor_id=sensor_id,
                node_index=node_index,
            )
        )
        assert output[0] == device_id
        assert output[1] == type_id
        assert output[2] == sensor_id
        assert output[3] == node_index
