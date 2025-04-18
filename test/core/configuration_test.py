
import pytest
from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core import config_validator
from sensor_core.config_objects import SensorDsCfg, DatastreamCfg, DataProcessorCfg, DeviceCfg
from example.my_config_object_defs import (
    ExampleSensorCfg, EXAMPLE_LOG_DS_TYPE, EXAMPLE_FILE_DS_TYPE_ID, EXAMPLE_DF_DS_TYPE_ID
)

logger = root_cfg.setup_logger("sensor_core")
root_cfg.TEST_MODE = root_cfg.MODE.TEST

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

    @pytest.mark.quick
    def test_config_validator(self) -> None:
        INVENTORY = [
            DeviceCfg(
                name="Alex",
                device_id="d01111111111",  # This is the DUMMY MAC address for windows
                notes="Testing example camera device",
                sensor_ds_list=[
                    SensorDsCfg(
                        sensor_cfg=ExampleSensorCfg(sensor_index=1),
                        datastream_cfgs=[
                            EXAMPLE_LOG_DS_TYPE,
                            DatastreamCfg(
                                ds_type_id = EXAMPLE_FILE_DS_TYPE_ID,
                                raw_format = "jpg",
                                raw_fields = [*api.REQD_RECORD_ID_FIELDS, "pixel_count"],
                                archived_format = "csv",
                                archived_fields= [*api.REQD_RECORD_ID_FIELDS, "pixel_count"],
                                archived_data_description = "Example file datastream for testing. ",
                                edge_processors = [DataProcessorCfg(
                                    dp_class_ref = "example.my_processor_example.ExampleProcessor",
                                    dp_description = "Dummy file processor for testing",
                                    input_format = "jpg",
                                    output_format = "df",
                                    output_fields = [*api.REQD_RECORD_ID_FIELDS, "pixel_count"],
                                    derived_datastreams = [
                                        DatastreamCfg(
                                            ds_type_id = EXAMPLE_DF_DS_TYPE_ID,
                                            raw_format = "csv",
                                            raw_fields = [*api.REQD_RECORD_ID_FIELDS, "pixel_count_transformed"],
                                            archived_format = "csv",
                                            #archived_fields = [*api.REQD_RECORD_ID_FIELDS, "pixel_count_transformed"],
                                            archived_data_description = "Example df datastream for testing. ",
                                        )
                                    ],
                                )],
                            ),
                        ],
                    )
                ]
            )
        ]
        # Check the configuration is valid
        is_valid, error_message = config_validator.validate(INVENTORY)
        # We expect to fail because the archived_fields are not set in the derived datastream
        # This tests recursve validation of the datastreams
        assert not is_valid, error_message