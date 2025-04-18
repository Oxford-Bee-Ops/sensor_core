from sensor_core.config_objects import SensorDsCfg

from example import my_config_object_defs as my_config_object_defs
from example.my_config_object_defs import ExampleSensorCfg

###############################################################################
# Define global configuration & device types for the fleet
###############################################################################

# Define the datastreams produced by a device type
experiment1_standard_camera_device = [
    SensorDsCfg(
        sensor_cfg=ExampleSensorCfg(sensor_index=1),
        datastream_cfgs=[
            my_config_object_defs.EXAMPLE_LOG_DS_TYPE,
            my_config_object_defs.EXAMPLE_FILE_DS_TYPE,
        ],
    )
]

experiment1_double_camera_device = [
    SensorDsCfg(
        sensor_cfg=ExampleSensorCfg(sensor_index=1),
        datastream_cfgs=[
            my_config_object_defs.EXAMPLE_LOG_DS_TYPE,
            my_config_object_defs.EXAMPLE_FILE_DS_TYPE,
        ],
    ),
    SensorDsCfg(
        sensor_cfg=ExampleSensorCfg(sensor_index=2),
        datastream_cfgs=[
            my_config_object_defs.EXAMPLE_LOG_DS_TYPE,
            my_config_object_defs.EXAMPLE_FILE_DS_TYPE,
        ],
    ),
]
