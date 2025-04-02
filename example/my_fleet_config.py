
from example import my_config_object_defs as my_config_object_defs
from example.my_config_object_defs import ExampleDfDsCfg, ExampleSensorCfg
from sensor_core.config_objects import DeviceCfg, SensorDsCfg, WifiClient

###############################################################################
# SensorCore config model
#
# At the top level, we are defining configuration for a fleet of devices.
# This fleet config must be returned by a class called Inventory.get_inventory().
# The fully qualified class reference for this Inventory class is what is passed to SensorCore
# when it is first configured:
#
#   SensorCore.configure(fleet_config_py="example.my_fleet_config.Inventory")
#
# The fleet config returned by get_inventory() is a dictionary of DeviceCfg objects,
# with the device's mac address as the key (stripped of ':'s).
#
#   {"d01111111111": DeviceCfg()}
#
# The DeviceCfg contains:
# - name: a friendly name for the device (eg Alex)
# - notes: free-form notes on what the device is being used for (eg "Experiment 1")
# - system: a SystemCfg object which defines how the system parameters, such as cloud storage configuration
# - sensor_ds_list: a list of SensorDsCfg objects defining each Sensor and its associated Datastreams
#
# A Datastream defines a source of data coming from a Sensor.
# A Sensor may produce multiple Datastreams, each with a different type of data.
# The Sensor configuration is stored in a SensorCfg object.
# The Datastream configuration is stored in a DatastreamCfg object.
# The combined config of a sensor and its datastreams are in a SensorDsCfg object.
#
# The data produced by a Datastream (eg video files) may be processed by 0 or more DataProcessors.
# In the video file example, a DataProcessor might use an ML algorithm to identify bees in a video
# and output the number of bees identified.
# DataProcessors act in a chain, with data being passed from one to the next.
# The DataProcessors associated with a Datastream are defined on the DatastreamCfg
# as lists of DataProcessorCfg objects.
# There are two lists:
#  - EdgeProcessors that act on the device
#  - CloudProcessors that act as part of a subsequent ETL on a server or in the cloud.
#
# DeviceCfg (1 per physical device)
# -> sensor_ds_list: list[SensorDsCfg] - 1 per Sensor)
#    -> [0]
#       -> sensor_cfg: SensorCfg
#       -> datastream_cfgs: list[DatastreamCfg]
#          -> [0]
#             -> edge_processors: list[DataProcessorCfg]
#             -> cloud_processors: list[DataProcessorCfg]
#
###############################################################################

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
            my_config_object_defs.EXAMPLE_FILE_DS_TYPE,
        ],
    ),
    SensorDsCfg(
        sensor_cfg=ExampleSensorCfg(sensor_index=2),
        datastream_cfgs=[
            my_config_object_defs.EXAMPLE_FILE_DS_TYPE,
        ],
    ),
]

# Pre-configure the devices with awareness of wifi APs
WIFI_CLIENTS: list[WifiClient] = [
        WifiClient("bee-ops", 100, "abcdabcd"),
        WifiClient("bee-ops-zone", 85, "abcdabcd"),
        WifiClient("bee-ops-zone1", 80, "abcdabcd"),
        WifiClient("bee-ops-zone2", 70, "abcdabcd"),
    ]

    
###############################################################################
# Define per-device configuration for the fleet of devices
###############################################################################
INVENTORY: dict[str, DeviceCfg] = {
    "d01111111111": DeviceCfg(  # This is the DUMMY MAC address for windows
        name="Alex",
        device_id="d01111111111",
        notes="Using Alex as an all-defaults camera in Experiment A",
        sensor_ds_list=experiment1_standard_camera_device,
        wifi_clients=WIFI_CLIENTS,
    ),
    "d01111111112": DeviceCfg(
        name="Bob",
        device_id="d01111111112",
        notes="Using Bob as a close up camera in Experiment A",
        sensor_ds_list=experiment1_double_camera_device,
        wifi_clients=WIFI_CLIENTS,
    ),
    "d12222222222": DeviceCfg(
        name="Charlie",
        device_id="d12222222222",
        notes="Using Charlie as a special sort of camera in Experiment A",
        sensor_ds_list=[
            SensorDsCfg(
                sensor_cfg=ExampleSensorCfg(a_custom_field="value_set_per_device"),
                datastream_cfgs=[
                    ExampleDfDsCfg(),
                    my_config_object_defs.EXAMPLE_LOG_DS_TYPE,
                    my_config_object_defs.EXAMPLE_FILE_DS_TYPE,
                ],
            )
        ],
        wifi_clients=WIFI_CLIENTS,
    ),
}


# Implement the class definition so that this file can be imported by SensorCore
class Inventory:
    def get_inventory(self) -> dict[str, DeviceCfg]:
        return INVENTORY
