############################################################################################################
# Datastream defines a source of data coming from a sensor.
# A sensor may produce multiple datastreams, each with a different type of data.
# The type of data is defined by a DatastreamType object.
# This file defines the concrete DatastreamTypes in use .
#############################################################################################################

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.config_objects import DatastreamCfg

############################################################################################################
# Datastreams produced by the SensorCore system
#############################################################################################################

# SCORE - DatastreamType for recording sample count / duration from the data pipeline
SCORE_DS_TYPE = DatastreamCfg(
    ds_type_id="SCORE",
    raw_format="log",
    raw_fields=[*api.REQD_RECORD_ID_FIELDS, "observed_ds_type_id", "sample_period", "count", "duration"],
    archived_format="csv",
    archived_fields=[*api.REQD_RECORD_ID_FIELDS, "observed_ds_type_id", "sample_period", "count", "duration"],
    archived_data_description=(
        "Data on sample counts and recording period durations from all Datastreams. "
        "The data is automatically recorded by the SensorCore for all datastreams when "
        "they log data or save a recording."
    ),
    cloud_container=root_cfg.my_device.cc_for_system_records,
    edge_processors=None,
)

# SCORP - special DatastreamType for recording performance of the data pipeline
SCORP_DS_TYPE = DatastreamCfg(
    ds_type_id="SCORP",
    raw_format="log",
    raw_fields=[*api.REQD_RECORD_ID_FIELDS, "data_processor_id", "observed_ds_type_id", "duration"],
    archived_format="csv",
    archived_fields=[*api.REQD_RECORD_ID_FIELDS, "data_processor_id", "observed_ds_type_id", "duration"],
    archived_data_description=(
        "Performance data from the data pipeline. "
        "The data is recorded as a log file on the device and archived as a CSV file."
    ),
    cloud_container=root_cfg.my_device.cc_for_system_records,
    edge_processors=None,
)

# FAIRY - special DatastreamType for recording FAIR records of Datastream config
FAIRY_DS_TYPE = DatastreamCfg(
    ds_type_id="FAIRY",
    raw_format="yaml",
    archived_format="yaml",
    archived_data_description=("Record of Datastream config created when a Datastream starts. "),
    cloud_container=root_cfg.my_device.cc_for_fair,
)

# HEART - special DatastreamType for recording device & system health
HEART_FIELDS = [
    "boot_time",
    "last_update_timestamp",
    "device_id",
    "cpu_percent",
    "cpu_idle",
    "cpu_user",
    "total_memory_gb",
    "memory_percent",
    "memory_free",
    "disk_percent",
    "disk_writes_in_period",
    "sc_mount_size",
    "sc_ram_percent",
    "cpu_temperature",
    "ssid",
    "ip_address",
    "power_status",
    "process_list",
    "git_commit_hash",
]

HEART_DS_TYPE = DatastreamCfg(
    ds_type_id="HEART",
    raw_format="log",
    raw_fields=api.REQD_RECORD_ID_FIELDS + HEART_FIELDS,
    archived_format="csv",
    archived_fields=api.REQD_RECORD_ID_FIELDS + HEART_FIELDS,
    archived_data_description=("Device and system health records. "),
    cloud_container=root_cfg.my_device.cc_for_system_records,
)

# WARNING - special DatastreamType for capturing warning and error logs from any component
WARNING_FIELDS = [
    "time_logged",
    "message",
    "process_id",
    "process_name",
    "executable_path",
    "priority",
]

WARNING_DS_TYPE = DatastreamCfg(
    ds_type_id="WARNING",
    raw_format="log",
    raw_fields=api.REQD_RECORD_ID_FIELDS + WARNING_FIELDS,
    archived_format="csv",
    archived_fields=api.REQD_RECORD_ID_FIELDS + WARNING_FIELDS,
    archived_data_description=("Warning and error logs raised on the device. "),
    cloud_container=root_cfg.my_device.cc_for_system_records,
)

SYSTEM_DS_TYPES = [
    SCORE_DS_TYPE.ds_type_id, 
    SCORP_DS_TYPE.ds_type_id,
    FAIRY_DS_TYPE.ds_type_id,
    HEART_DS_TYPE.ds_type_id,
    WARNING_DS_TYPE.ds_type_id
]