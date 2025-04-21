from dataclasses import dataclass, field
from typing import Any, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

from sensor_core import api
from sensor_core.utils import dc
from sensor_core.dp_tree import DPtree, DPtreeNodeCfg

############################################################################################
#
# Configuration classes
#
# The system assumes the following files are present in the KEYS directory:
# - keys.env (cloud storage and git keys)
# - sc_config.env (class reference for the fleet config)
#
# The system loads its main config from the fleet_config_py defined in the sc_config.env.
############################################################################################
FAILED_TO_LOAD = "Not set"


@dataclass
class Configuration:
    """Utility super class"""

    def update_field(self, field_name: str, value: Any) -> None:
        setattr(self, field_name, value)

    def update_fields(self, **kwargs: Any) -> None:
        for field_name, value in kwargs.items():
            self.update_field(field_name, value)

    def display(self) -> str:
        display_str = dc.display_dataclass(self)
        return display_str

    def get_field(self, field_name: str) -> Any:
        return getattr(self, field_name)


@dataclass
class SensorCfg(DPtreeNodeCfg):
    """Defines the configuration for a concrete Sensor class implementation.
    Can be subclassed to add additional configuration parameters specific to the Sensor class.

    Parameters:
    ----------
    sensor_type: str
        One of the sensor types defined in api.SENSOR_TYPES.

    sensor_index: int
        The index of the sensor in the list of sensors.
        Must be unique in combination with the sensor_type.
        Used, for example, where a device has 4 audio sensors.

    sensor_class_ref: str
        The fully qualified class name of the sensor.
        This must be interpretable as a Class by the Python interpreter.

    sensor_model_description: str
        A human-readable description of the sensor model.
    """

    sensor_type: api.SENSOR_TYPES
    sensor_index: int
    sensor_class_ref: str
    sensor_model_description: str


@dataclass
class DataProcessorCfg(DPtreeNodeCfg):
    """Defines the configuration for a concrete DataProcessor class implementation.
    Can be subclassed to add additional configuration parameters specific to the DataProcessor class."""



@dataclass
class DatastreamCfg(DPtreeNodeCfg):
    """Defines the configuration for a datastream produced by a sensor."""

    # The cloud storage container to which the data is archived.
    # This is required for all types uploading files, other than output_format="CSV".
    # "CSV" data is uploaded to the DeviceCfg.cc_for_journals container.
    cloud_container: Optional[str] = None


############################################################################################
# Wifi configuration
############################################################################################
@dataclass
class WifiClient:
    ssid: str
    priority: int
    pw: str

############################################################################################
# Configuration for a device
############################################################################################
@dataclass
class DeviceCfg(Configuration):
    """Configuration for a device"""

    name: str = "default"
    device_id: str = "unknown"
    notes: str = "blank"

    # Sensor and datastream configuration
    dp_trees: list[DPtree] = field(default_factory=list)

    # Default cloud container for file upload
    cc_for_upload: str = "sensor-core-upload"

    # Cloud storage container for raw CSV journals uploaded by the device
    cc_for_journals: str = "sensor-core-journals"

    # Cloud storage container for system records (Datasreams: SCORE, SCORP, FAIRY, HEART)
    cc_for_system_records: str = "sensor-core-system-records"

    # Cloud container for FAIR records
    cc_for_fair: str = "sensor-core-fair"

    # Frequency of sending device health heart beat
    heart_beat_frequency: int = 60 * 10

    # Max recording timer in seconds
    # This limits how quickly the system will cleanly shutdown as we wait for all recording 
    # threads to complete. It also limits the duration of any recordings
    max_recording_timer: int = 180

    # Logging: 20=INFO, 10=DEBUG as per logging module
    log_level: int = 20

    # Device management
    auto_update_os: bool = True
    auto_update_os_cron: str = "0 2 * * 0"  # Every Sunday at 2am
    auto_update_code: bool = True
    auto_update_code_cron: str = "0 3 * * *"  # Every day at 3am
    attempt_wifi_recovery: bool = False
    manage_leds: bool = True

    # Wifi networks
    # These are the networks that the device will connect to if they are available.
    wifi_clients: list[WifiClient] = field(default_factory=list)

    # Wifi devices
    client_wlan: str = "wlan0"

    # Additional wifi configuration
    local_wifi_ssid: str = "not_set"
    local_wifi_pw: str = "not_set"
    local_wifi_priority: int = 80

    # Test device
    is_testnet: int = 0

    def sensor_types_configured(self) -> dict[str, int]:
        """Counts the number of sensors of each type installed on the device"""
        sensor_types: dict[str, int] = {}
        for dptree in self.dp_trees:
            sensor_type = dptree.sensor.get_config().sensor_type
            if sensor_type in sensor_types:
                sensor_types[sensor_type] += 1
            else:
                sensor_types[sensor_type] = 1
        return sensor_types

    def datastreams_configured(self) -> dict[str, int]:
        """Counts the number of datastreams of each type produced by the sensors on the device"""
        datastreams: dict[str, int] = {}
        for sensor_ds in self.dp_trees:
            for datastream_cfg in sensor_ds.datastream_cfgs:
                datastream_type = datastream_cfg.type_id
                if datastream_type in datastreams:
                    datastreams[datastream_type] += 1
                else:
                    datastreams[datastream_type] = 1
        return datastreams


@dataclass
class DpContext:
    """Class for supplying context information on calls out from SensorCore"""
    sensor: Optional[SensorCfg]
    ds: Datastream
    dp: DataProcessorCfg


############################################################################################
# Define the two .env files that hold the keys and the SensorCore configuration class ref
############################################################################################
class Keys(BaseSettings):
    """Class to hold the keys for the system"""

    cloud_storage_key: str = FAILED_TO_LOAD
    model_config = SettingsConfigDict(extra="ignore")


class SystemCfg(BaseSettings):
    """Class to hold the keys for the system"""
    ############################################################
    # Mandatory custom settings
    ############################################################
    # The URL for the Git repo with the user's config and custom sensor code.
    my_git_repo_url: str = FAILED_TO_LOAD
    my_git_branch: str = FAILED_TO_LOAD
    my_git_ssh_private_key_file: str = FAILED_TO_LOAD
    my_fleet_config: str = FAILED_TO_LOAD
    my_start_script: str = FAILED_TO_LOAD
    ############################################################
    # Default-able settings
    ############################################################
    install_type: str ="rpi_sensor"
    # Logging and storage settings
    enable_volatile_logs: str ="Yes"
    # Do you want SensorCore to start automatically after running the rpi_installer.sh script?
    auto_start: str ="Yes"
    # Enable the UFW firewall
    enable_firewall: str = "Yes"
    # Enable use of predictable network interface names
    enable_predictable_interface_names: str = "Yes"
    # Enable the I2C interface on the Raspberry Pi
    enable_i2c: str = "Yes"
    # The location of the virtual environment relative to the $HOME directory.
    # (ie will expand to "$HOME/$venv_dir").
    # This will be created if it does not exist.
    venv_dir: str ="venv"
    # The URL for the Git repo with the SensorCore code.
    # We only need this while SensorCore is in development.
    sensor_core_git_url: str ="github.com:Oxford-Bee-Ops/sensor_core.git"
    sensor_core_git_branch: str ="main"
    # Pydantic-settings helper
    model_config = SettingsConfigDict(extra="ignore")
