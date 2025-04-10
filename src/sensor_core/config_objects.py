from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

from sensor_core import api
from sensor_core.utils import dc

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
class SensorCfg:
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


@dataclass(frozen=True)
class DataProcessorCfg:
    """Defines the configuration for a concrete DataProcessor class implementation.
    Can be subclassed to add additional configuration parameters specific to the DataProcessor class."""

    # We use class references instead of instances to avoid circular imports
    # On initialisation, we will create an instance of the DataProcessor
    # Class ref like "example.my_example_processor.DataProcessor"
    dp_class_ref: str
    dp_description: str
    input_format: api.FILE_FORMATS
    output_format: api.FILE_FORMATS
    input_fields: Optional[list[str]] = None
    output_fields: Optional[list[str]] = None
    derived_datastreams: Optional[list["DatastreamCfg"]] = None


@dataclass
class DatastreamCfg:
    """Defines the configuration for a datastream produced by a sensor."""

    # datastream_type_id is a unique 5-character string that identifies the type of data.
    # This combines the intended use of the data and the format of the data that is to be archived.
    # One of allowed_datastream_types
    ds_type_id: str

    raw_format: api.FILE_FORMATS
    archived_format: api.FILE_FORMATS
    archived_data_description: str

    # The cloud storage container to which the data is archived.
    # This is required for all types uploading files, other than archived_format="CSV".
    # "CSV" data is uploaded to the DeviceCfg.cc_for_journals container.
    cloud_container: Optional[str] = None

    # data_fields is a list defining the names of the fields expected for each data entry.
    # This is only used for log-type and csv-type data.
    raw_fields: Optional[list[str]] = None

    archived_fields: Optional[list[str]] = None

    # Some datastreams support saving of sample raw recordings to the archive.
    # This string is interpreted by the Sensor to determine the frequency of raw data sampling.
    # The format of this string is sensor-specific.
    sample_probability: Optional[str] = None

    # If raw sampling is enabled, a sample_container must be specified.
    sample_container: Optional[str] = None

    # transformations is a list of transformations that are applied to the raw data to produce the data
    # that is stored.
    edge_processors: Optional[list[DataProcessorCfg]] = None
    cloud_processors: Optional[list[DataProcessorCfg]] = None


@dataclass
class SensorDsCfg:
    """Bundles the configuration for a sensor and the datastreams it produces."""

    # The configuration for the sensor that produces the data.
    sensor_cfg: SensorCfg

    # The configuration for the datastream(s) produced.
    datastream_cfgs: list[DatastreamCfg]

    def get_datastream_cfg(self, ds_type_id: str) -> DatastreamCfg:
        for ds_cfg in self.datastream_cfgs:
            if ds_cfg.ds_type_id == ds_type_id:
                return ds_cfg
        raise ValueError(f"Datastream {ds_type_id} not found in sensor config {self.datastream_cfgs}")

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
    sensor_ds_list: list[SensorDsCfg] = field(default_factory=list)

    # Default cloud container for file upload
    cc_for_upload: str = "sensor-core-upload"

    # Cloud storage container for raw CSV journals uploaded by the device
    cc_for_journals: str = "sensor-core-journals"

    # Cloud storage container for system records (Datasreams: SCORE, SCORP, FAIRY)
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
        for sensor_ds in self.sensor_ds_list:
            sensor_type = sensor_ds.sensor_cfg.sensor_type
            if sensor_type in sensor_types:
                sensor_types[sensor_type] += 1
            else:
                sensor_types[sensor_type] = 1
        return sensor_types

    def datastreams_configured(self) -> dict[str, int]:
        """Counts the number of datastreams of each type produced by the sensors on the device"""
        datastreams: dict[str, int] = {}
        for sensor_ds in self.sensor_ds_list:
            for datastream_cfg in sensor_ds.datastream_cfgs:
                datastream_type = datastream_cfg.ds_type_id
                if datastream_type in datastreams:
                    datastreams[datastream_type] += 1
                else:
                    datastreams[datastream_type] = 1
        return datastreams

############################################################
# Inventory class
############################################################
class Inventory(ABC):
    @staticmethod
    @abstractmethod
    def get_inventory() -> list[DeviceCfg]:
        """Return a list of DeviceCfg inventory objects."""
        raise NotImplementedError("get_inventory() must be implemented in subclasses")

    @staticmethod
    def validate_my_config() -> None:
        """Validate the configuration for this device."""
        from sensor_core import SensorCore
        SensorCore().configure(Inventory.get_inventory())

@dataclass
class DpContext:
    """Class for supplying context information on calls out from SensorCore"""
    sensor: Optional[SensorCfg]
    ds: DatastreamCfg
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
    inventory_class: str = FAILED_TO_LOAD
    ############################################################
    # Default-able settings
    ############################################################
    install_type: str ="rpi_sensor"
    # Logging and storage settings
    enable_volatile_logs: str ="Yes"
    journald_SystemMaxUse: str ="50M"
    # Do you want SensorCore to start automatically after running the rpi_installer.sh script?
    enable_auto_start: str ="Yes"
    # Enable the UFW firewall
    enable_firewall: str = "Yes"
    # Enable use of predictable network interface names
    enable_predictable_interface_names: str = "Yes"
    # Enable the I2C interface on the Raspberry Pi
    enable_i2c: str = "Yes"
    # The location of the virtual environment relative to the $HOME directory.
    # (ie will expand to "$HOME/$venv_dir").
    # This will be created if it does not exist.
    venv_dir: str =".venv"
    # The location where your custom code is installed, relative to the $HOME directory 
    # (ie will expand to "$HOME/<my_code_dir>/<Git project name>").
    # The Git project name is the final component of the URL above (with the ".git" suffix removed)
    my_code_dir: str ="code"
    # The URL for the Git repo with the SensorCore code.
    # We only need this while SensorCore is in development.
    sensor_core_git_url: str ="github.com:Oxford-Bee-Ops/sensor_core.git"
    sensor_core_git_branch: str ="main"
    # Pydantic-settings helper
    model_config = SettingsConfigDict(extra="ignore")
