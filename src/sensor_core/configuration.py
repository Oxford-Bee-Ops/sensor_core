import platform
from enum import Enum
from pathlib import Path
from typing import Optional

import psutil
from pydantic_settings import SettingsConfigDict

from sensor_core.config_objects import FAILED_TO_LOAD, DeviceCfg, Keys, SystemCfg

############################################################################################
# Test mode flag
############################################################################################
TEST_MODE: bool = False

############################################################################################
#
# Platform discovery
#
############################################################################################
def _get_pi_model() -> str:
    try:
        with open("/proc/device-tree/model", "r") as model_file:
            return model_file.read()
    except FileNotFoundError:
        print("ERROR: proc/device-tree/model file not found. Are you running this on a Raspberry Pi?")
        return "Unknown"


running_on_linux = False
running_on_rpi = False
running_on_rpi5 = False
running_on_windows = False
running_on_azure = False

if "Linux" in platform.platform():
    running_on_linux = True
    if "rpi" in platform.platform():
        running_on_rpi = True
        if "Pi 5" in _get_pi_model():
            running_on_rpi5 = True
elif "Windows" in platform.platform():
    running_on_windows = True
elif platform.node().startswith("fv-az"):
    running_on_azure = True
else:
    raise Exception("Unknown platform: " + platform.platform())

DUMMY_MAC = "d01111111111"


# Get the MAC address of the device
def get_mac_address(interface_name: str) -> str:
    if not running_on_rpi:
        # get_mac_address is only supported on rpi; we dummy this out elsewhere
        return DUMMY_MAC
    else:
        addrs = psutil.net_if_addrs()
        if interface_name in addrs:
            for addr in addrs[interface_name]:
                if addr.family == psutil.AF_LINK:
                    return str(addr.address)
        return ""


my_mac = get_mac_address("wlan0")
assert len(my_mac) > 0, f"Failed to get MAC address for wlan0 on {platform.platform()}"
my_device_id = my_mac.replace(":", "")


############################################################################################
#
# Platform-dependent directory structure
#
############################################################################################
if running_on_windows:
    # Set paths for development mode where we're running everything locally on a laptop
    HOME_DIR: Path = Path.home()
    CODE_DIR: Path = Path(__file__).parent.parent.parent.parent
    SC_CODE_DIR: Path = CODE_DIR / "sensor_core"
    CFG_DIR: Path = HOME_DIR / ".sensor_core"
    ROOT_WORKING_DIR: Path = HOME_DIR / "sensor_core"
    assert HOME_DIR is not None, f"No 'code' directory found in path {Path.cwd()}"

elif running_on_rpi:
    # Check we're not running in the root context
    assert Path.cwd() != Path("/"), f"Running in root context: {Path.cwd()}"
    HOME_DIR = Path.home()
    CODE_DIR = Path(__file__).parent.parent.parent.parent
    SC_CODE_DIR = CODE_DIR / "sensor_core"
    CFG_DIR = HOME_DIR / ".sensor_core"  # In the base user directory on the RPi
    ROOT_WORKING_DIR = Path("/sensor_core")  # We always create a /sensor_core directory on the RPi

elif running_on_linux:
    # This is Docker on Linux
    HOME_DIR = Path("/app")
    CODE_DIR = Path("/app")
    SC_CODE_DIR = Path("/app")
    CFG_DIR = Path("/run/secrets")
    ROOT_WORKING_DIR = Path("/sensor_core")
else:
    raise Exception("Unknown platform: " + platform.platform())

FLAGS_DIR: Path = CFG_DIR / "flags"  # For the flag files
RESOURCES_DIR: Path = SC_CODE_DIR / "sensors" / "resources"
TMP_DIR: Path = ROOT_WORKING_DIR / "tmp"
LOG_DIR: Path = ROOT_WORKING_DIR / "logs"
TEST_DIR: Path = SC_CODE_DIR / "test"
SCRIPTS_DIR: Path = SC_CODE_DIR / "scripts"  # For the shell scripts

###########################################################################################
# SensorCore uses 3 directories on the edge device:
# - EDGE_PROCESSING_DIR for recordings that need to be processed
# - EDGE_STAGING_DIR for open journal files that are storing data output by processing
# - EDGE_UPLOAD_DIR for files that are ready for upload, including closed journals
###########################################################################################
EDGE_PROCESSING_DIR = ROOT_WORKING_DIR / "processing"  # Awaiting DP processing
EDGE_STAGING_DIR = ROOT_WORKING_DIR / "staging"  # Journals awaiting flush
EDGE_UPLOAD_DIR = ROOT_WORKING_DIR / "upload"  # Any file awaiting upload
ETL_UNZIP_DIR = ROOT_WORKING_DIR / "unzip"  # Where zip files are downloaded to
ETL_PROCESSING_DIR = ROOT_WORKING_DIR / "processing"  # Awaiting ETL DP processing
ETL_ARCHIVE_DIR = ROOT_WORKING_DIR / "etl_archive"  # Awaiting archive by Datastream
dirs = [
    FLAGS_DIR,
    RESOURCES_DIR,
    TMP_DIR,
    LOG_DIR,
    TEST_DIR,
    SCRIPTS_DIR,
    EDGE_PROCESSING_DIR,
    EDGE_STAGING_DIR,
    EDGE_UPLOAD_DIR,
    ETL_UNZIP_DIR,
    ETL_PROCESSING_DIR,
    ETL_ARCHIVE_DIR,
]
for d in dirs:
    if not d.exists():
        d.mkdir(parents=True, exist_ok=True)

KEYS_FILE = CFG_DIR / "keys.env"
SYSTEM_CFG_FILE = CFG_DIR / "system.cfg"

############################################################################################
# Mode of operation
# Set by the EdgeOrchestrator or the ETL orchestrator
############################################################################################
class Mode(Enum):
    EDGE = "edge"
    ETL = "etl"

_mode = Mode.EDGE

def get_mode() -> Mode:
    return _mode

def set_mode(mode: Mode) -> None:
    global _mode
    _mode = mode

############################################################
# Flag files set in FLAGS_DIR
############################################################
# Used by BCLI to signal to VideoCapture to take a picture
TAKE_PICTURE_FLAG = FLAGS_DIR / "TAKE_PICTURE"

# Used by BCLI to signal to AudioCapture and VideoCapture to pause recording
PERMANENT_PAUSE_RECORDING_FLAG = FLAGS_DIR / "PAUSE_RECORDING_FLAG"

# Used by the CLI and SensorCore.py to start / stop SensorCore
STOP_SENSOR_CORE_FLAG = FLAGS_DIR / "STOP_SENSOR_CORE_FLAG"


# We use a dummy device for testing purposes and when no config is specified
DUMMY_DEVICE = DeviceCfg(device_id=DUMMY_MAC, name="DUMMY")
INVENTORY: dict[str, DeviceCfg] = {DUMMY_MAC: DUMMY_DEVICE}
my_device: DeviceCfg = DUMMY_DEVICE

################################################################################################
# Load the .env files
################################################################################################
try:

    def _load_keys() -> Optional[Keys | None]:
        if not KEYS_FILE.exists():
            print("#################################################################")
            print(f"# Keys file {KEYS_FILE} does not exist")
            print("#################################################################")
            return None

        try:
            # Create a new Keys class with the env_file set in the model_config
            keys = Keys(_env_file=KEYS_FILE, _env_file_encoding="utf-8")  # type: ignore
            if keys.cloud_storage_key == FAILED_TO_LOAD:
                print("#################################################################")
                print(f"# WARNING: cloud_storage_key not set in {KEYS_FILE}")
                print("#################################################################")
            return keys
        except Exception as e:
            print("#################################################################")
            print(f"Failed to load keys from {KEYS_FILE}: {e}")
            print("#################################################################")
            return None

    keys = _load_keys()

    ############################################################################################
    # Load inventory configuration
    #
    # The user sets configuratoin by calling SensorCore.configure() with the fully-qualified
    # class reference of a class that implements the get_inventory() method.
    # Once they've done this, we store the fully-qualified class reference persistently in
    # home/<user>/.sensor_core/sc_config.env so that we can reload the configuration on reboot.
    # We use pydantic_settings to load the .env file.
    ############################################################################################
    if SYSTEM_CFG_FILE.exists():
        localised_model_config = SettingsConfigDict(
            extra="ignore", env_file_encoding="utf-8", env_file=SYSTEM_CFG_FILE
        )
    else:
        print("#################################################################")
        print(f"# SC config file {SYSTEM_CFG_FILE} does not exist")
        print("#################################################################")
        localised_model_config = SettingsConfigDict()

    def _load_system_cfg() -> Optional[SystemCfg | None]:
        try:
            # Use the Keys class to load the configuration
            return SystemCfg()
        except Exception as e:
            print("#################################################################")
            print(f"Failed to load keys from {SYSTEM_CFG_FILE}: {e}")
            print("#################################################################")
            return None

    def _load_inventory(inventory_class_ref: str) -> Optional[dict[str, DeviceCfg]]:
        # Load the inventory by instantiating the Inventory class from the fully qualified class reference
        # and calling the get_inventory method
        inventory: dict[str, DeviceCfg] = {}
        if inventory_class_ref != FAILED_TO_LOAD:
            try:
                inventory_class_parts = inventory_class_ref.split(".")
                module_name = ".".join(inventory_class_parts[:-1])
                class_name = inventory_class_parts[-1]
                module = __import__(module_name, fromlist=[class_name])
                # The class ref must be to a class called Inventory with a method get_inventory()
                inventory = module.Inventory().get_inventory()
            except Exception as e:
                print("#################################################################")
                print(f"Failed to load Inventory class from {inventory_class_ref}: {e}")
                print("#################################################################")
        else:
            print("#################################################################")
            print("# WARNING: no inventory class set")
            print("#################################################################")
        return inventory

    system_cfg = _load_system_cfg()
    if (system_cfg is not None) and (system_cfg.inventory_class != FAILED_TO_LOAD):
        inventory = _load_inventory(system_cfg.inventory_class)
        if inventory is not None:
            INVENTORY = inventory
            print("Inventory loaded")

except Exception as e:
    print("#################################################################")
    print(f"# Config load failed: {e}")
    print("#################################################################")


if my_device_id in INVENTORY:
    my_device = INVENTORY[my_device_id]
else:
    print("########################################################################")
    print(f"# ERROR: Device {my_device_id} not found in inventory; using defaults")
    print("########################################################################")


def reload_inventory() -> dict[str, DeviceCfg]:
    global INVENTORY
    global system_cfg
    global my_device
    if system_cfg is None or system_cfg.inventory_class == FAILED_TO_LOAD:
        system_cfg = _load_system_cfg()

    if system_cfg:
        inventory = _load_inventory(system_cfg.inventory_class)
        if inventory is not None:
            INVENTORY = inventory
            if my_device_id in INVENTORY:
                my_device = INVENTORY[my_device_id]
        print(f"Inventory reloaded: found {len(INVENTORY)} devices")
    return INVENTORY


def update_my_device_id(new_device_id: str) -> None:
    """Function used in testing to change the device_id"""
    global my_device_id, my_device
    assert len(new_device_id) == 12, f"Invalid device_id: {new_device_id}"
    my_device_id = new_device_id
    if my_device_id in INVENTORY:
        my_device = INVENTORY[my_device_id]


def display_config(device_id: Optional[str] = None) -> str:
    if device_id is None:
        device_id = my_device_id

    # We want to display the my_device dataclass hierarchy of objects in a clean way
    display_str = f"Device: {device_id}\n"
    display_str += INVENTORY[device_id].display()
    return display_str



