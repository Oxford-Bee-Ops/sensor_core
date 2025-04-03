import platform
from enum import Enum
from pathlib import Path
from typing import Optional

import psutil
from pydantic_settings import SettingsConfigDict

from sensor_core.config_objects import FAILED_TO_LOAD, DeviceCfg, Inventory, Keys, SystemCfg

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
# Load the keys.env file
################################################################################################
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

def check_keys() -> tuple[bool, str]:
    """Check the keys.env file exists and has loaded; provided a helpful display string if not."""
    CFG_DIR.mkdir(parents=True, exist_ok=True)
    success = False
    error = ""
    if not KEYS_FILE.exists():
        error = (f"Keys file {KEYS_FILE} does not exist. "
                    f"Please create it and set the 'cloud_storage_key' key.")
    elif (KEYS_FILE.exists()) and (
        (keys is None
        ) or (keys.cloud_storage_key is None
        ) or (keys.cloud_storage_key == FAILED_TO_LOAD)
        ):
        error = f"Keys file {KEYS_FILE} exists but 'cloud_storage_key' key not set."
    else:
        success = True
        error = "Keys loaded successfully."

    return success, error


############################################################################################
# Load system.cfg configuration
############################################################################################
if SYSTEM_CFG_FILE.exists():
    localised_model_config = SettingsConfigDict(
        extra="ignore", env_file_encoding="utf-8", env_file=SYSTEM_CFG_FILE
    )
else:
    print("#################################################################")
    print(f"# {SYSTEM_CFG_FILE} does not exist")
    print("#################################################################")
    localised_model_config = SettingsConfigDict()

def _load_system_cfg() -> Optional[SystemCfg | None]:
    try:
        # Use the Keys class to load the configuration
        return SystemCfg()
    except Exception as e:
        print("#################################################################")
        print(f"Failed to load {SYSTEM_CFG_FILE}: {e}")
        print("#################################################################")
        return None

system_cfg = _load_system_cfg()

#############################################################################################
# Load the inventory from the config python file
##############################################################################################
def load_inventory(inventory_class_ref: Inventory) -> list[DeviceCfg]:
    """Load the inventory by calling get_inventory() on the class provided.
    Does not set the inventory in SensorCore - call set_inventory() for that.
    """
    inventory: list[DeviceCfg] = []
    if inventory_class_ref is None:
        print("#################################################################")
        print("# WARNING: no inventory class set")
        print("#################################################################")
    else:
        try:
            # The class ref must be to a class called Inventory with a method get_inventory()
            inventory = inventory_class_ref.get_inventory() # type: ignore
        except Exception as e:
            print("#################################################################")
            print(f"Failed to load Inventory class from {inventory_class_ref}: {e}")
            print("#################################################################")

    return inventory

def set_inventory(inventory_class: Inventory) -> dict[str, DeviceCfg]:
    """Reload the inventory from the config file.
    It is assumed that the config has already been validated by SensorCore.configure().
    """
    global INVENTORY
    global system_cfg
    global my_device

    inventory = load_inventory(inventory_class)
    for device in inventory:
        INVENTORY[device.device_id] = device
    if my_device_id in INVENTORY:
        my_device = INVENTORY[my_device_id]
    print(f"Inventory reloaded: found {len(INVENTORY)} devices")

    return INVENTORY

def check_inventory_loaded() -> bool:
    """Check if the inventory has been loaded.
    This is used in testing to check if the inventory has been loaded.
    """
    global INVENTORY

    # If we have not loaded the inventory yet, it will still be set to the DUMMY_DEVICE
    if my_device == DUMMY_DEVICE:
        return False

    # Check if the inventory is empty
    return len(INVENTORY) > 0

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



