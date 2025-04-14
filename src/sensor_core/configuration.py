import logging
import platform
import sys
from enum import Enum
from pathlib import Path
from typing import Optional

import psutil
from pydantic_settings import SettingsConfigDict

from sensor_core import api
from sensor_core.config_objects import FAILED_TO_LOAD, DeviceCfg, Keys, SystemCfg
from sensor_core.utils import dc

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
    CODE_DIR = Path(__file__).parent.parent
    SC_CODE_DIR = CODE_DIR / "sensor_core"
    CFG_DIR = HOME_DIR / ".sensor_core"  # In the base user directory on the RPi
    ROOT_WORKING_DIR = Path("/sensor_core")  # We always create a /sensor_core directory on the RPi
    dc.create_root_working_dir(ROOT_WORKING_DIR)

elif running_on_linux:
    # This is Docker on Linux
    HOME_DIR = Path("/app")
    CODE_DIR = Path("/app")
    SC_CODE_DIR = Path("/app")
    CFG_DIR = Path("/run/secrets")
    ROOT_WORKING_DIR = Path("/sensor_core")
    dc.create_root_working_dir(ROOT_WORKING_DIR)
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

############################################################################################################
# Set up logging
#
# The logging level is a combination of:
#  - the value set in bee-ops.cfg
#  - the value requested by the calling module (default is INFO)
#
# There is update code at the end of this file that sets the level once we've loaded bee-ops.cfg
############################################################################################################
TEST_LOG = LOG_DIR.joinpath("test.log")
_DEFAULT_LOG: Optional[Path] = None
_LOG_LEVEL = logging.INFO


def set_log_level(level: int) -> None:
    global _LOG_LEVEL
    _LOG_LEVEL = level


def setup_logger(name: str, level: Optional[int]=None, filename: Optional[str|Path]=None) -> logging.Logger:
    global _DEFAULT_LOG
    if level is not None:
        set_log_level(level)
    if running_on_rpi:
        from systemd.journal import JournalHandler as JournaldLogHandler  # type: ignore

        logger = logging.getLogger(name)
        logger.setLevel(_LOG_LEVEL)
        if len(logger.handlers) == 0:
            handler = JournaldLogHandler()
            handler.setFormatter(logging.Formatter("%(name)s %(levelname)-6s %(message)s"))
            logger.addHandler(handler)
    else:  # elif root_cfg.running_on_windows
        logger = logging.getLogger(name)
        logger.setLevel(_LOG_LEVEL)
        formatter = logging.Formatter("%(asctime)-15s %(name)-6s %(levelname)-6s - %(message)s")

        # Create a console handler and set the log level
        # Check if we've already added a console handler
        if len(logger.handlers) == 0:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(_LOG_LEVEL)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        # By default, we always want to log to a file
        # Check whether there are any FileHander handlers already
        file_handler_count = 0
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler):
                file_handler_count += 1

        if filename is None:
            if _DEFAULT_LOG is None:
                _DEFAULT_LOG = LOG_DIR.joinpath("default_" + api.utc_to_fname_str() + ".log")
            if not _DEFAULT_LOG.parent.exists():
                _DEFAULT_LOG.parent.mkdir(parents=True, exist_ok=True)
            if file_handler_count == 0:
                handler = logging.FileHandler(_DEFAULT_LOG)
                handler.setFormatter(formatter)
                logger.addHandler(handler)
                print(f"Logging {name} to default file: {_DEFAULT_LOG} at level {_LOG_LEVEL}")
        # Limit to 2 file loggers
        elif file_handler_count <= 1:
            handler = logging.FileHandler(filename)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            print(f"Logging {name} to file: {filename} at level {_LOG_LEVEL}")

    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING)
    return logger

def RAISE_WARN() -> str:
    return f"{api.RAISE_WARN_TAG}_{my_device_id}: "

logger = setup_logger("sensor_core")

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
def _load_system_cfg() -> Optional[SystemCfg | None]:
    if not SYSTEM_CFG_FILE.exists():
        print("#################################################################")
        print(f"# {SYSTEM_CFG_FILE} does not exist")
        print("#################################################################")
        logger.error(f"{SYSTEM_CFG_FILE} does not exist")
        return SystemCfg()

    try:
        # Use the Keys class to load the configuration
        logger.info(f"Loading {SYSTEM_CFG_FILE}...")
        cfg = SystemCfg(_env_file=SYSTEM_CFG_FILE, _env_file_encoding="utf-8")  # type: ignore
        logger.info(f"my_git_repo_url={cfg.my_git_repo_url}")
        return cfg
    except Exception as e:
        print("#################################################################")
        print(f"Failed to load {SYSTEM_CFG_FILE}: {e}")
        print("#################################################################")
        logger.error(f"Failed to load {SYSTEM_CFG_FILE}: {e}")
        return SystemCfg()

system_cfg = _load_system_cfg()

#############################################################################################
# Store the provided inventory
##############################################################################################
def set_inventory(inventory: list[DeviceCfg]) -> dict[str, DeviceCfg]:
    """Reload the inventory from the config file.
    It is assumed that the config has already been validated by SensorCore.configure().
    """
    global INVENTORY
    global system_cfg
    global my_device

    for device in inventory:
        INVENTORY[device.device_id] = device
    if my_device_id in INVENTORY:
        my_device = INVENTORY[my_device_id]
        if (my_device.log_level != _LOG_LEVEL):
            logger.info(f"Setting log level from {_LOG_LEVEL!s} to {level!s}")
            set_log_level(my_device.log_level)
    else:
        logger.error(f"{RAISE_WARN()}Device ID {my_device_id} not found in inventory")
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

############################################################################################################
# Update the logging to reflect the logging level requested in the cfg file
#
# We set the logging level to the lowest of what's set in cfg and the level requested in the code.
############################################################################################################
# if root_cfg.running_on_rpi:
cfg_level = my_device.log_level
level = min(cfg_level, _LOG_LEVEL)
set_log_level(level)
logger.info(f"Setting log level from {_LOG_LEVEL!s} to {level!s}")


