########################################################
# Execute environment dependent setup
########################################################
import datetime as dt
import hashlib
import importlib
import logging
import os
import random
import shutil
import subprocess
import sys
import time
import zipfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from threading import Timer
from typing import Any, Generator, Optional

import pandas as pd
import psutil

from sensor_core import api
from sensor_core import configuration as root_cfg

# Configure pandas to use copy-on-write
# https://pandas.pydata.org/pandas-docs/stable/user_guide/copy_on_write.html#copy-on-write-enabling
pd.options.mode.copy_on_write = True


############################################################################################################
# OpenCV color constants (BGR format)
############################################################################################################
RED = (0, 0, 255)
GREEN = (0, 255, 0)
BLUE = (255, 0, 0)
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)
YELLOW = (0, 255, 255)
CYAN = (255, 255, 0)
MAGENTA = (255, 0, 255)


############################################################################################################
# Set up logging
#
# The logging level is a combination of:
#  - the value set in bee-ops.cfg
#  - the value requested by the calling module (default is INFO)
#
# There is update code at the end of this file that sets the level once we've loaded bee-ops.cfg
############################################################################################################
TEST_LOG = root_cfg.LOG_DIR.joinpath("test.log")
_DEFAULT_LOG: Optional[Path] = None
_LOG_LEVEL = logging.INFO


def set_log_level(level: int) -> None:
    global _LOG_LEVEL
    _LOG_LEVEL = level


def setup_logger(name: str, level: Optional[int]=None, filename: Optional[str|Path]=None) -> logging.Logger:
    global _DEFAULT_LOG
    if level is not None:
        set_log_level(level)
    if root_cfg.running_on_rpi:
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
                _DEFAULT_LOG = root_cfg.LOG_DIR.joinpath("default_" + api.utc_to_fname_str() + ".log")
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


logger = setup_logger("sensor_core")


############################################################
# Functions used by sensors.
############################################################


# Control function that pauses recording if we're running low on space
# or if a manual flag has been set to pause recording.
def pause_recording() -> bool:
    if failing_to_keep_up():
        return True

    # Check if the permanent pause flag has been set
    if os.path.exists(root_cfg.PERMANENT_PAUSE_RECORDING_FLAG):
        logger.info("Pausing recording due to permanent flag")
        return True

    return False


last_space_check = dt.datetime(1970, 1, 1)
last_check_outcome = False


def failing_to_keep_up()-> bool:
    """Function that allows us to back off intensive operations if we're running low on space"""
    # Cache the result for 30 seconds to avoid repeated disk checks
    global last_space_check, last_check_outcome
    now = api.utc_now()
    if (now - last_space_check).seconds < 30:
        return last_check_outcome
    else:
        last_space_check = now

    if root_cfg.running_on_rpi and psutil.disk_usage("/sensor_core").percent > 50:
        # Check if we're running low on space
        logger.warning(f"{RAISE_WARN()} Failing to keep up due to low disk space")
        last_check_outcome = True
    else:
        last_check_outcome = False

    return last_check_outcome


def is_sampling_period(
    sample_probability: float,
    period_len: int,
    timestamp: Optional[dt.datetime] = None,
    sampling_window: Optional[tuple[str, str]] = None,
) -> bool:
    """Used to synchronise sampling between sensors, the function returns True/False based
    on the time, periodicity of sampling and probability requested.

    In this context, "sampling" is not about recording normal periodic data (eg recording 180s
    of audio every hour, anaysing it for sounds, and saving numerical results).  Instead, it is
    about choosing to save a full sample of that audio *intact* to enable offline validation of
    the analysis process. In this case it is useful to have samples from all the different sensors
    at the same time, so that we can compare the results between audio & video, for example.

    It is assumed that sensors record data at a fixed periodicity (eg every 180s), aligned to
    the start of the day (00:00:00).  This segments the day into a fixed number of periods.
    The number of segments that should be sampled is a function of the sample_probability.

    Sensors that want to synchronise their sampling can call this function to determine if
    they should save a sample at a specified time.  The outcome is randomly distributed but
    deterministic so that any sensor calling with the same periodicity and sample_probability
    will get the same answer for a given sampling period.

    Parameters:
    ----------
    sample_probability: float
        The probability of sampling in a given period.  This is a float between 0 and 1.
    period_len: int
        The length of the sampling period in seconds.  This should be a factor of 86400.
    timestamp: datetime
        The timestamp to check for sampling. api.utc_now() if not specified.
    sampling_window: tuple(datetime, datetime)
        The start and end of the sampling window.  If the timestamp is outside this window, return False.
        Useful for sensors that only sample during daylight hours.

    Returns:
    --------
    bool
        True if the sensor should sample at this time, False otherwise.
    """

    if timestamp is None:
        timestamp = api.utc_now()

    # Check if the timestamp is within the sampling window
    if sampling_window is not None:
        # Convert the sampling_window elements from "HH:MM" to a datetime object
        start_time = datetime.strptime(sampling_window[0], "%H:%M")
        end_time = datetime.strptime(sampling_window[1], "%H:%M")
        timestamp_time = timestamp.time()
        if not start_time.time() <= timestamp_time <= end_time.time():
            return False

    # Calculate the period number for the timestamp
    period_num = (timestamp.hour * 3600 + timestamp.minute * 60 + timestamp.second) // period_len

    # Seed the generator so that it is deterministic based on today's date and the period_num.
    random.seed(str(timestamp.date()) + str(period_num))

    if random.random() < sample_probability:
        sample_this_period = True
    else:
        sample_this_period = False

    return sample_this_period


############################################################
# Wrapper to enable easy manipulation of error logging
# This prefix is then parsed by IOT_ETL.
#
# ETL_ERRORv1_[CUSTOMER]_[MAC_ADDRESS]: [ERROR_MESSAGE]
# ETL_ERRORv2_[MAC_ADDRESS]: [ERROR_MESSAGE]
############################################################
def RAISE_WARN() -> str:
    return f"{api.RAISE_WARN_TAG}_{root_cfg.my_device_id}: "


############################################################
# Access the DUA persistent key value store
############################################################
def read_persistent_key_value(key: str) -> str:
    value = str(
        run_cmd(
            f"{root_cfg.SC_CODE_DIR / 'device' / 'kv_store.sh'} read_persistent_key_value {key}",
            ignore_errors=True,
        )
    )
    return value


def record_persistent_key_value(key: str, value: str) -> None:
    run_cmd(
        f"{root_cfg.SC_CODE_DIR / 'device' / 'kv_store.sh'} record_persistent_key_value {key} {value}",
        ignore_errors=True,
    )


############################################################
# Run a linux command and return the output, or throw an exception on bad return code
############################################################
def run_cmd(cmd: str, ignore_errors: bool=False, grep_strs: Optional[list[str]]=None) -> str:
    if root_cfg.running_on_windows:
        assert ignore_errors, "run_cmd is not supported on Windows"

    # We don't support pipes for security reasons; call the command multiple times if needed
    # if "|" in cmd:
    #    raise Exception(RAISE_WARN() + "Pipes not supported in run_cmd: " + cmd)

    # Decompose the command into its args
    # We want to keep arguments in '' or "" together
    # eg we split "grep -E "test.*tset"" into ['grep', '-E', 'test.*tset']
    # args = shlex.split(cmd)
    # logger.debug("Running command: " + str(args))

    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out, err = p.communicate()

        if p.returncode != 0:
            if ignore_errors:
                logger.info("Ignoring failure running command: " + cmd + " Err output: " + str(err))
                return ""
            else:
                raise Exception(RAISE_WARN() + "Error running command: " + cmd + " Error: " + str(err))

        # Return lines that contain all of the entries in grep_strs
        output = out.decode("utf-8").strip()
        if grep_strs is not None:
            for grep_str in grep_strs:
                output = "\n".join([x for x in output.split("\n") if grep_str in x])
            
        return output

    except FileNotFoundError as e:
        logger.error(RAISE_WARN() + "Command not found: " + cmd)
        if ignore_errors:
            return ""
        else:
            raise e


# Get entries from the journalctl log
def save_journald_log_entries(output_file_name: Path, grep_str: str="", since_minutes: int=31) -> None:
    if root_cfg.running_on_windows:
        logger.warning("save_journald_log_entries not supported on Windows")
    else:
        import systemd.journal  # type: ignore

    # Calculate the start time for entries
    start_time = api.utc_now() - dt.timedelta(minutes=since_minutes)

    # Create a journal reader
    j = systemd.journal.Reader()
    j.this_boot()  # Optional: only entries from the current boot
    j.log_level(systemd.journal.LOG_INFO)  # Equivalent to --priority=6

    # Set the time range for entries
    j.seek_realtime(start_time)

    # Filter by log prefix (case-insensitive)
    j.add_match(MESSAGE=grep_str)

    # Open the log file for writing
    with open(output_file_name, "w") as log_file:
        # Read and process entries
        for entry in j:
            # Format the entry as 'short-iso-precise' equivalent
            timestamp = entry["__REALTIME_TIMESTAMP"].isoformat()
            message = entry["MESSAGE"]
            # Write to file
            log_file.write(f"{timestamp} {message}\n")


############################################################
# Timer class that repeats
############################################################
class RepeatTimer(Timer):
    def run(self) -> None:
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


############################################################
# Compute MD5 hash locally
# Used to compare whether files are the same
############################################################
def compute_local_md5(file_path: str) -> str:
    if not os.path.exists(file_path):
        return ""

    with open(file_path, "rb") as file:
        md5_hash = hashlib.md5(usedforsecurity=False)
        while chunk := file.read(8192):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


############################################################
# Instantiate an object from a class name
# Pass in the provided arguments on instantiation
############################################################
def get_class_instance(class_path: str, *args: Any, **kwargs: Any) -> Any:
    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    instance = cls(*args, **kwargs)
    return instance


def get_current_user() -> str:
    """Get the current user name."""
    if root_cfg.running_on_windows:
        try:
            return os.getlogin()
        except Exception as e:
            return f"Error retrieving user: {e}"    
    else:
        try:
            import pwd
            return pwd.getpwuid(os.getuid()).pw_name # type: ignore
        except Exception as e:
            return f"Error retrieving user: {e}"


############################################################
# Utility to determine if a process is already running
#
# Looks for process_name in the list of running processes
# and confirms that the process ID is not the current process ID.
############################################################
def is_already_running(process_name: str) -> bool:
    if root_cfg.running_on_windows:
        logger.warning("is_already_running not supported on Windows")
        return False

    for proc in psutil.process_iter():
        try:
            # Check if process name contains the given name string.
            if process_name in str(proc.cmdline()):
                # Check that the process ID is not our process ID
                if proc.pid != os.getpid():
                    print("Process already running:" + str(proc.cmdline()) + " PID:" + str(proc.pid))
                    return True

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False


############################################################
# Utility to check what processes are running.
#
# All the interesting ones are python ones and we can match a module string
# eg core.device_manager
#
# This function discards all lines and all parts of the line that don't match the module string
# It builds up a set of the module strings, discarding duplicates
###########################################################
def check_running_processes(search_string: str="core") -> set:
    if root_cfg.running_on_windows:
        logger.warning("check_running_processes not supported on Windows")
        return set()

    processes = set()
    for proc in psutil.process_iter():
        try:
            for line in proc.cmdline():
                # Parse the line into the space-separated segments
                segments = line.split(" ")
                # Find the segment that contains the search string
                for segment in segments:
                    if search_string in segment:
                        processes.add(segment)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return processes


###########################################################
# Utility to extract a zip file to a directory but flattening all hierarchies
###########################################################
def extract_zip_to_flat(zip_path: Path, dest_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for member in zip_ref.namelist():
            # Extract only the specific file
            filename = os.path.basename(member)

            with zip_ref.open(member) as source, open(dest_path.joinpath(filename), "wb") as target:
                shutil.copyfileobj(source, target)


############################################################
# List all files in a directory that match a search string and are older than the specified age
############################################################
def list_files_older_than(search_string: Path, age_in_seconds: float) -> list[Path]:
    now = time.time()

    # List files matching the search string
    all_files = list(search_string.parent.glob(search_string.name))

    # Now check the age of each file
    old_files: list[Path] = []
    for file in all_files:
        if now - file.stat().st_mtime > age_in_seconds:
            old_files.append(file)

    # Remove directories from the list
    old_files = [x for x in old_files if not x.is_dir()]

    return old_files


def list_all_large_dirs(path: str, recursion: int=0) -> int:
    """Utility function that walks the directory tree and logs all directories using more than 1GB of space"""
    total = 0
    if recursion == 0:
        print("Large directories:")
    recursion += 1
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            try:
                dir_size = list_all_large_dirs(entry.path, recursion)
            except PermissionError:
                dir_size = 0
            if dir_size > 2**32:
                recursion_padding = "-" * recursion
                print(f"{recursion_padding}{entry.path!s} - {round(dir_size / 2**30, 1)}Gb")
            total += dir_size
        else:
            total += entry.stat(follow_symlinks=False).st_size
    return total


############################################################
# Convert a file from H264 to MP4 format
############################################################
def convert_h264_to_mp4(src_file: Path, dst_file: Path) -> None:
    # Use ffmpeg to convert H264 to MP4 while maintaining image quality
    command = [
        "ffmpeg",
        "-y",  # Overwrite the output file if it exists
        "-i",
        str(src_file),
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-preset",
        "superfast",
        "-crf",
        "18",
        str(dst_file),
    ]
    subprocess.run(command, check=True)


############################################################################################################
# Update the logging to reflect the logging level requested in the cfg file
#
# We set the logging level to the lowest of what's set in cfg and the level requested in the code.
############################################################################################################
# if root_cfg.running_on_rpi:
cfg_level = root_cfg.my_device.log_level
level = min(cfg_level, _LOG_LEVEL)
set_log_level(level)
logger.info(f"Setting log level from {_LOG_LEVEL!s} to {level!s}")


@contextmanager
def disable_console_logging(logger_name: str) -> Generator[Any, Any, Any]:
    """
    Temporarily disable console logging for the specified logger.
    We use in the CLI to avoid interspersing log output with the output of the command.

    Args:
        logger_name: The name of the logger to modify.
    """
    logger = logging.getLogger(logger_name)
    original_handlers = logger.handlers[:]  # Save the original handlers

    # Remove console handlers
    logger.handlers = [h for h in logger.handlers if not isinstance(h, logging.StreamHandler)]

    try:
        yield  # Allow the code block to execute
    finally:
        logger.handlers = original_handlers  # Restore original handlers
