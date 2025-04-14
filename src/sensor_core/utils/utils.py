########################################################
# Execute environment dependent setup
########################################################
import datetime as dt
import hashlib
import importlib
import logging
import os
import random
import shlex
import shutil
import subprocess
import time
import zipfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from threading import Timer
from typing import Any, Generator, Optional
from zoneinfo import ZoneInfo

import cv2
import numpy as np
import pandas as pd
import psutil

from sensor_core import api
from sensor_core import configuration as root_cfg

# Configure pandas to use copy-on-write
# https://pandas.pydata.org/pandas-docs/stable/user_guide/copy_on_write.html#copy-on-write-enabling
pd.options.mode.copy_on_write = True

logger = root_cfg.setup_logger("sensor_core")

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


last_space_check = dt.datetime(1970, 1, 1, tzinfo=ZoneInfo("UTC"))
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
        logger.warning(f"{root_cfg.RAISE_WARN()} Failing to keep up due to low disk space")
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
# Run a system command and return the output, or throw an exception on bad return code
############################################################
def run_cmd(cmd: str, ignore_errors: bool=False, grep_strs: Optional[list[str]]=None) -> str:
    """Run a system command and return the output, or throw an exception on bad return code.

    Parameters:
    ----------
    cmd: str
        The command to run.  This should be a string that can be passed to the shell.
    ignore_errors: bool
        If True, ignore errors and return an empty string.  If False, raise an exception on error.
    grep_strs: list[str]
        A list of strings to grep for in the output.  If None, return the full output.
        If not None, return only the lines that contain all of the strings in the list.

    Returns:
    -------
    str
        The output of the command.  If ignore_errors is True, return an empty string on error.
        If grep_strs is not None, return only the lines that contain all of the strings in the list.

    Raises:
    ------
    Exception
        If the command fails and ignore_errors is False, raise an exception with the error message.
        
    """
    if root_cfg.running_on_windows:
        if root_cfg.TEST_MODE:
            # In test mode, we may stub out a command so that we can run a more realistic test scenario.
            return run_cmd_test_stub(cmd, ignore_errors, grep_strs)
        else:
            assert ignore_errors, "run_cmd is not fully supported on Windows"


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
                raise Exception(f"{root_cfg.RAISE_WARN()}Error running command: {cmd}, Error: {err!s}")

        # Return lines that contain all of the entries in grep_strs
        output = out.decode("utf-8").strip()
        if grep_strs is not None:
            for grep_str in grep_strs:
                output = "\n".join([x for x in output.split("\n") if grep_str in x])
            
        return output

    except FileNotFoundError as e:
        logger.error(root_cfg.RAISE_WARN() + "Command not found: " + cmd)
        if ignore_errors:
            return ""
        else:
            raise e

def run_cmd_test_stub(cmd: str, ignore_errors: bool=False, grep_strs: Optional[list[str]]=None) -> str:
    """For testing purposes, we emulate certain basic Linux sensor commands so that we can run more 
     realistic test scenarios on Windows.

    We currently emulate:
    - rpicam-vid
    - arecord

    Parameters:
    ----------
    cmd: str
        The command to run.  This should be a string that can be passed to the shell.
    ignore_errors: bool
        If True, ignore errors and return an empty string.  If False, raise an exception on error.
    grep_strs: list[str]
        A list of strings to grep for in the output.  If None, return the full output.
        If not None, return only the lines that contain all of the strings in the list.

    Returns:
    -------
    str
        The output of the command.  If ignore_errors is True, return an empty string on error.
        If grep_strs is not None, return only the lines that contain all of the strings in the list.

    """
    # Parse the command to get the command name and arguments
    # We want to keep arguments in '' or "" together
    # eg we split "grep -E "test.*tset"" into ['grep', '-E', 'test.*tset']
    args = shlex.split(cmd, posix=False)

    if cmd.startswith("rpicam-vid"):
        # Emulate the rpicam-vid command
        # We expect commands like:
        #  "rpicam-vid --framerate 4 --width 640 --height 480 -o FILENAME -t 180000 -v 0"
        # We create a video file with:
        # - filename taken from the -o parameter
        # - duration taken from the -t parameter (in milliseconds)
        # - framerate taken from the --framerate parameter
        # - width taken from the --width parameter
        # - height taken from the --height parameter
        if args.index("-o") == -1 or args.index("-t") == -1:
            raise ValueError("Missing required arguments in command: " + cmd)
        filename = args[args.index("-o") + 1]
        suffix = filename.split(".")[-1]
        duration = int(args[args.index("-t") + 1]) / 1000  # Convert to seconds
        # We divide duration by 10 to get a 10x speedup for testing purposes
        duration = int(duration / 10)

        if "--framerate" not in args:
            framerate = 30  # Default framerate
        else:
            framerate = int(args[args.index("--framerate") + 1])
        if "--width" not in args:
            width = 640
        else:
            width = int(args[args.index("--width") + 1])
        if "--height" not in args:
            height = 480
        else:
            height = int(args[args.index("--height") + 1])

        # Use OpenCV to create a dummy video file
        if suffix == "h264":
            fourcc = cv2.VideoWriter.fourcc(*"h264")
        elif suffix == "mp4":
            fourcc = cv2.VideoWriter.fourcc(*"mp4v")
        else:
            raise ValueError("Unsupported video format: " + suffix)

        out = cv2.VideoWriter(filename, fourcc, framerate, (width, height))
        num_frames = int(framerate * duration)
        for i in range(num_frames):
            # Create a dummy frame (e.g., a solid color or gradient)
            frame = np.zeros((height, width, 3), dtype=np.uint8)
            frame[:] = (i % 256, (i * 2) % 256, (i * 3) % 256)  # Example gradient
            out.write(frame)

        # Release the VideoWriter
        out.release()
        time.sleep(duration)
        return f"rpicam-vid command emulated successfully, created {filename}"
        
    elif cmd.startswith("arecord"):
        # Emulate the arecord command
        # This is a simple emulation that just returns a success code and a message.
        # In a real scenario, we would run the command and return the output.
        return "arecord command emulated successfully"
    return "Command not run on windows: " + cmd


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
