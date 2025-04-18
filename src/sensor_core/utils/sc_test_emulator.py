###################################################################################################
# The test harness enables thorough testing of the sensor code without RPi hardware.
# It emulates / intercepts:
# - the run_cmd function to enable injection of example recordings that then flow through the system
# - the CloudConnnector to store results locally (this is done via the LocalCloudConnector)
#
# It provides utilities to interrogate the local output and check that the expected data is present.
#
# To use the test harness:
# - set the TEST_MODE flag to MODE.TEST in the root configuration file.
####################################################################################################
import shlex
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import cv2
import numpy as np

from sensor_core import configuration as root_cfg
from sensor_core.cloud_connector import CloudConnector, LocalCloudConnector, local_cloud

logger = root_cfg.setup_logger("sensor_core")

@dataclass
class ScTestRecording():
    cmd_prefix: str
    recordings: list[Path]

class ScEmulator():
    """The test harness enables thorough testing of the sensor code without RPi hardware."""
    _instance = None

    def __init__(self):
        self.previous_recordings_index: int = 0
        self.recordings_saved: dict[str, int] = {}
        self.recording_cap: int = -1
        root_cfg.TEST_MODE = root_cfg.MODE.TEST
        cc = CloudConnector().get_instance()
        if not isinstance(cc, LocalCloudConnector):
            raise TypeError("Expected LocalCloudConnector, but got a different type.")
        cc.clear_local_cloud()

    @staticmethod
    def get_instance() -> "ScEmulator":
        if ScEmulator._instance is None:
            ScEmulator._instance = ScEmulator()
        return ScEmulator._instance

    ##################################################################################################
    # Test harness functions
    ##################################################################################################
    def set_recordings(self, recordings: list[ScTestRecording]) -> None:
        """Set the recordings to be used for testing.

        Call this function to specify which recording should be returned in which conditions."""
        self.recordings = recordings

    def set_recording_cap(self, cap: int) -> None:
        """Set the maximum number of recordings to be saved."""
        self.recording_cap = cap

    def assert_records(self, container: str, expected: dict[str, int]) -> None:
        """Assert that the expected number of files exist.

        Parameters:
        ----------
        expected: dict[str, int]
            A dictionary with the expected number of recordings for each file name prefix.
            The keys are the prefixes of the file names.
            The values are the expected number of recordings.
        """
        for file_prefix, count in expected.items():
            files = list((local_cloud /container).glob(file_prefix))
            assert len(files) == count, (
                f"Expected {count} files with prefix {file_prefix}, "
                f"but found {len(files)} files."
            )

    ##################################################################################################
    # Internal implementation functions
    ##################################################################################################
    def _match_recording(self, cmd: str) -> Optional[list[Path]]:
        """Check if the command matches any of the recordings.

        Parameters:
        ----------
        cmd: str
            The command to run.  This should be a string that can be passed to the shell.

        Returns:
        -------
        Path | None
            The path to the recording file if a match is found, None otherwise.
        """
        for recording in self.recordings:
            if cmd.startswith(recording.cmd_prefix):
                return recording.recordings
        return None

    def ok_to_save_recording(self, ds_id) -> bool:
        if self.recording_cap != -1:
            previous_recordings = self.recordings_saved.get(ds_id, 0)
            self.recordings_saved[ds_id] = previous_recordings + 1
            return previous_recordings < self.recording_cap
        else:
            return True

    #################################################################################################
    # Sensor command emulation
    #################################################################################################
    def run_cmd_test_stub(self, cmd: str, 
                          ignore_errors: bool=False, 
                          grep_strs: Optional[list[str]]=None) -> str:
        """For testing purposes, we emulate certain basic Linux sensor commands so that we can run more 
        realistic test scenarios on Windows.

        We currently emulate:
        - rpicam-vid

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
        if cmd.startswith("rpicam-vid"):
            self.emulate_rpicam_vid(cmd, ignore_errors, grep_strs)
            
        elif cmd.startswith("arecord"):
            # Emulate the arecord command
            # This is a simple emulation that just returns a success code and a message.
            # In a real scenario, we would run the command and return the output.
            return "arecord command emulated successfully"
        return "Command not run on windows: " + cmd

    def emulate_rpicam_vid(self, cmd: str, 
                           ignore_errors: bool=False, 
                           grep_strs: Optional[list[str]]=None) -> str:
        # Emulate the rpicam-vid command
        # We expect commands like:
        #  "rpicam-vid --framerate 4 --width 640 --height 480 -o FILENAME -t 180000 -v 0"
        #
        # We try to find a matching recording to provide. 
        # 
        # If we fail, we create a video file with:
        # - filename taken from the -o parameter
        # - duration taken from the -t parameter (in milliseconds)
        # - framerate taken from the --framerate parameter
        # - width taken from the --width parameter
        # - height taken from the --height parameter
        args = shlex.split(cmd, posix=False)
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

        # See if we have a matching cmd in the recordings list
        # We need to replace the filename with FILENAME
        match_cmd = cmd.replace(filename, "FILENAME")
        if "--camera" in match_cmd:
            parts = match_cmd.split("--camera")
            match_cmd = parts[0] + " SENSOR_INDEX" + parts[1][2:]
        recordings = self._match_recording(cmd)
        logger.info(f"Found match command {recordings is not None} "
                    f"for match command: {match_cmd}")

        if recordings:
            # We have a recording so save that with the appropriate filename
            recording = recordings[self.previous_recordings_index]
            self.previous_recordings_index += 1
            self.previous_recordings_index %= len(recordings)
            shutil.copy(recording, filename)
        else:
            # No recording.  Create a dummy video file.
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

        # Sleep for the duration of the video to simulate recording time.
        time.sleep(duration)
        return f"rpicam-vid command emulated successfully, created {filename}"