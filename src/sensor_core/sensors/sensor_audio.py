############################################################################################################
# AudioSensor
#
# Called by SensorCore to record audio from USB microphones plugged into the Raspberry Pi.
#
# Most mics are attached on the outside of hives and record audio continuously if possible.
#
# We also support 1 mic per device being attached inside the hive - in which case we record audio snippets
# and upload them without processing on the device - because we want to do more complex analysis on the sound.
# We identify these in-hive mics using the "in_hive_mic_port" setting in bee-ops.cfg.
# Other than scheduling them less frequently, we treat them the same as the external mics; it is up to the
# post-processing (AudioProcessor) to handle them differently (by adding an INHIVE tag to the filename).
############################################################################################################
import os
from dataclasses import dataclass
from datetime import datetime
from time import sleep
from typing import ClassVar

from sensor_core import Sensor, SensorCfg, api
from sensor_core import configuration as root_cfg
from sensor_core.utils import file_naming, utils

logger = root_cfg.setup_logger("sensor_core")

AUDIO_SENSOR_STREAM_INDEX = 0

@dataclass
class AudioSensorCfg(SensorCfg):
    ############################################################
    # SensorCfg fields
    ############################################################
    # The type of sensor.
    sensor_type: api.SENSOR_TYPE = api.SENSOR_TYPE.USB
    sensor_model: str = "USBAudioSensor"
    # A human-readable description of the sensor model.
    description: str = "Default audio sensor"

    ############################################################
    # Custom fields
    ############################################################
    av_rec_seconds: int = 180
    microphones_installed: int = 0
    in_hive_mic_port: int = 0

############################################################################################################
# The AudioSensor class is used to manage the audio recording
############################################################################################################
class AudioSensor(Sensor):
    port_index_map: ClassVar[dict[int, int]] = {}

    # Constructor for the AudioSensor class
    #
    # We call get_pyaudio() to initialise the PyAudio instance - this may take a few retries and up to 30s
    # We do this in the constructor so that it blocks creation of other AudioSensor instances until it's done
    # This avoids us being multi-threaded in the initialisation of PyAudio, which can cause problems.
    # Once we have a PyAudio instance, we should be good from then on.
    def __init__(self, config: AudioSensorCfg):
        super().__init__(config)
        self.config = config
        self.port = self.config.sensor_index
        self.av_rec_seconds = self.config.av_rec_seconds
        self.num_devices = self.config.microphones_installed
        self.in_hive_mic_port = self.config.in_hive_mic_port

    ############################################################################################################
    # RPi can only handle 3 audio devices recording simultaneously.
    # This function manages the scheduling if there are more than 3 devices.
    # We do this based on the minutes in the hour and the port number.
    # The function also checks whether we have sufficient memory to record.
    # The function also handles in-hive mics differently, recording audio for brief periods.
    #
    # Rather than returning True/False, we return a sleep-for time, after which the caller should re-check.
    # If the sleep-for is 0s, the caller should proceed with recording.
    ############################################################################################################
    @staticmethod
    def ok_to_record(
        num_devices: int,
        in_hive_port: int,
        port: int,
        timestamp: datetime,
        av_rec_seconds: int,
    ) -> tuple[int, int]:
        """Used to schedule periods of recording so that capture data on a schedule that may be 
        sync'd between audio and video.

        Returns a tuple with:
            - the number of seconds to sleep before recording starts (so we don't clash with other devices)
            If the sleep-for is 0s, the caller should proceed with recording.
            - the number of seconds to record for before stopping (so we stop in time to stay in sync).
            This is only valid if the sleep-for is 0s.

        RPi4 can only handle 3 audio devices recording simultaneously.
        This function manages the scheduling if there are more than 3 devices.
        We do this based on the minutes in the hour and the port number.
        The function also checks whether we have sufficient memory to record.
        The function also handles in-hive mics differently, recording audio for brief periods.
        """
        # Default max sleep in seconds
        max_sleep = 1800
        schedule = None

        # Check memory and manual override flag
        if utils.pause_recording():
            logger.info("Audio recording paused; sleeping for " + str(max_sleep) + " seconds")
            return (max_sleep, 0)

        # Check scheduling based on number of devices and in_hive_port setting
        if in_hive_port == 0:
            if num_devices <= 3:
                schedule = {
                    1: [0, 3600],
                    2: [0, 3600],
                    3: [0, 3600],
                }
            elif num_devices <= 6:
                schedule = {
                    1: [0, 1800],
                    2: [0, 1800],
                    3: [1800, 3600],
                    4: [1800, 3600],
                    5: [0, 1800],
                    6: [1800, 3600],
                }
            elif num_devices <= 9:
                schedule = {
                    1: [0, 1200],
                    2: [0, 1200],
                    3: [0, 1200],
                    4: [1200, 2400],
                    5: [1200, 2400],
                    6: [1200, 2400],
                    7: [2400, 3600],
                    8: [2400, 3600],
                    9: [2400, 3600],
                }
        else:
            # For simplicity we assume that the in_hive_port is always port 4
            assert num_devices == 4, root_cfg.RAISE_WARN() + " in_hive_mic set but num_devices not equal to 4"
            assert in_hive_port == 4, root_cfg.RAISE_WARN() + " in_hive_port must be port 4"

            # If we have an in-hive mic, we only record for a short period at the beginning of each hour
            # And we just turn off the other mics for that period
            in_hive_end_time = av_rec_seconds * 2
            schedule = {
                1: [in_hive_end_time, 3600],
                2: [in_hive_end_time, 3600],
                3: [in_hive_end_time, 3600],
                4: [0, in_hive_end_time],
            }

        if schedule is None:
            raise Exception(
                f"{root_cfg.RAISE_WARN()}Unsupported number of audio devices={num_devices!s};"
                + f" in_hive_port={in_hive_port!s}"
            )

        # Get the current minute of the hour
        current_sec = (timestamp.minute * 60) + timestamp.second
        start_time = schedule[port][0]
        end_time = schedule[port][1]
        sleep_time = 0

        if current_sec < start_time:
            # We're before the start time: sleep until the start time
            sleep_time = start_time - current_sec
            record_for_duration = 0
        elif current_sec >= end_time:
            # We're after the end time: sleep until the next start time
            sleep_time = (3600 - current_sec) + start_time
            record_for_duration = 0
        else:
            # We're in the middle of the recording period: record!
            # We want to try and stay in sync with the video recording as much as possible so we record for
            # a period that ends at a multiple of the av_rec_seconds - hence subtract the modulus of the 
            # current time
            sleep_time = 0
            record_for_duration = av_rec_seconds - (current_sec % av_rec_seconds)

        # We set a max_sleep so we can interrupt the thread within a reasonable time
        sleep_time = min(sleep_time, max_sleep)

        logger.info(f"ok_to_record: sleep for {sleep_time!s}s, record for {record_for_duration!s}s")
        return (sleep_time, record_for_duration)

    ############################################################################################################
    # Function that maps from port number to card index
    ############################################################################################################
    @staticmethod
    def get_card_index_from_port(total_num_devices, port):
        # Find the right card index - this can change on reboot!
        #
        # *** RPI4 USB PORTS ***
        #
        # !Ports! | LEFT | RIGHT |
        # UPPER   |   3  |   1   |
        # LOWER   |   4  |   2   |
        #
        # We assume that the microphones are plugged into USB ports starting with port 1 (top right).
        # We expect the output to be of the form:
        #       /sys/devices/platform/soc/3f980000.usb/usb1/1-1/1-1.2/1-1.2:1.1/sound/card1/id
        #       /sys/devices/platform/scb/fd500000.pcie/pci0000:00/0000:00:00.0/0000:01:00.0/usb1/1-1/1-1.3/1-1.3:1.0/sound/card5/id # noqa
        # We spot the right card by finding the substring "1.<port>:1"
        #
        # To get more than 4 microphones, we plug a USB hub into port 4. In this case, we expect the output 
        # for ports 4+ to be of the form:
        #       /sys/devices/platform/scb/fd500000.pcie/pci0000:00/0000:00:00.0/0000:01:00.0/usb1/1-1/1-1.4/1-1.4.3/1-1.4.3:1.0/sound/card6/id # noqa
        # We spot the right card by finding the substring "1.4.<port>:1"
        # We only use the USB hub if we've specified more than 4 microphones in the config file
        #
        # *** RPI5 USB PORTS ***
        # On the Pi5, the USB ports are split between usb1 and usb3, so we create a map to get from port to 
        # device string
        #        | LEFT | RIGHT |
        # UPPER  |  3-1 |  1-2  |
        # LOWER  |  1-1 |  3-2  |
        # /sys/devices/platform/axi/1000120000.pcie/1f00300000.usb/xhci-hcd.1/usb3/3-1/3-1:1.0/sound/card4/id
        # /sys/devices/platform/axi/1000120000.pcie/1f00200000.usb/xhci-hcd.0/usb1/1-2/1-2:1.0/sound/card2/id
        # /sys/devices/platform/axi/1000120000.pcie/1f00200000.usb/xhci-hcd.0/usb1/1-1/1-1:1.0/sound/card5/id
        # /sys/devices/platform/axi/1000120000.pcie/1f00300000.usb/xhci-hcd.1/usb3/3-2/3-2:1.0/sound/card3/id
        if root_cfg.running_on_rpi5:
            assert total_num_devices <= 4, "ERROR: More than 4 microphones not supported on RPi5"
            port_map = {1: "1-2:1", 2: "3-2:1", 3: "3-1:1", 4: "1-1:1"}
            card_info = utils.run_cmd(
                "find /sys/devices/platform -name id",
                grep_strs=["usb", "sound", port_map[port]],
                ignore_errors=True,
            )
        else:
            if total_num_devices <= 4 or port < 4:
                card_info = utils.run_cmd(
                    "find /sys/devices/platform -name id",
                    grep_strs=["usb", "sound", "1." + str(port) + ":1"],
                    ignore_errors=True,
                )
            else:
                # In the case where we have more than 4 microphones, we need to use the USB hub
                # Port 4 is the first port on the USB hub, so will be 1.4.<offset_port=1>:1
                # Port 5 is the second port on the USB hub, so will be offset_port=2, etc
                offset_port = port - 3
                card_info = utils.run_cmd(
                    "find /sys/devices/platform -name id",
                    grep_strs=["usb", "sound", "1.4." + str(offset_port) + ":1"],
                    ignore_errors=True,
                )

        if "card" in card_info:
            card_index = int(card_info[card_info.find("card") + 4])
            logger.debug(
                "Found card_index=" + str(card_index) + " for port " + str(port) + "; card_info:" + card_info
            )
        else:
            logger.error(
                root_cfg.RAISE_WARN()
                + "No USB sound device plugged into port "
                + str(port)
                + "; card_info:"
                + card_info
                + "; exiting"
            )
            card_index = -1

        return card_index

    ############################################################################################################
    # Function that records audio on a loop, creating chunks of length defined in the config file
    # This is called from a separate thread and inherited from Thread
    ############################################################################################################
    def run(self):
        """Subclass of Sensor.run() to record audio from the microphone."""
        logger.info(f"Starting AudioSensor.run on {self.port}")

        try:
            chans = 1  # 1 channel
            dev_index = self.get_card_index_from_port(self.num_devices, self.port)
            samp_rate = 44100

            # This thread will run indefinitely
            while not self.stop_requested:
                logger.debug("Thread alive for port " + str(self.port) + "; " + str(self.port))
                time_to_sleep, length_to_record = AudioSensor.ok_to_record(
                    self.num_devices,
                    self.in_hive_mic_port,
                    self.port,
                    api.utc_now(),
                    self.av_rec_seconds,
                )

                # This inner loop will record audio in chunks
                first_fail = True
                while time_to_sleep == 0 and not self.stop_requested:
                    logger.debug("Next audio while on " + str(self.port))
                    recording_successful = True
                    wav_output_filename = file_naming.get_temporary_filename(api.FORMAT.WAV)
                    try:
                        arecord_cmd = (
                            f"arecord -D hw:{dev_index!s} -r {samp_rate!s} -c {chans!s}"
                            f" -f S16_LE -t wav -d {length_to_record!s} {wav_output_filename!s}"
                        )
                        logger.debug(str(arecord_cmd))
                        start_time = api.utc_now()
                        utils.run_cmd(arecord_cmd)
                    except Exception as e:
                        recording_successful = False

                        # The first time we fail, just sleep briefly before trying again
                        # on the assumption it was a temporary busy issue
                        if first_fail:
                            first_fail = False
                            logger.warning(
                                f"First fail exception occurred in arecord; e={e!s}",
                                exc_info=True,
                            )
                            sleep(2)
                        else:
                            # To avoid a tight loop, we sleep for length_to_record
                            logger.error(
                                f"{root_cfg.RAISE_WARN()}Repeat exception occurred in arecord; e={e!s}",
                                exc_info=True,
                            )
                            sleep(length_to_record)

                    # Only process if we successfully created the recording
                    if recording_successful:
                        first_fail = True
                        orig_file_size = os.path.getsize(wav_output_filename)
                        logger.debug(
                            f"Audio stream closed with size {orig_file_size} on port "
                            f"{self.config.sensor_index}"
                        )

                        # Let the wave file close, then change it's name to include the end timestamp
                        final_output_filename = self.save_recording(stream_index=AUDIO_SENSOR_STREAM_INDEX,
                                                                    temporary_file=wav_output_filename, 
                                                                    start_time=start_time, 
                                                                    end_time=api.utc_now())

                        logger.info(
                            f"Saved audio for {self.port}: {length_to_record!s}s to {final_output_filename}; "
                        )

                    # Check whether we've been asked to pause
                    time_to_sleep, length_to_record = AudioSensor.ok_to_record(
                        self.num_devices,
                        self.in_hive_mic_port,
                        self.port,
                        api.utc_now(),
                        self.av_rec_seconds,
                    )
                    if (time_to_sleep == 0) and (length_to_record < 5):
                        # If there is less than 5s left in the period, don't bother recording
                        time_to_sleep = length_to_record

                # We've failed the ok_to_record check, so we need to pause.
                # ok_to_record returns the number of seconds to sleep for.
                logger.info(
                    "Pausing audio rec on {self.port} as scheduled for {time_to_sleep}"
                )
                sleep(time_to_sleep)

            logger.info(
                f"Terminating AudioSensor for {self.port}; stop_requested={self.stop_requested}"
            )
        except Exception as e:
            logger.error(f"{root_cfg.RAISE_WARN()}Exception occurred in record_audio on {self.port}; {e}",
                exc_info=True,
            )
