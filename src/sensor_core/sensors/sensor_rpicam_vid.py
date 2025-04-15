####################################################################################################
# Sensor class that provides a direct map onto Raspberry Pi's rpicam-vid for continuous video recording.
#
# The user specifies the rpicam-vid command line, except for the file name, which is set by SensorCore.
#
####################################################################################################

from time import sleep

from sensor_core import Datastream, Sensor, SensorDsCfg, api
from sensor_core import configuration as root_cfg
from sensor_core.sensors.config_object_defs import RpicamSensorCfg
from sensor_core.utils import file_naming, utils

logger = root_cfg.setup_logger("sensor_core")


class RpicamSensor(Sensor):
    def __init__(self, sds_config: SensorDsCfg):
        """Constructor for the RpicamSensor class"""
        super().__init__(sds_config)
        self.sds_config = sds_config

        assert isinstance(sds_config.sensor_cfg, RpicamSensorCfg)
        self.sensor_cfg: RpicamSensorCfg = sds_config.sensor_cfg
        self.raw_format = self.sds_config.datastream_cfgs[0].raw_format
        self.rpicam_cmd = self.sensor_cfg.rpicam_cmd

        assert self.rpicam_cmd, (
            f"rpicam_cmd must be set in the sensor configuration: {self.rpicam_cmd}"
        )
        assert self.rpicam_cmd.startswith("rpicam-vid "), (
            f"rpicam_cmd must start with 'rpicam-vid ': {self.rpicam_cmd}"
        )
        assert "FILENAME" in self.rpicam_cmd, (
            f"FILENAME placeholder missing in rpicam_cmd: {self.rpicam_cmd}"
        )
        assert "FILENAME " in self.rpicam_cmd, (
            f"FILENAME placeholder should be specified without any suffix rpicam_cmd: {self.rpicam_cmd}"
        )


    def run(self):
        """Main loop for the RpicamSensor - runs continuously unless paused."""
        if not root_cfg.running_on_rpi and not root_cfg.TEST_MODE:
            logger.warning("Video configuration is only supported on Raspberry Pi.")
            return

        # Get the Datastream objects for this sensor so we can log / save data to them
        # We expect 1 video datastream with raw_format="h264" or "mp4"
        video_ds = self.get_datastream(format=self.raw_format)
        assert video_ds is not None, (
            f"Datastream with raw_format={self.raw_format} not found in {self.sds_config.datastream_cfgs}"
        ) 
        self.video_ds: Datastream = video_ds

        # Main loop to record video and take still images
        while not self.stop_requested:
            try:
                # If memory is running low, we pause recording until the downstream processing
                # catches up.
                if utils.pause_recording():
                    sleep(180)
                    continue

                # Record video for the specified number of seconds
                start_time = api.utc_now()

                # Get the filename for the video file
                filename = file_naming.get_temporary_filename(
                    self.video_ds.ds_config.raw_format
                )

                # Replace the FILENAME placeholder in the command with the actual filename
                cmd = self.rpicam_cmd.replace("FILENAME", str(filename))

                # If the "--camera SENSOR_INDEX" string is present, replace SENSOR_INDEX with
                # the actual sensor index
                if "--camera SENSOR_INDEX" in cmd:
                    cmd = cmd.replace("SENSOR_INDEX", str(self.sds_config.sensor_cfg.sensor_index))

                logger.info(f"Recording video with command: {cmd}")

                # Start the video recording process
                rc = utils.run_cmd(cmd)
                logger.info(f"Video recording completed with rc={rc}")

                # Save the video file to the datastream
                self.video_ds.save_recording(filename, start_time=start_time, end_time=api.utc_now())

            except FileNotFoundError as e:
                logger.error(f"{root_cfg.RAISE_WARN()}FileNotFoundError in RpicamSensor: {e}", exc_info=True)
                
            except Exception as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Error in RpicamSensor: {e}", exc_info=True)
                break

        logger.warning("Exiting RpicamSensor loop")
