####################################################################################################
# VideoSensor class used to manage the camera on a Raspberry Pi.
#
# We record images and video on a loop and save off as files to ramdisk.
# Video files are either:
#  - processed locally by VideoProcessor (spotting bees in HIVECAM and PAMCAM videos)
#  - or moved to the VIDEO_UPLOAD_DIR from where they are pushed to the cloud by push_to_cloud.sh.
# Image files are always moved to the VIDEO_UPLOAD_DIR.
#
# HIVECAM and PAMCAM spend the majority of their time recording and processing video.
# We can also trigger the camera to take still images:
# - we do this on-demand (from BCLI) to check that the camera is set up correctly
# - PAMCAMs also take still images at regular intervals (hourly) that feed into our flower analysis.
#
# To make this on-demand image-taking work via BCLI (which is a separate python process), we use
# a flag file (utils.TAKE_PICTURE_FLAG) to signal to the VideoSensor process that it should
# take a picture. This flag file is created by BCLI and deleted by VideoSensor.time_to_take_still_image.
#
####################################################################################################

import os
import subprocess
from datetime import datetime, timedelta
from time import sleep

from sensor_core import Sensor, SensorDsCfg, api
from sensor_core import configuration as root_cfg
from sensor_core.sensors.config_object_defs import VideoSensorCfg
from sensor_core.utils import file_naming, utils

if root_cfg.running_on_rpi:
    from libcamera import Transform, controls  # type: ignore
    from picamera2 import Picamera2  # type: ignore
    from picamera2.encoders import H264Encoder, Quality  # type: ignore
else:
    class Quality: # type: ignore
        """Mock class for Quality to avoid import errors on non-Raspberry Pi environments."""
        LOW = 1
        MEDIUM = 2
        HIGH = 3

logger = root_cfg.setup_logger("sensor_core")


############################################################
# Default camera configuration
############################################################
CAMERA_SENSOR_RESOLUTION = (4608, 2592)


class VideoSensor(Sensor):
    def __init__(self, sds_config: SensorDsCfg):
        """Constructor for the VideoSensor class"""
        super().__init__(sds_config)
        self.sds_config = sds_config

        # Type check the sensor configuration so we get strong type checking
        assert isinstance(sds_config.sensor_cfg, VideoSensorCfg)
        self.sensor_cfg: VideoSensorCfg = sds_config.sensor_cfg

        # Need to reimplement bcli test mode @@@
        self.bcli_test_mode = False
        if self.bcli_test_mode:
            self.av_rec_seconds = 10
        else:
            self.av_rec_seconds = self.sensor_cfg.av_rec_seconds

        # Set up the camera configuration based on the device type
        self.video_resolution = self.sensor_cfg.video_resolution
        self.still_resolution = self.sensor_cfg.still_resolution
        self.framerate = self.sensor_cfg.fps
        self.video_zoom = self.sensor_cfg.video_zoom
        self.focal_length = self.sensor_cfg.focal_length
        self.video_quality = self.sensor_cfg.video_quality

        # Track the periodic taking of still images for PAMCAMs
        self.still_interval = self.sensor_cfg.still_interval
        if self.still_interval > 0:
            self.next_still_image_time = datetime.now().replace(minute=0, second=0, microsecond=0)
            self.next_still_image_time += timedelta(seconds=self.still_interval)
        else:
            # If we don't want to take still images, set the next time to be an arbitrary date 
            # far in the future
            self.next_still_image_time = datetime(2100, 1, 1)

        self.scaler_crop = (
            int(CAMERA_SENSOR_RESOLUTION[0] * (0.5 - (0.5 / self.video_zoom))),  # x
            int(CAMERA_SENSOR_RESOLUTION[1] * (0.5 - (0.5 / self.video_zoom))),  # y
            int(CAMERA_SENSOR_RESOLUTION[0] / self.video_zoom),  # width
            int(CAMERA_SENSOR_RESOLUTION[1] / self.video_zoom),  # height
        )

        logger.info(
            f"Video sensor initialised with config:"
            f" video_resolution={self.video_resolution},"
            f" still_resolution={self.still_resolution},"
            f" framerate={self.framerate},"
            f" video_zoom={self.video_zoom},"
            f" focal_length={self.focal_length},"
            f" scaler_crop={self.scaler_crop},"
            f" video_quality={self.video_quality},"
            f" still_interval={self.still_interval},"
            f" next_still_image_time={self.next_still_image_time}"
            f" av_rec_seconds={self.av_rec_seconds}"
            f" bcli_test_mode={self.bcli_test_mode}"
        )

    def run_single_image(self):
        """Take a single image and save it to the VIDEO_CAPTURE_DIR"""
        if not root_cfg.running_on_rpi:
            logger.warning("Video configuration is only supported on Raspberry Pi.")
            return
        with Picamera2() as camera:
            self.configure_for_still_image(camera)
            self.take_still_image(camera, bcli_test_mode=self.bcli_test_mode)

    def run(self):
        """Main loop for the VideoSensor - runs continuously unless paused."""
        if not root_cfg.running_on_rpi:
            logger.warning("Video configuration is only supported on Raspberry Pi.")
            return

        # Get the Datastream objects for this sensor so we can log / save data to them
        # We expect 0 or 1 video datastreams with input_format="h264" or "mp4"
        # We expect 0 or 1 still image datastreams with input_format="jpg"
        self.video_ds = self.get_datastreams(format=self.sensor_cfg.video_format, expected=1)[0]
        self.image_ds = self.get_datastreams(format="jpg", expected=1)[0]

        # Use the "with" context manager to ensure camera is closed after use
        failures = 0
        with Picamera2() as camera:
            # We don't need to specify a bitrate because we're specify the quality setting 
            # when we start recording
            encoder = H264Encoder()
            self.configure_for_video(camera)

            # Main loop to record video and take still images
            while not self.stop_requested:
                try:
                    # Check if we need to take a still image
                    if self.image_ds is not None:
                        if self.time_to_take_still_image():
                            self.configure_for_still_image(camera)
                            self.take_still_image(camera, self.bcli_test_mode)

                            # We assume we always flip back to video recording after taking a still image
                            # Re-configure video settings appropriately for PAMCAM or HIVECAM
                            self.configure_for_video(camera)
                            encoder = H264Encoder()

                    # If memory is running low, we pause recording until the video_processor and push_to_cloud
                    # have dealt with the backlog.
                    if utils.pause_recording() and not self.bcli_test_mode:
                        sleep(180)
                        continue

                    # Record video for the specified number of seconds
                    if self.video_ds is not None:
                        # To keep the video and audio in sync, we trim the recording time to 
                        # finish on a multiple of the av_rec_seconds.
                        # The audio recording logic does the same.
                        now = datetime.now()
                        current_sec = (now.minute * 60) + now.second
                        record_for = self.av_rec_seconds - (current_sec % self.av_rec_seconds)
                        self.capture_video(camera, encoder, record_for)
                    else:
                        # If we're not recording video, sleep for a bit to avoid a tight loop
                        sleep(min(2, self.still_interval))

                    # If we're in bcli_test_mode, we only want to record one video
                    if self.bcli_test_mode:
                        break

                except Exception as e:
                    failures += 1
                    # This is not an ETL_ERROR because we will retry
                    logger.error(
                        f"{root_cfg.RAISE_WARN()}Exception in Picamera2 loop. Failcount={failures!s}, {e!s}",
                        exc_info=True,
                    )
                    # Dump some memory diagnostics before we clean up
                    free_m = utils.run_cmd("free -m", ignore_errors=True)
                    logger.info(free_m)
                    df_h = utils.run_cmd("df -h", ignore_errors=True)
                    logger.info(df_h)
                    mem_p = utils.run_cmd(
                        "ps aux --sort=-%mem",
                        ignore_errors=True,
                    )
                    logger.info(mem_p[:10])
                    # Re-raise the exception if we've failed 3 times
                    if failures >= 3:
                        raise e

            logger.warning("Exiting VideoSensor loop")

    def capture_video(self, camera, encoder, record_for_seconds: int):
        """Capture video for the specified number of seconds"""
        if not root_cfg.running_on_rpi:
            logger.warning("Video configuration is only supported on Raspberry Pi.")
            return

        # Debug option to dump out the video configuration
        logger.debug(f"Video configuration: {camera.video_configuration}")
        start_time = api.utc_now()

        # Create the timestamped filename just before we start recording
        vid_output_filename = file_naming.get_temporary_filename("h264")
        logger.info(f"Recording to {vid_output_filename} for {record_for_seconds} seconds")

        camera.start_recording(encoder, str(vid_output_filename), quality=self.video_quality)
        sleep(max(record_for_seconds, 5))

        # Grab the end timestamp now before the potentially slow saving of the video stream
        end_time = api.utc_now()
        camera.stop_recording()

        # Reformat to MP4 if required
        new_fname = vid_output_filename
        if self.video_ds.ds_config.input_format == "mp4":
            # Convert the H264 file to MP4 format and delete the original H264 file
            new_fname = new_fname.with_suffix(".mp4")
            if logger.isEnabledFor(10):
                logger.debug(f"Recording has {get_frame_count(vid_output_filename)} frames")
            convert_h264_to_mp4(vid_output_filename, new_fname, self.framerate)
            if logger.isEnabledFor(10):
                logger.debug(f"Recording has {get_frame_count(new_fname)} frames")
            vid_output_filename.unlink()

        logger.debug(f"Captured video {new_fname}")

        assert self.video_ds is not None
        self.video_ds.save_recording(
            new_fname,
            start_time=start_time,
            end_time=end_time,
        )

    def take_still_image(self, camera, bcli_test_mode):
        """Take a still image and save it to the VIDEO_UPLOAD_DIR"""
        if not root_cfg.running_on_rpi:
            logger.warning("Video configuration is only supported on Raspberry Pi.")
            return

        if not utils.pause_recording() or bcli_test_mode:
            start_time = api.utc_now()
            camera.start()
            still_output_filename = self.image_ds.get_temporary_filename("jpg")
            camera.capture_file(still_output_filename)
            camera.stop()
            logger.info(f"Temporarily saved image to {still_output_filename}")

            # Save the image to the datastream
            assert self.image_ds is not None
            self.image_ds.save_recording(
                still_output_filename,
                start_time=start_time,
                end_time=api.utc_now()
            )

    def configure_for_video(self, camera):
        """Configure the camera for video recording"""
        if not root_cfg.running_on_rpi:
            logger.warning("Video configuration is only supported on Raspberry Pi.")
            return
        camera.video_configuration.controls.FrameRate = self.framerate
        camera.video_configuration.controls.AfMode = controls.AfModeEnum.Manual
        camera.video_configuration.controls.LensPosition = 1 / self.focal_length
        if self.video_zoom != 1.0:
            camera.video_configuration.controls.ScalerCrop = self.scaler_crop
        camera.video_configuration.main.size = self.video_resolution
        camera.video_configuration.encode = "main"
        if self.sensor_cfg.rotate_camera == 180:
            camera.video_configuration.transform = Transform(hflip=1, vflip=1)
        camera.configure("video")

        logger.info(f"Camera configuration: {camera.camera_configuration()}")

    def configure_for_still_image(self, camera):
        """Configure the camera for still image capture"""
        if not root_cfg.running_on_rpi:
            logger.warning("Video configuration is only supported on Raspberry Pi.")
            return
        camera.still_configuration.controls.AfMode = controls.AfModeEnum.Manual
        camera.still_configuration.controls.LensPosition = 1 / self.focal_length
        if self.video_zoom != 1.0:
            camera.still_configuration.controls.ScalerCrop = self.scaler_crop
        camera.still_configuration.size = self.still_resolution
        if self.sensor_cfg.rotate_camera == 180:
            camera.still_configuration.transform = Transform(hflip=1, vflip=1)
        camera.configure("still")

        logger.info(f"Configured for still image: {camera.camera_configuration}")

    # Returns true if it's time to take a still image; false otherwise
    def time_to_take_still_image(self):
        """
        There are two scenarios where we want to take a still image:
        1. On-demand from BCLI, in which case we check for the flag file
        2. Regularly scheduled still images (eg every hour) as defined by STILL_INTERVAL
        """

        # Check if the flag file exists and if so, delete it and return True
        if os.path.exists(root_cfg.TAKE_PICTURE_FLAG):
            os.remove(root_cfg.TAKE_PICTURE_FLAG)
            return True

        # Check if we've reached the next interval and if so, return True
        if datetime.now() >= self.next_still_image_time:
            self.next_still_image_time += timedelta(seconds=self.still_interval)
            return True

        # Otherwise return false
        return False


############################################################
# Convert a file from H264 to MP4 format
############################################################
def get_frame_count(video_file):
    command = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-count_frames",
        "-show_entries",
        "stream=nb_read_frames",
        "-of",
        "default=nokey=1:noprint_wrappers=1",
        str(video_file),
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return int(result.stdout.strip())


def convert_h264_to_mp4(src_file, dst_file, framerate):
    # Use ffmpeg to convert H264 to MP4 while maintaining image quality
    # The settings are finnickity, so test thoroughly if you make any changes
    # Need to use -r to explicitly set input and output framerates because it's not set in the raw H264
    # Don't use -framerate (instead of -r) because it doesn't work reliably
    command = [
        "ffmpeg",
        "-r",  # Set input framerate
        str(framerate),
        "-i",  # Set the input file
        str(src_file),
        "-c:v",  # Encode using libx264; we can't just copy the stream because it's missing data
        "libx264",
        "-r",  # Set output framerate
        str(framerate),
        "-pix_fmt",
        "yuv420p",
        "-preset",
        "superfast",
        "-crf",  # Set the constant rate factor (0-51, 0=lossless, 23=default, 51=worst)
        "18",
        "-y",  # Overwrite the output file if it exists
        str(dst_file),  # Set the output file
    ]
    subprocess.run(command, check=True)
