from datetime import timedelta
from pathlib import Path
from typing import Optional

import cv2
import pandas as pd
import numpy as np

from sensor_core import DataProcessor, Datastream, DpContext, api
from sensor_core import configuration as root_cfg
from sensor_core.sensors.config_object_defs import TRAP_CAM_DS_TYPE_ID, TrapCamProcessorCfg
from sensor_core.utils import file_naming

logger = root_cfg.setup_logger("sensor_core")

class ProcessorVideoTrapCam(DataProcessor):
    def __init__(self):
        super().__init__()
        self.kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE,(3,3))
        self.background_subtractor = cv2.createBackgroundSubtractorMOG2()

    def process_data(
        self, 
        datastream: Datastream,
        input_data: pd.DataFrame | list[Path],
        context: DpContext
    ) -> Optional[pd.DataFrame]:
        """Process a list of video files and resave video segments with movement."""

        assert isinstance(input_data, list), f"Expected list of files, got {type(input_data)}"
        files: list[Path] = input_data
        assert isinstance(context.dp, TrapCamProcessorCfg)
        dp_cfg: TrapCamProcessorCfg = context.dp
        min_blob_size = dp_cfg.min_blob_size
        max_blob_size = dp_cfg.max_blob_size

        # We save the processed video segments to a derived datastream
        derived_ds = self.get_derived_datastreams(TRAP_CAM_DS_TYPE_ID)[0]
        assert derived_ds is not None, (
            f"Derived datastream {TRAP_CAM_DS_TYPE_ID} not found"
        )

        for f in files:
            try:
                logger.info(f"Processing video file: {f!s}")
                self.process_video(derived_ds, f, min_blob_size, max_blob_size)
            except Exception as e:
                logger.error(
                    f"{root_cfg.RAISE_WARN()}Exception occurred processing video {f!s}; {e!s}",
                    exc_info=True,
                )
        return None

    def process_video(self, 
                      derived_ds: Datastream, 
                      video_path: Path, 
                      min_blob_size: int, 
                      max_blob_size: int) -> None:
        """ Process a video file to detect movement and save segments with movement. 
        We record for a minimum of 2 seconds after movement is detected."""
        assert video_path.suffix[1:] == derived_ds.ds_config.raw_format, (
            f"Video file suffix {video_path.suffix} doesn't match "
            f"expected format {derived_ds.ds_config.raw_format}"
        )

        cap = cv2.VideoCapture(str(video_path))
        if not cap.isOpened():
            exists = video_path.exists()
            raise ValueError(f"Unable to open video file (exists={exists}): {video_path};"
                             f" opencv installation issue?")

        fname_details = file_naming.parse_record_filename(video_path)
        start_time = fname_details[api.RECORD_ID.TIMESTAMP.value]
        frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = int(cap.get(cv2.CAP_PROP_FPS))
        suffix = video_path.suffix[1:]
        if suffix == "h264":
            fourcc = cv2.VideoWriter.fourcc(*'h264')
        elif suffix == "mp4":
            fourcc = cv2.VideoWriter.fourcc(*'mp4v')
        else:
            raise ValueError(f"Unsupported video format: {suffix}")

        samples_saved = 0
        output_stream: Optional[cv2.VideoWriter | None] = None
        current_frame = -1
        sample_first_frame = 0
        sample_last_movement_frame = 0
        frames_to_record = 1 * fps  # Record for some time after movement is detected
        discard_threshold = 2 # Discard the recording if it was just noise; ie less than X frames
        temp_filename: Path = Path("unspecified") # Set when we start saving video

        logger.info(f"Processing video with fps={fps}, res={frame_width}x{frame_height}: {video_path}")

        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                # If were in the middle of recording and the video ends, stop saving
                if output_stream:
                    output_stream.release()
                    output_stream = None
                    sample_start_time = start_time + timedelta(seconds=(sample_first_frame / fps))
                    sample_end_time = start_time + timedelta(seconds=(current_frame / fps))
                    derived_ds.save_recording(
                        temp_filename,
                        start_time=sample_start_time,
                        end_time=sample_end_time,
                    )
                    samples_saved += 1
                break

            current_frame += 1
            fg_mask = self.background_subtractor.apply(frame)
            fg_mask = cv2.morphologyEx(fg_mask, cv2.MORPH_OPEN, self.kernel)
            contours, _ = cv2.findContours(fg_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            movement = False
            for c in contours:
                contour_area = cv2.contourArea(c)
                if contour_area > min_blob_size and contour_area < max_blob_size:
                    movement = True
                    break

            if movement and (current_frame > 1): # Ignore the first 2 frames while the BS settles
                if not output_stream:
                    # Not currently recording; start recording
                    sample_first_frame = current_frame
                    sample_last_movement_frame = current_frame
                    temp_filename=file_naming.get_temporary_filename(derived_ds.ds_config.raw_format)
                    output_stream = cv2.VideoWriter(
                        filename=str(temp_filename),
                        fourcc=fourcc,
                        fps=fps,
                        frameSize=(frame_width, frame_height)
                    )
                    output_stream.write(frame)
                else:
                    # Already recording; update the last movement frame ID
                    sample_last_movement_frame = current_frame
                    output_stream.write(frame)
            else:
                # No movement detected... 
                if output_stream:
                    # ...but we are currently saving video
                    if (current_frame - sample_last_movement_frame) < frames_to_record:
                        # ...and we're still within the recording window
                        output_stream.write(frame)
                    else:
                        # No movement for a while, stop saving video
                        output_stream.release()
                        output_stream = None
                        sample_start_time = start_time + timedelta(seconds=(sample_first_frame / fps))
                        sample_end_time = start_time + timedelta(seconds=(current_frame / fps))

                        # Check if we have enough frames to save
                        sample_duration = (sample_end_time - sample_start_time).total_seconds()
                        if (sample_last_movement_frame - sample_first_frame) > discard_threshold:
                            # Save the video segment to the derived datastream
                            logger.info(f"Saving video of {sample_duration}s to {derived_ds.name}")
                            derived_ds.save_recording(
                                temp_filename,
                                start_time=sample_start_time,
                                end_time=sample_end_time,
                            )
                            samples_saved += 1
                        else: 
                            # Discard the video segment
                            logger.info(f"Discarding {(sample_last_movement_frame - sample_first_frame)}"
                                        f" frames of movement as noise")
                            temp_filename.unlink(missing_ok=True)
                            
        logger.info(f"Saved {samples_saved} from video: {video_path}")
        cap.release()

