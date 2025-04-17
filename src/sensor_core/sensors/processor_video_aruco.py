####################################################################################################
# Class: VideoArucoProcessor
#
# This class performs event detection to identify ARUCO markers in videos.
####################################################################################################
from array import array
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import cv2
import numpy as np
import pandas as pd

from sensor_core import DataProcessor, Datastream, DpContext, api
from sensor_core import configuration as root_cfg
from sensor_core.sensors.config_object_defs import ArucoProcessorCfg
from sensor_core.utils import file_naming

cv2.setRNGSeed(42)

logger = root_cfg.setup_logger("sensor_core")


@dataclass
class MarkersData:
    for_csv: list[dict] = field(default_factory=list)
    corner_sets: array = field(default_factory=lambda: array("f"))


@dataclass
class FrameMarkersData:
    known_markers: MarkersData = field(default_factory=MarkersData)
    unknown_markers: MarkersData = field(default_factory=MarkersData)


class VideoArucoProcessor(DataProcessor):

    def process_data(self, 
                     datastream: Datastream,
                     input_data: pd.DataFrame | list[Path],
                     context: DpContext) -> Optional[pd.DataFrame]:
        """Process a list of video files and identify ARUCO markers."""

        assert isinstance(input_data, list), f"Expected list of files, got {type(input_data)}"
        files: list[Path] = input_data
        results: list[pd.DataFrame] = []
        assert isinstance(context.dp, ArucoProcessorCfg)
        dp_cfg: ArucoProcessorCfg = context.dp
        aruco_dict_name = dp_cfg.aruco_dict_name
        save_marked_up_video = dp_cfg.save_marked_up_video

        for f in files:
            try:
                #############################################################################################
                # Step 1: identify & output potential ARUCO markers
                # This also saves a marked up version of the video to a derived datastream
                #############################################################################################
                result = self.process_video_file(f, 
                                                 save_marked_up_video,
                                                 aruco_dict_name=aruco_dict_name)
                if result is not None:
                    results.append(result)

            except Exception as e:
                logger.error(
                    f"{root_cfg.RAISE_WARN()}Exception occurred processing video {f!s}; {e!s}",
                    exc_info=True,
                )

        # Return the results as a single dataframe
        if len(results) > 0:
            return pd.concat(results)
        else:
            return None

    def process_video_file(self, 
                           source_file: Path, 
                           save_marked_up_video: bool = True,
                           aruco_dict_name: str = "DICT_4X4_50") -> pd.DataFrame:
        """Process a single file - find potential aruco markers in each frame & save results"""
        # In future this might also return the CSV, so that the results can be
        # combined with other analysis of the video (e.g. to find
        # unmarked bees)

        # Verify the parameters
        logger.debug(f"process_video_file() using {aruco_dict_name} on {source_file}")
        assert source_file.exists(), f"{source_file=} video file does not exist"

        video = cv2.VideoCapture(str(source_file))
        assert video.isOpened(), f"Unable to open video at {source_file} \n(str version = {source_file!s})"
        assert hasattr(cv2.aruco, aruco_dict_name), (
            f"{aruco_dict_name} is not a recognized Aruco dictionary name"
        )

        try:
            # If requested, get ready to output a marked up version of the video,
            # with the same properties as the original.
            if save_marked_up_video:
                fps = int(video.get(cv2.CAP_PROP_FPS))
                width = int(video.get(cv2.CAP_PROP_FRAME_WIDTH))
                height = int(video.get(cv2.CAP_PROP_FRAME_HEIGHT))
                fourcc = cv2.VideoWriter.fourcc(*"mp4v")  # Better compatibility for MP4
                out_path = file_naming.get_temporary_filename("mp4")
                out_video = cv2.VideoWriter(str(out_path), fourcc, fps, (width, height))

            # Get the appropriate Aruco tag dictionary
            tag_dictionary_id = getattr(cv2.aruco, aruco_dict_name)
            tag_dictionary: cv2.aruco.Dictionary = cv2.aruco.getPredefinedDictionary(tag_dictionary_id)

            # Set up the detection parameters
            parameters: cv2.aruco.DetectorParameters = cv2.aruco.DetectorParameters()
            parameters.cornerRefinementMethod = cv2.aruco.CORNER_REFINE_SUBPIX
            parameters.minMarkerPerimeterRate = 0.03
            parameters.adaptiveThreshWinSizeMin = 5
            parameters.adaptiveThreshWinSizeStep = 6
            parameters.polygonalApproxAccuracyRate = 0.06
            detector = cv2.aruco.ArucoDetector(tag_dictionary, parameters)
            frame_num = 1
            all_markers_full_info = []
            total_frame_count = video.get(cv2.CAP_PROP_FRAME_COUNT)

            while frame_num <= total_frame_count:
                read_ok, frame = video.read()
                # Read_ok will be also false when we reach the end of the file,
                # but we check the total_frame_count so if read_ok
                # is false, then something bad has happened
                assert read_ok, f"Unable to read {frame_num=} in {source_file=}"
                frame_markers = self._get_aruco_markers_in_frame(
                    detector=detector, frame=frame, frame_num=frame_num
                )
                standard_columns = {"filename": source_file.name, "frame_number": frame_num}
                for marker_csv_info in (
                    frame_markers.known_markers.for_csv + frame_markers.unknown_markers.for_csv
                ):
                    all_markers_full_info.append(standard_columns | marker_csv_info)

                # If requested, store a marked up version
                # Color format is BGR
                # Note - the red is quite orange (to my eyes).  It also looks like
                # some bad markers are red and some are orange, but I think
                # that may be an optical effect when a marker changes from
                # green in 1 frame to orange in the next frame, because it only happens
                # on actual aruco markers
                if save_marked_up_video:
                    for corner_set in frame_markers.known_markers.corner_sets:
                        pts = np.array(corner_set).astype(int).reshape((-1, 1, 2))
                        cv2.polylines(frame, [pts], isClosed=True, color=(0, 255, 0), thickness=2)
                    for corner_set in frame_markers.unknown_markers.corner_sets:
                        pts = np.array(corner_set).astype(int).reshape((-1, 1, 2))
                        cv2.polylines(frame, [pts], isClosed=True, color=(0, 0, 255), thickness=2)
                    out_video.write(frame)

                frame_num += 1

            logger.debug(f"Finished processing {frame_num - 1} frames")
        finally:
            video.release()
            if save_marked_up_video:
                out_video.release()

        # Save the marked up video to the derived datastream
        derived_dss = self.get_derived_datastreams()
        assert len(derived_dss) == 1
        marked_up_ds: Datastream = derived_dss[0]
        parts = file_naming.parse_record_filename(source_file)
        marked_up_ds.save_recording(
            temporary_file=out_path,
            start_time=parts[api.RECORD_ID.TIMESTAMP.value],
            end_time=parts[api.RECORD_ID.END_TIME.value],
        )

        # Return the data as a dataframe
        df = pd.DataFrame(all_markers_full_info)
        return df

    def _get_aruco_markers_in_frame(
        self, detector: cv2.aruco.ArucoDetector, frame, frame_num: int
    ) -> FrameMarkersData:
        """Find Aruco markers, and possible markers, in a single frame.

        Returns:
            FrameMarkersData, which contains csv-able data and corners data
            for known and unknown markers.

            Notes:
            - Both known & known sets of data contain the same fields,
              i.e. they could be
              combined into single lists, but are returned separately for
              easier processing, e.g. when marking up videos.
            - the x,y coordinates of the corners are included in the csvs,
              but are also returned in the CV2's corner format, again
              to make it easier to mark up videos without needing to convert
              data formats.
        """
        return_data = FrameMarkersData()
        known_marker_info = return_data.known_markers.for_csv
        unknown_marker_info = return_data.unknown_markers.for_csv

        def add_marker_info(list_to_update, corners_as_3d_array, marker_id):
            # Note, a corner set from detectMarkers() is a 3D array, (1,4,2).
            # 4 = number of corners, 2 = x,y for each corner.  Not sure
            # why we need the extra dimension, but hence taking the [0] element
            # to get the actual list of x,y corners.

            # From https://docs.opencv.org/4.x/d5/dae/tutorial_aruco_detection.html
            # " For each marker, its four corners are returned in their original
            # order (which is clockwise starting with top left).
            # So, the first corner is the top left corner,
            # followed by the top right, bottom right and bottom left."
            # Note that this is top-left of the marker in the original
            # printed version, not top-left of the detected square.  i.e. if
            # a marker is rotated, the marker top-left corner also rotates
            corners_as_2d_array = corners_as_3d_array[0]
            top_left, top_right, bottom_right, bottom_left = corners_as_2d_array
            x_col = 0
            y_col = 1

            # Centre of marker box = average of all x,y values
            centreX = corners_as_2d_array[:, x_col].mean()
            centreY = corners_as_2d_array[:, y_col].mean()

            # Do we need to store the midpoint of the top edge.  Not sure
            # why but assume it might be useful for some sort of analysis.
            # Assuming we always stick the markers on the bees in the same
            # orientation,then I think the top edge would be the
            # front/head of the bee = roughly which way it's looking

            # We just record all 4 corners explicitly for now
            list_to_update.append(
                {
                    "marker_id": marker_id,
                    "centreX": float(centreX),
                    "centreY": float(centreY),
                    # "topEdgeMidX": float(topEdgeMidX),
                    # "topEdgeMidY": float(topEdgeMidY),
                    "topLeftX": float(top_left[x_col]),
                    "topLeftY": float(top_left[y_col]),
                    "topRightX": float(top_right[x_col]),
                    "topRightY": float(top_right[y_col]),
                    "bottomRightX": float(bottom_right[x_col]),
                    "bottomRightY": float(bottom_right[y_col]),
                    "bottomLeftX": float(bottom_left[x_col]),
                    "bottomLeftY": float(bottom_left[y_col]),
                }
            )

        # Convert the frame to greyscale
        # @@@ TO DO, what python type is frame (so we can type check)?
        gray = cv2.cvtColor(frame, cv2.COLOR_RGB2GRAY)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        cl1 = clahe.apply(gray)
        gray = cv2.cvtColor(cl1, cv2.COLOR_GRAY2RGB)

        # Detect markers in the frame, with and without known IDs
        # Note:
        # - known_ids can be None or a vector of vectors
        # - corner_sets are arrays (which can be empty but never None)
        return_data.known_markers.corner_sets, known_ids, return_data.unknown_markers.corner_sets = (
            detector.detectMarkers(frame)
        )

        for corner_set in return_data.unknown_markers.corner_sets:
            add_marker_info(
                list_to_update=unknown_marker_info, corners_as_3d_array=corner_set, marker_id=None
            )

        if known_ids is not None:
            for corner_set, known_id_vector in zip(return_data.known_markers.corner_sets, known_ids):
                add_marker_info(
                    list_to_update=known_marker_info,
                    corners_as_3d_array=corner_set,
                    marker_id=known_id_vector[0],
                )

        known_marker_count = len(known_marker_info)
        unknown_marker_count = len(unknown_marker_info)
        logger.debug(
            f"Frame {frame_num}: {known_marker_count} known IDs, {unknown_marker_count} unknown IDs.  "
            f"Total = {known_marker_count + unknown_marker_count}"
        )
        return return_data
