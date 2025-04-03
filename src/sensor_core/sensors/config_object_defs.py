from dataclasses import dataclass, field
from typing import Optional

from sensor_core import api
from sensor_core.config_objects import DataProcessorCfg, DatastreamCfg, SensorCfg

#############################################################################################################
# Define the DatastreamType IDs
#############################################################################################################
WHOCAM_DATA_DS_TYPE_ID = "WHOCAM"
ARUCO_MARKED_UP_VIDEOS_DS_TYPE_ID = "WHOMARKED"

#############################################################################################################
# Define the SensorCfg objects
#############################################################################################################
@dataclass
class VideoSensorCfg(SensorCfg):
    ############################################################
    # SensorCfg fields
    ############################################################
    # The type of sensor.
    sensor_type: api.SENSOR_TYPES = "CAMERA"
    # Sensor index
    sensor_index: int = 1
    # The fully qualified class name of the sensor.
    # This must be interpretable as a Class by the Python
    sensor_class_ref: str = "rpi_sensor.sensor_video.VideoSensor"
    # A human-readable description of the sensor model.
    sensor_model_description: str = "Default video sensor"

    ############################################################
    # Custom fields
    ############################################################
    video_format: str = "h264"  # Video format
    video_resolution: tuple = (1920, 1080)  # Image resolution
    still_resolution: tuple = (1920, 1080)  # Still image resolution
    video_zoom: float = 1.0  # Zoom factor used on video only
    video_quality: int = 2  # Video quality picamera2.encoders.Quality
    av_rec_seconds: int = 180  # Record video for this many seconds
    fps: float = 4.0  # Frames per second
    focal_length: float = 0.2  # Focal length of the camera lens in metres
    rotate_camera: int = 180  # Rotate the camera image by 180 degrees
    # Interval between still images in seconds;
    # 0 disables still images
    # Cannot be less than 2s - use video recording for shorter intervals
    still_interval: int = 3600
    save_orig_video: bool = False  # Save the original video for upload to the cloud
    save_first_frame: bool = False  # Save the first frame of the video as a JPEG
    direct_video_upload: bool = True  # Upload the video directly to the cloud


@dataclass
class AudioSensorCfg(SensorCfg):
    ############################################################
    # SensorCfg fields
    ############################################################
    # The type of sensor.
    sensor_type: api.SENSOR_TYPES = "CAMERA"
    # Sensor index
    sensor_index: int = 1
    # The fully qualified class name of the sensor.
    # This must be interpretable as a Class by the Python
    sensor_class_ref: str = "rpi_sensor.video_sensor.VideoSensor"
    # A human-readable description of the sensor model.
    sensor_model_description: str = "Default video sensor"

    ############################################################
    # Custom fields
    ############################################################
    av_rec_seconds: int = 180
    microphones_installed: int = 0
    in_hive_mic_port: int = 0


#############################################################################################################
# Define the DERIVED DatastreamCfg objects
#############################################################################################################
@dataclass
class WhocamMarkedUpVideosDsCfg(DatastreamCfg):
    ds_type_id: str = ARUCO_MARKED_UP_VIDEOS_DS_TYPE_ID
    raw_format: api.FILE_FORMATS = "mp4"
    archived_format: api.FILE_FORMATS = "mp4"
    archived_data_description: str = "Marked up videos from the WHOCAM."

#############################################################################################################
# Define the DataProcessorCfg objects
#############################################################################################################
MARKER_INFO_REQD_COLUMNS: list[str] = [
    "filename",
    "frame_number",
    "marker_id",
    "centreX",
    "centreY",
    "topEdgeMidX",
    "topEdgeMidY",
    "topLeftX",
    "topLeftY",
    "topRightX",
    "topRightY",
    "bottomLeftX",
    "bottomLeftY",
    "bottomRightX",
    "bottomRightY",
]
@dataclass(frozen=True)
class ArucoProcessorCfg(DataProcessorCfg):
    #######################################################################
    # Standard DataProcessorCfg fields
    #######################################################################
    dp_class_ref: str = "rpi_sensor.processor_video_who.VideoWHOProcessor"
    dp_description: str = "WHOCAM video processor"
    input_format: api.FILE_FORMATS = "mp4"
    output_format: api.FILE_FORMATS = "df"
    output_fields: Optional[list[str]] = field(
        default_factory=lambda: api.REQD_RECORD_ID_FIELDS + MARKER_INFO_REQD_COLUMNS
    )
    derived_datastreams: list[DatastreamCfg] = field(
        default_factory=lambda: [WhocamMarkedUpVideosDsCfg()]
    )
    ########################################################################
    # Custom fields
    ########################################################################
    aruco_dict_name: str = "DICT_4X4_50"
    save_marked_up_video: bool = True  # Save the marked up video


#############################################################################################################
# Define the PRIMARY DatastreamCfg objects
#############################################################################################################

@dataclass
class WhocamDfDsCfg(DatastreamCfg):
    ds_type_id: str = WHOCAM_DATA_DS_TYPE_ID
    raw_format: api.FILE_FORMATS = "mp4"
    archived_format: api.FILE_FORMATS = "csv"
    archived_fields: list[str] = field(
        default_factory=lambda: api.REQD_RECORD_ID_FIELDS + MARKER_INFO_REQD_COLUMNS)
    archived_data_description: str = "Identified ARUCO markers from WHOCAM videos."
    sample_probability: str = str(0.01)
    sample_container: str = "sensor-core-upload"
    edge_processors: list[DataProcessorCfg] = field(
        default_factory=lambda: [ArucoProcessorCfg()])


