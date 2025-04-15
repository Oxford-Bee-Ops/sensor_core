###################################################################################################
# Thie file contains recipes for fully specified device types.
#
# SensorCore config model
#
# DeviceCfg (1 per physical device)
# -> sensor_ds_list: list[SensorDsCfg] - 1 per Sensor)
#    -> [0]
#       -> sensor_cfg: SensorCfg
#       -> datastream_cfgs: list[DatastreamCfg]
#          -> [0]
#             -> edge_processors: list[DataProcessorCfg]
#             -> cloud_processors: list[DataProcessorCfg]
#
###################################################################################################
from sensor_core import DatastreamCfg, SensorDsCfg
from sensor_core import configuration as root_cfg
from sensor_core.sensors.config_object_defs import (
    ARUCO_DATA_DS,
    ARUCO_MARKED_UP_VIDEO_DS,
    CONTINUOUS_VIDEO_DS,
    CONTINUOUS_VIDEO_DS_TYPE_ID,
    TRAP_CAM_DS,
    RpicamSensorCfg,
    TrapCamProcessorCfg,
)

logger = root_cfg.setup_logger("sensor_core")


###################################################################################################
# Low FPS continuous video reording device
###################################################################################################
continuous_video_4fps_device = [
    SensorDsCfg(
        sensor_cfg=RpicamSensorCfg(
            rpicam_cmd = "rpicam-vid --framerate 4 --width 640 --height 480 -o FILENAME -t 180000 -v 0"
        ),
        datastream_cfgs=[
            CONTINUOUS_VIDEO_DS,
        ],
    )
]
###################################################################################################
# Trap cameras
#
# We start with a low FPS continuous video recording device, and add a trap camera processor to it.
# This creates a derived TRAP_CAM_DS datastream with the sub-sampled videos.
# The original continuous video recording is deleted after being passed to the trap cam DP; 
# we could opt to save raw samples if we wanted to.
###################################################################################################
trap_cam_device = [
    SensorDsCfg(
        sensor_cfg=RpicamSensorCfg(
            rpicam_cmd = ("rpicam-vid --autofocus-mode manual --lens-position 6 "
                          "--framerate 4 --width 640 --height 480 -o FILENAME -t 180000 -v 0")
        ),
        datastream_cfgs=[
            DatastreamCfg(
                ds_type_id = CONTINUOUS_VIDEO_DS_TYPE_ID,
                raw_format = "mp4",
                archived_format = "mp4",
                archived_data_description = "Basic continuous video recording.",
                cloud_container = "sensor-core-upload",
                edge_processors=[
                    TrapCamProcessorCfg(
                        derived_datastreams=[
                            TRAP_CAM_DS,
                        ],
                    )
                ],
            ),
        ],
    )
]

double_trap_cam_device = [
    SensorDsCfg(
        sensor_cfg=RpicamSensorCfg(
            sensor_index = 0,
            rpicam_cmd = ("rpicam-vid --camera SENSOR_INDEX --autofocus-mode manual --lens-position 6 "
                          "--framerate 8 --width 640 --height 480 -o FILENAME -t 180000 -v 0")
        ),
        datastream_cfgs=[
            DatastreamCfg(
                ds_type_id = CONTINUOUS_VIDEO_DS_TYPE_ID,
                raw_format = "mp4",
                archived_format = "mp4",
                archived_data_description = "Basic continuous video recording.",
                cloud_container = "sensor-core-upload",
                edge_processors=[
                    TrapCamProcessorCfg(
                        min_blob_size=1000,
                        max_blob_size=1000000,
                        derived_datastreams=[
                            TRAP_CAM_DS,
                        ],
                    )
                ],
            ),
        ],
    ),
    SensorDsCfg(
        sensor_cfg=RpicamSensorCfg(
            sensor_index = 1,
            rpicam_cmd = ("rpicam-vid --camera SENSOR_INDEX --autofocus-mode manual --lens-position 6 "
                          "--framerate 8 --width 640 --height 480 -o FILENAME -t 180000 -v 0")
        ),
        datastream_cfgs=[
            DatastreamCfg(
                ds_type_id = CONTINUOUS_VIDEO_DS_TYPE_ID,
                raw_format = "mp4",
                archived_format = "mp4",
                archived_data_description = "Basic continuous video recording.",
                cloud_container = "sensor-core-upload",
                edge_processors=[
                    TrapCamProcessorCfg(
                        min_blob_size=1000,
                        max_blob_size=1000000,
                        derived_datastreams=[
                            TRAP_CAM_DS,
                        ],
                    )
                ],
            ),
        ],
    )
]
###################################################################################################
# Device for spotting Aruco markers in video
###################################################################################################
aruco_device = [
    SensorDsCfg(
        sensor_cfg=RpicamSensorCfg(
            rpicam_cmd = "rpicam-vid --framerate 4 --width 640 --height 480 -o FILENAME -t 180000 -v 0"
        ),
        datastream_cfgs=[
            ARUCO_DATA_DS,
            ARUCO_MARKED_UP_VIDEO_DS,
        ],
    )
]
