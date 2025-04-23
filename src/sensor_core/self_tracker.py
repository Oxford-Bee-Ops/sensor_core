from datetime import timedelta
from time import sleep

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.config_objects import Datastream, DatastreamCfg
from sensor_core.dp_engine import DPengine
from sensor_core.dp_tree_node import DPtree, Stream
from sensor_core.sensor import Sensor, SensorCfg

logger = root_cfg.setup_logger("sensor_core")


############################################################################################################
# Datastreams produced by the SensorCore system
#############################################################################################################

# SCORE - DatastreamType for recording sample count / duration from the data pipeline
SCORE_FIELDS = [
    "observed_type_id",
    "observed_sensor_index",
    "sample_period",
    "count",
    "duration",
]
SCORE_DS_TYPE = DatastreamCfg(
    type_id=api.SCORE_DS_TYPE_ID,
    input=Stream(0, "log", SCORE_FIELDS),
    output=[Stream(0, "csv", SCORE_FIELDS)],
    description=(
        "Data on sample counts and recording period durations from all Datastreams. "
        "The data is automatically recorded by the SensorCore for all datastreams when "
        "they log data or save a recording."
    ),
    cloud_container=root_cfg.my_device.cc_for_system_records,
    edge_processors=None,
)

# SCORP - special DatastreamType for recording performance of the data pipeline
SCORP_FIELDS = [
    "data_processor_id", 
    "observed_type_id", 
    "observed_sensor_index", 
    "duration"
]
SCORP_DS_TYPE = DatastreamCfg(
    type_id=api.SCORP_DS_TYPE_ID,
    input_format="log",
    input_fields=SCORP_FIELDS,
    output_streams="csv",
    output_fields=SCORP_FIELDS,
    description=(
        "Performance data from the data pipeline. "
        "The data is recorded as a log file on the device and archived as a CSV file."
    ),
    cloud_container=root_cfg.my_device.cc_for_system_records,
    edge_processors=None,
)

SC_TRACKING_CFG = SensorCfg(
    sensor_type="SYS",
    sensor_index=0,
    type_id="ScTracking",
    node_index=0,
    description="SensorCore self-telemetry",
    outputs=[
        Stream(0, format="log", fields=SCORE_FIELDS),
        Stream(1, format="log", fields=SCORP_FIELDS),
    ],
    sensor_class_ref="sensor_core.device_health.DeviceHealth",
)

class SelfTracker(Sensor):
    """SelfTracking is a special Sensor class that is used to track the performance of 
    the SensorCore system.
    
    It is not a physical sensor, but is used to track the performance of the system.
    """
    def __init__(self) -> None:
        super().__init__(SC_TRACKING_CFG)

    def build_dptree(self) -> DPtree:
        sys_dptree = DPtree()
        sys_dptree.connect((self, 0), Datastream(SCORE_DS_TYPE))
        sys_dptree.connect((self, 1), Datastream(SCORP_DS_TYPE))
        return sys_dptree

    def set_dp_engine(self, dp_engines: list[DPengine]) -> None:
        """Set the DPengine for the SelfTracking sensor.
        
        This method is called by the EdgeOrchestrator when the SelfTracking is started.
        """
        self.dp_engines = dp_engines

    def run(self) -> None:
        """Main loop for the DeviceHealth sensor.
        This method is called when the thread is started.
        It runs in a loop, logging health data and warnings at regular intervals.
        """
        logger.info(f"Starting DeviceHealth thread {self!r}")

        while not self.stop_requested:
            # Trigger each datastream to log sample counts
            for dp_engine in self.dp_engines:
                dp_engine.log_sample_data(self.last_ran, self)

            # Set timer for next run
            self.last_ran = api.utc_now()
            next_hour = (self.last_ran + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            sleep_time = (next_hour - self.last_ran).total_seconds()
            sleep(sleep_time)
