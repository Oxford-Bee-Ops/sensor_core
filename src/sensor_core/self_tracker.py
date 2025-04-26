from datetime import datetime, timedelta
from time import sleep

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.dp_config_object_defs import Stream
from sensor_core.dp_engine import DPengine
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
# SCORP - special DatastreamType for recording performance of the data pipeline
SCORP_FIELDS = [
    "data_processor_id", 
    "observed_type_id",
    "observed_sensor_index", 
    "duration"
]

SC_TRACKING_CFG = SensorCfg(
    sensor_type=api.SENSOR_TYPE.SYS,
    sensor_index=0,
    sensor_model="SelfTracker",
    description="SensorCore self-telemetry",
    outputs=[
        Stream("System datastream of DataProcessor performance data", 
               api.SCORP_DS_TYPE_ID, 
               api.SCORP_STREAM_INDEX, 
               format=api.FORMAT.LOG, 
               fields=SCORP_FIELDS, 
               cloud_container=root_cfg.my_device.cc_for_system_records),
        Stream("System datastream of count data of records saved to streams", 
               api.SCORE_DS_TYPE_ID, 
               api.SCORE_STREAM_INDEX, 
               format=api.FORMAT.LOG, 
               fields=SCORE_FIELDS, 
               cloud_container=root_cfg.my_device.cc_for_system_records),
    ],
)

class SelfTracker(Sensor):
    """SelfTracking is a special Sensor class that is used to track the performance of 
    the SensorCore system.
    
    It is not a physical sensor, but is used to track the performance of the system.
    """
    def __init__(self) -> None:
        super().__init__(SC_TRACKING_CFG)
        self.last_ran: datetime = api.utc_now()

    def set_dp_engines(self, dp_engines: list[DPengine]) -> None:
        """Set the DPengine for the SelfTracking sensor.
        
        This method is called by the EdgeOrchestrator when the SelfTracking is started.
        """
        self.dp_engines = dp_engines

    def run(self) -> None:
        """Main loop for the DeviceHealth sensor.
        This method is called when the thread is started.
        It runs in a loop, logging health data and warnings at regular intervals.
        """
        logger.info(f"Starting SelfTracker thread {self!r}")

        while not self.stop_requested:
            logger.debug(f"SelfTracker {self.sensor_index} running log_sample_data() "
                         f"for {len(self.dp_engines)} DP engines")
            # Trigger each datastream to log sample counts
            for dp_engine in self.dp_engines:
                dp_engine.log_sample_data(self.last_ran)

            # Set timer for next run
            self.last_ran = api.utc_now()
            next_hour = (self.last_ran + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            sleep_time = (next_hour - self.last_ran).total_seconds()
            if root_cfg.TEST_MODE == root_cfg.MODE.TEST:
                # In test mode, sleep for 1 second to speed up tests
                sleep_time = 1
            sleep(sleep_time)
