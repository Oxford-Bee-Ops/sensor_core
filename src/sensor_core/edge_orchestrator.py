####################################################################################################
# EdgeOrchestrator: Manages the state of the sensor threads
####################################################################################################
import sys
import threading
import zipfile
from dataclasses import asdict
from datetime import timedelta
from time import sleep
from typing import Optional

from sensor_core import api, datastream
from sensor_core import configuration as root_cfg
from sensor_core.cloud_connector import CloudConnector
from sensor_core.datastream import Datastream, JournalPool
from sensor_core.device_health import DeviceHealth
from sensor_core.sensor import Sensor
from sensor_core.system_datastreams import (
    FAIRY_DS_TYPE,
    HEART_DS_TYPE,
    SCORE_DS_TYPE,
    SCORP_DS_TYPE,
    WARNING_DS_TYPE,
)
from sensor_core.utils import file_naming, utils

logger = utils.setup_logger("sensor_core")


class EdgeOrchestrator:
    """The EdgeOrchestrator manages the state of the sensors and their associated Datastreams.

    Started by the SensorFactory, which creates the sensors and registers them with the EdgeOrchestrator.
    The EdgeOrchestrator:
    - interrogates the Sensor to get its Datastreams
    - starts the Sensor and Datastream threads
    - starts an observability thread to monitor the performance of the SensorCore
    """

    _instance = None
    orchestrator_lock = threading.RLock()  # Re-entrant lock to ensure thread-safety
    upload_lock = threading.RLock()
    root_cfg.set_mode(root_cfg.Mode.EDGE)

    def __new__(cls, *args, **kwargs): # type: ignore
        if not cls._instance:
            cls._instance = super(EdgeOrchestrator, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self) -> None:
        logger.info(f"Initialising EdgeOrchestrator {self!r}")

        self.reset_orchestrator_state()
        if root_cfg.TEST_MODE:
            # Override the RUN_FREQUENCY_SECS so that tests exit faster; default is 60s
            datastream.RUN_FREQUENCY_SECS = 1
        logger.info(f"Initialised EdgeOrchestrator {self!r}")

    @staticmethod
    def get_instance() -> "EdgeOrchestrator":
        """Get the singleton instance of the EdgeOrchestrator"""
        with EdgeOrchestrator.orchestrator_lock:
            if EdgeOrchestrator._instance is None:
                EdgeOrchestrator._instance = EdgeOrchestrator()

        return EdgeOrchestrator._instance

    def reset_orchestrator_state(self) -> None:
        logger.info("Reset orchestrator state")
        with EdgeOrchestrator.orchestrator_lock:
            self._sensorThreads: list[Sensor] = []
            self._datastreams: list[Datastream] = []
            self._datastream_sensor_map: dict[Datastream, Sensor] = {}

            self._stop_observability_requested = threading.Event()
            self._observability_timer: Optional[threading.Timer] = None

            self._stop_upload_requested = threading.Event()
            self._upload_timer: Optional[threading.Timer] = None

            # We create a series of special Datastreams for recording:
            # SCORE - data save events
            # SCORP - DP performance
            # FAIRY - DS FAIR config records
            # HEART - device health
            # WARNING - captures error & warning logs
            # Register it so that it's started / stopped with the rest.
            # Set it in the superclass, so Datastreams can log to it.
            self._score_ds = Datastream(SCORE_DS_TYPE, device_id=root_cfg.my_device_id, sensor_index=1)
            self._scorp_ds = Datastream(SCORP_DS_TYPE, device_id=root_cfg.my_device_id, sensor_index=1)
            self._fairy_ds = Datastream(FAIRY_DS_TYPE, device_id=root_cfg.my_device_id, sensor_index=1)
            self._heart_ds = Datastream(HEART_DS_TYPE, device_id=root_cfg.my_device_id, sensor_index=1)
            self._warning_ds = Datastream(WARNING_DS_TYPE, device_id=root_cfg.my_device_id, sensor_index=1)
            self._register_datastreams([self._scorp_ds, self._score_ds, self._fairy_ds, self._heart_ds, 
                                        self._warning_ds])
            Datastream._set_special_dss(self._scorp_ds, self._score_ds, self._fairy_ds)

            # Create the DeviceHealth object
            self.device_health = DeviceHealth()

            self.orchestrator_is_running = False


    def status(self) -> dict[str, str]:
        """Return a key-value status describing the state of the EdgeOrchestrator"""
        status = {
            "Orchestrator running": str(self.orchestrator_is_running),
            "Sensor threads": str(self._sensorThreads),
            "Observability timer": str(self._observability_timer),
            "Upload timer": str(self._upload_timer),
            "Datastream-Sensor map": str(self._datastream_sensor_map),
            "Datastreams": str(self._datastreams),
        }
        return status

    def load_sensors(self) -> None:
        """Load the sensors based on the configuration and register them with the EdgeOrchestrator"""

        if root_cfg.my_device.sensor_ds_list:
            if not isinstance(root_cfg.my_device.sensor_ds_list, list):
                root_cfg.my_device.sensor_ds_list = [root_cfg.my_device.sensor_ds_list]

            # Loop around the SensorDatastreamConfig objects and instantiate the appropriate ones
            for sds_config in root_cfg.my_device.sensor_ds_list:
                sensor_cfg = sds_config.sensor_cfg
                logger.info(
                    f"Instantiating a {sensor_cfg.sensor_type} sensor on {root_cfg.my_device_id}"
                    f" with sensor_index {sensor_cfg.sensor_index} using {sensor_cfg.sensor_class_ref}"
                )
                # Create a new sensor instance of the appropriate type
                # sensor_cfg.sensor_class_ref is a class identifier str, so we need to call it to instantiate
                # We pass in an index that can be used as the sensor_index, but it is up to the class
                # __init__ to use the index as it sees fit.
                try:
                    sensor = utils.get_class_instance(sensor_cfg.sensor_class_ref, sds_config)
                except Exception:
                    logger.error(f"Failed to instantiate {sensor_cfg.sensor_class_ref}")
                    continue

                if sensor is not None:
                    self._register_sensor(sensor)
        else:
            logger.error(f"No sensor_ds_list defined for {root_cfg.my_device}")


    #########################################################################################################
    #
    # Sensor interface
    #
    #########################################################################################################
    def sensor_failed(self, sensor: Sensor) -> None:
        """Called by Sensor to indicate that it has failed; orchestrator will then restarting everything."""
        logger.error(f"{utils.RAISE_WARN()}Sensor failed; restarting all; {sensor}")
        logger.info(self.status())
        self.stop_all()
        # The orchestrator monitors it's own status and will re-register all Sensors and Datastreams.

    def _register_sensor(self, sensor: Sensor) -> None:
        """Called by the Orchestrator to register newly created Sensors with the EdgeOrchestrator"""

        logger.info(f"Register Sensor {sensor!r} to EdgeOrchestrator {self!r}")
        assert isinstance(sensor, Sensor)
        if sensor in self._sensorThreads:
            logger.info(self.status())
            raise ValueError(f"Sensor already added: {sensor!r}")

        # Get the datastreams from the Sensor
        datastreams = sensor.create_datastreams(sensor.sds_config.datastream_cfgs)
        self._datastream_sensor_map.update({ds: sensor for ds in datastreams})
        self._register_datastreams(datastreams)

        self._sensorThreads.append(sensor)

    def _get_sensor(self, sensor_type: str, sensor_index: int) -> Optional[Sensor| None]:
        """Private method to get a sensor by type & index"""
        logger.debug(f"_get_sensor {sensor_type} {sensor_index} from {self._sensorThreads}")
        for sensor in self._sensorThreads:
            if (sensor._sensor_type == sensor_type) and (sensor._sensor_index == sensor_index):
                return sensor
        return None


    #########################################################################################################
    #
    # Private methods for management of Datastreams
    #
    #########################################################################################################
    def _register_datastreams(self, datastreams: list[Datastream]) -> None:
        """Register a Sensor's Datastreams"""

        with EdgeOrchestrator.orchestrator_lock:
            # Check the datastreams aren't already registered
            if any(ds in self._datastreams for ds in datastreams):  # Avoid infinite recursion
                logger.info(f"1 or more Datastreams already registered, exiting: {datastreams}")
                return

            logger.info(f"Registering datastreams with EdgeOrchestrator: {datastreams}")

            # Register the datastreams
            if len(datastreams) > 0:
                self._datastreams.extend(datastreams)

            """
            # Interrogate the datastreams to see if they have any derived datastreams
            # This is recursive, so we can have multiple levels of derived datastreams
            # But we need to make sure it's not infinitely recursive!
            for ds in datastreams:
                if ds._edge_dps is None:
                    continue
                for dp in ds._edge_dps:
                    derived_datastreams = dp.create_derived_datastreams()
                    if derived_datastreams is None:
                        continue
                    if any(ds in self._datastreams for ds in derived_datastreams):
                        logger.info("Completed recursion")
                    else:
                        # Register the derived datastreams
                        logger.info(f"Register derived datastreams: {derived_datastreams}")
                        self._register_datastreams(derived_datastreams)
            """

    #########################################################################################################
    #
    # Management of Sensor and Datastream threads
    #
    #########################################################################################################
    def start_all(self) -> None:
        """Start all Sensor, Datastream and observability threads"""

        if self.orchestrator_is_running:
            logger.warning(f"Sensor_manager is already running; {self}")
            logger.info(self.status())
            return
        self.orchestrator_is_stopping = False

        # Set the flag monitored by the SensorFactory
        self.orchestrator_is_running = True

        # Start the Datastreams threads
        # Start FAIRY first, so that we can use it to save other FAIR archive records
        self._fairy_ds.start()
        for ds in self._datastreams:
            # We exclude the FAIRY as it's explicitly started first
            if ds.ds_config.ds_type_id == FAIRY_DS_TYPE.ds_type_id:
                continue

            # Write a log message to record that the datastream has started.
            # This creates the FAIR config record that is archived along with the data.
            ds_dict = asdict(ds.ds_config)  # DS config
            ds_dict["ds_status_update"] = api.DS_STARTED
            if ds in self._datastream_sensor_map:
                sensor_config = self._datastream_sensor_map[ds].sds_config.sensor_cfg
                ds_dict["sensor_config"] = asdict(sensor_config)
            ds.save_FAIR_record(ds_dict)
            ds.start()

        # Only once we've started the datastreams, do we start the Sensor threads
        # otherwise we get a "Datastream not started" error.
        for sensor in self._sensorThreads:
            sensor.start()

        # We also start our own threads to periodically log the sample counts
        self.start_observability_timer()
        self.start_upload_timer()

        # Dump status to log
        logger.info(f"EdgeOrchestrator started: {self.status()}")

    def stop_all(self) -> None:
        """Stop all Sensor, Datastream and observability threads

        Blocks until all threads have exited"""

        logger.info(f"stop_all on {self!r} called by {threading.current_thread().name}")
        logger.info(f"stop_all stack: {utils.get_stack_info()}")

        self.orchestrator_is_stopping = True

        if not self.orchestrator_is_running:
            logger.warning(f"EdgeOrchestrator not started when stop called; {self}")
            logger.info(self.status())
            if self._observability_timer:
                self.stop_observability_timer()
            if self._upload_timer:  
                self.stop_upload_timer()
            self.reset_orchestrator_state()
            return

        # Stop our own observability Timer
        self.stop_observability_timer()
        self.stop_upload_timer()

        # Stop all the sensor threads
        for sensor in self._sensorThreads:
            sensor.stop()

        # Block until all Sensor threads have exited
        for sensor in self._sensorThreads:
            # We need the check that the thread we're waiting on is not our own thread,
            # because that will cause a RuntimeError
            our_thread = threading.current_thread().ident
            if (sensor.ident != our_thread) and sensor.is_alive():
                logger.info(f"Waiting for sensor thread {sensor}")
                sensor.join()

        # Stop all the datastream threads
        for ds in self._datastreams:
            ds.stop()

        # Block until all Datastreams have exited
        for ds in self._datastreams:
            if ds.is_alive():
                logger.info(f"Waiting for datastream thread {ds}")
                ds.join()
            else:
                logger.info(f"Datastream thread {ds} already stopped")

        # Trigger a flush_all on the CloudJournals so we save collected information 
        # before we kill everything
        jp = JournalPool.get(root_cfg.Mode.EDGE)
        jp.flush_journals()
        jp.stop()
        

        # Clear our thread lists
        self.reset_orchestrator_state()
        self.orchestrator_is_running = False
        logger.info("Stopped all sensors and datastreams")

    def is_stop_requested(self) -> bool:
        """Check if a stop has been manually requested by the user.
        This function is polled by the SensorFactory main thread to check if the user has requested a stop."""
        stop_requested = root_cfg.STOP_SENSOR_CORE_FLAG.exists()
        if stop_requested and not self.orchestrator_is_stopping:
            self.stop_all()

        return stop_requested

    #########################################################################################################
    #
    # Observability thread & methods
    #
    #########################################################################################################
    def start_observability_timer(self) -> None:
        logger.debug("Start obs timer")
        self._observability_period_start_time = api.utc_now()
        self.observability_run()

    def stop_observability_timer(self) -> None:
        logger.debug("Stop obs timer")
        self._stop_observability_requested.set()
        if self._observability_timer:
            self._observability_timer.cancel()

    def schedule_next_obs_run(self) -> None:
        logger.debug("Schedule next obs timer")
        if not self._stop_observability_requested.is_set():
            start_time = api.utc_now()
            next_hour = (start_time + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            sleep_time = (next_hour - start_time).total_seconds()
            self._observability_period_start_time = start_time
            if self._observability_timer:
                self._observability_timer.cancel()
            self._observability_timer = threading.Timer(sleep_time, self.observability_run)
            self._observability_timer.name = "obs_timer"
            self._observability_timer.start()

    # We run a thread to dump observability data on an hourly basis
    def observability_run(self) -> None:
        logger.debug(f"observability_run for period starting {self._observability_period_start_time}")

        # Trigger each datastream to log sample counts
        for ds in self._datastreams:
            ds.log_sample_data(self._observability_period_start_time)

        # Trigger the HEART and WARNING datastreams to log device health
        self.device_health.log_health(self._heart_ds)
        self.device_health.log_warnings(self._warning_ds)

        # In testing, we call this method directly.  In this case, we don't want
        # to schedule another Timer because we get hanging threads.
        # So check if we've been scheduled (ie isAlive) or called directly.
        self.schedule_next_obs_run()

    ########################################################################################################
    #
    # Data upload to cloud
    #
    ########################################################################################################
    def start_upload_timer(self) -> None:
        logger.debug("Start upload timer")
        self.schedule_next_upload_run()

    def stop_upload_timer(self) -> None:
        logger.debug("Stop upload timer")
        self._stop_upload_requested.set()
        if self._upload_timer:
            self._upload_timer.cancel()

    def schedule_next_upload_run(self) -> None:
        logger.debug("Schedule next upload timer")
        if not self._stop_upload_requested.is_set():
            self._upload_timer = threading.Timer(30 * 60, self.check_upload_status)
            self._upload_timer.name = "upload_timer"
            self._upload_timer.start()

    def upload_to_cloud(self, dst_container: Optional[str] = None) -> None:
        """Method to zip up all sensor data and upload it to the cloud

        Looks for all files in the root_cfg.EDGE_UPLOAD_DIR except zip files.
        This should include all raw recordings that are to be uploaded.
        We flush the JournalPool to ensure all data is saved to disk in the EDGE_UPLOAD_DIR.
        """

        logger.debug("Upload from edge device to cloud")

        # First, flush all the data from the JournalPool (which saves the files to the EDGE_UPLOAD_DIR)
        # This is synchronous, but it locks all the journals until it completes.
        JournalPool.get(root_cfg.Mode.EDGE).flush_journals()

        files_to_zip = root_cfg.EDGE_UPLOAD_DIR.glob("*")
        zip_filename = file_naming.get_zip_filename()

        with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file in files_to_zip:
                if not file.is_file():
                    continue
                if file.suffix.endswith("zip"):
                    continue
                logger.debug(f"Add {file} to zip archive")
                zipf.write(file, file.name)
                # Delete the file after adding it to the zip archive
                file.unlink()

        logger.info(f"Created zip file: {zip_filename}")

        # Now upload all zipfiles to cloud storage
        # We explcitly get all zip files (rather than just the one we created) in case there are any left over
        # from previous failed uploads.
        if dst_container is None:
            dst_container = root_cfg.my_device.cc_for_upload
        connector = CloudConnector()
        zip_files = list(root_cfg.EDGE_UPLOAD_DIR.glob("*.zip"))
        connector.upload_to_cloud(dst_container, zip_files)

    def check_upload_status(self) -> None:
        """Method called by a timer to check storage capacity and call upload_to_cloud if required

        We upload_to_cloud every 30mins or if storage space is running low"""

        logger.debug("Check upload status")
        # @@@ For now, just call upload_to_cloud.  Need to implement storage space checking
        self.upload_to_cloud()
        if self._upload_timer is not None and self._upload_timer.is_alive():
            self.schedule_next_upload_run()

#############################################################################################################
# Orchestrator main loop
#
# Main loop called from crontab on boot up
#############################################################################################################
def request_stop() -> None:
    """Request all sensors to stop and SensorCore to exit"""
    root_cfg.STOP_SENSOR_CORE_FLAG.touch()


def main() -> None:
    try:
        # Check the "stop" file has been cleared
        root_cfg.STOP_SENSOR_CORE_FLAG.unlink(missing_ok=True)

        # Provide diagnostics
        logger.info(root_cfg.my_device.display())

        orchestrator = EdgeOrchestrator.get_instance()
        assert not orchestrator.orchestrator_is_running, "Sensor manager should not be running"
        orchestrator.load_sensors()

        # Start all the sensor threads
        orchestrator.start_all()

        # Keep the main thread alive
        while not orchestrator.is_stop_requested():
            sleep(1)

            # Restart the re-load and re-start the EdgeOrchestrator if it fails.
            if not orchestrator.orchestrator_is_running:
                logger.error("Sensor manager has stopped; restarting")
                orchestrator.load_sensors()
                orchestrator.start_all()

    except Exception as e:
        logger.error(
            f"{utils.RAISE_WARN()}({root_cfg.my_device_id}) Sensor exception: {e!s}",
            exc_info=True,
        )
    finally:
        # To get here, we hit an exception on one thread or have been explicitly asked to stop.
        # Tell all threads to terminate so we can cleanly restart all via cron
        if orchestrator is not None:
            orchestrator.stop_all()
        logger.info("Sensor script finished")


#############################################################################################################
# Main
#
# Use cfg to determine which sensors are installed on this device, and start the appropriate threads
#############################################################################################################
# Main loop called from crontab on boot up
if __name__ == "__main__":
    # Check if an instance of this script is already running
    if utils.is_already_running("sensor_core"):
        logger.warning("Sensor.py script already running; exiting")
        sys.exit(0)

    print("Starting EdgeOrchestrator")
    main()
