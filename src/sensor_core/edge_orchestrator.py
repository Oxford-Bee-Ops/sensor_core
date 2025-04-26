####################################################################################################
# EdgeOrchestrator: Manages the state of the sensor threads
####################################################################################################
import threading
import zipfile
from datetime import timedelta
from time import sleep
from typing import Callable, Optional

from sensor_core import api, dp_engine, file_naming
from sensor_core import configuration as root_cfg
from sensor_core.cloud_connector import CloudConnector
from sensor_core.device_health import DeviceHealth
from sensor_core.dp_engine import DPengine
from sensor_core.dp_tree import DPtree
from sensor_core.dp_tree_node import DPtreeNode
from sensor_core.stats_tracker import StatTracker
from sensor_core.sensor import Sensor
from sensor_core.utils.journal_pool import JournalPool

logger = root_cfg.setup_logger("sensor_core")

# Seconds between polls of is_stop_requested / touch is_running flag
WATCHDOG_FREQUENCY = 1  

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
        if root_cfg.TEST_MODE == root_cfg.MODE.TEST:
            # Override the RUN_FREQUENCY_SECS so that tests exit faster; default is 60s
            dp_engine.RUN_FREQUENCY_SECS = 1
        logger.info(f"Initialised EdgeOrchestrator {self!r}")

    @staticmethod
    def get_instance() -> "EdgeOrchestrator":
        """Get the singleton instance of the EdgeOrchestrator"""
        with EdgeOrchestrator.orchestrator_lock:
            if EdgeOrchestrator._instance is None:
                EdgeOrchestrator._instance = EdgeOrchestrator()

        return EdgeOrchestrator._instance

    def reset_orchestrator_state(self) -> None:
        logger.debug("Reset orchestrator state")
        with EdgeOrchestrator.orchestrator_lock:
            self._sensorThreads: list[Sensor] = []
            self._dpengines: list[DPengine] = []
            self.dp_trees: list[DPtree] = []

            self._stop_upload_requested = threading.Event()
            self._upload_timer: Optional[threading.Timer] = None

            # We create a series of special Datastreams for recording:
            # HEART - device health
            # WARNING - captures error & warning logs
            # SCORE - data save events
            # SCORP - DP performance
            self.device_health = DeviceHealth()
            health_dpe = DPengine(DPtree(self.device_health))
            self._sensorThreads.append(self.device_health)
            self._dpengines.append(health_dpe)

            self.selftracker = StatTracker()
            tracker_dpe = DPengine(DPtree(self.selftracker))
            self._sensorThreads.append(self.selftracker)
            self._dpengines.append(tracker_dpe)
            self.selftracker.set_dp_engines(self._dpengines)
            # We set the _selftracker as a class variable so that all DPtreeNoes instances can 
            # log their performance data
            DPtreeNode._selftracker = self.selftracker

            self._orchestrator_is_running = False


    def status(self) -> dict[str, str]:
        """Return a key-value status describing the state of the EdgeOrchestrator"""
        status = {
            "SensorCore running": str(self.is_running()),
            "Sensor threads": str(self._sensorThreads),
            "Upload timer": str(self._upload_timer),
            "DPtrees": str(self._dpengines),
        }
        return status

    def load_config(self) -> None:
        """Load the sensor and data processor config into the EdgeOrchestrator by calling
        the DeviceCfg.dp_trees_create_method()."""
        self.dp_trees = self._safe_call_create_method(root_cfg.my_device.dp_trees_create_method)
        for dptree in self.dp_trees:
            sensor = dptree.sensor
            if sensor in self._sensorThreads:
                logger.error(f"{root_cfg.RAISE_WARN()}Sensor already added: {sensor!r}")
                logger.info(self.status())
                raise ValueError(f"Sensor already added: {sensor!r}")
            self._sensorThreads.append(sensor)
            self._dpengines.append(DPengine(dptree))

    @staticmethod
    def _safe_call_create_method(create_method: Optional[Callable]) -> list[DPtree]:
        """Call the create method and return the DPtree object.
        Raises ValueError if the create method does not successfully create any DPtree objects."""
        if create_method is None:
            logger.error(f"{root_cfg.RAISE_WARN()}create_method not defined for {root_cfg.my_device_id}")
            raise ValueError(f"create_method not defined for {root_cfg.my_device_id}")

        logger.info(f"Creating DP trees for {root_cfg.my_device_id} using {create_method}")
        dp_trees: list[DPtree] = create_method()

        if not dp_trees:
            logger.error(f"{root_cfg.RAISE_WARN()}No sensors created by {root_cfg.my_device_id} "
                            f"{create_method}")
            raise ValueError(f"No sensors created by {create_method}")

        if not isinstance(dp_trees, list):
            logger.error(f"{root_cfg.RAISE_WARN()}create_method must return a list; "
                         f"created {dp_trees.__type__}")
            raise ValueError("create_method must return a list of DPtree objects")
        
        return dp_trees

    #########################################################################################################
    #
    # Sensor interface
    #
    #########################################################################################################
    def sensor_failed(self, sensor: Sensor) -> None:
        """Called by Sensor to indicate that it has failed; orchestrator will then restarting everything."""
        logger.error(f"{root_cfg.RAISE_WARN()}Sensor failed; restarting all; {sensor}")
        logger.info(self.status())
        self.stop_all(restart=True)
        # The orchestrator monitors it's own status and will re-register all Sensors and Datastreams.


    def _get_sensor(self, sensor_type: api.SENSOR_TYPE, sensor_index: int) -> Optional[Sensor | None]:
        """Private method to get a sensor by type & index"""
        logger.debug(f"_get_sensor {sensor_type} {sensor_index} from {self._sensorThreads}")
        for sensor in self._sensorThreads:
            if (sensor.config.sensor_type == sensor_type) and (sensor.sensor_index == sensor_index):
                return sensor
        return None


    #########################################################################################################
    #
    # Management of Sensor and Datastream threads
    #
    #########################################################################################################
    def start_all(self) -> None:
        """Start all Sensor & DPengine threads"""

        if self._orchestrator_is_running:
            logger.warning(f"Sensor_manager is already running; {self}")
            logger.info(self.status())
            return

        # Check the "stop" file has been cleared
        root_cfg.STOP_SENSOR_CORE_FLAG.unlink(missing_ok=True)
        self.orchestrator_is_stopping = False

        # Set the flag monitored by the SensorFactory
        self._orchestrator_is_running = True

        # Start the DPengine threads
        for dpe in self._dpengines:
            dpe.start()

        # Only once we've started the datastreams, do we start the Sensor threads
        # otherwise we get a "Datastream not started" error.
        for sensor in self._sensorThreads:
            sensor.start()

        # Start the upload timer to sweep data to the cloud that isn't uploaded directly
        self.start_upload_timer()

        # Dump status to log
        logger.info(f"EdgeOrchestrator started: {self.status()}")

    @staticmethod
    def start_all_with_watchdog() -> None:
        """This function starts the orchestrator and maintains it with a watchdog.
        This is a non-blocking function that starts a new thread and returns.
        It calls the edge_orchestrator main() function."""

        logger.debug("Start orchestrator with watchdog")
        orchestrator_thread = threading.Thread(target=main, name="EdgeOrchestrator")
        orchestrator_thread.start()
        # Block for long enough for the main thread to be scheduled
        # So we avoid race conditions with subsequence calls to stop_all()
        sleep(1)

    def stop_all(self, restart: Optional[bool] = False) -> None:
        """Stop all Sensor, Datastream and observability threads

        Blocks until all threads have exited"""

        logger.info(f"stop_all on {self!r} called by {threading.current_thread().name}")

        self.orchestrator_is_stopping = True

        # Set the STOP_SENSOR_CORE_FLAG file; this is polled by the main() method in 
        # the EdgeOrchestrator which will continue to restart the SensorCore until the flag is removed.
        # This is also important when we are not the running instance of the orchestrator,
        # as the running instance will check the file and stop itself.
        if not restart:
            root_cfg.STOP_SENSOR_CORE_FLAG.touch()
        else:
            # We use stop_all to restart the orchestrator cleanly in the event of a sensor failure.
            logger.info("Restart requested; not touching stop file")

        if not self._orchestrator_is_running:
            logger.warning(f"EdgeOrchestrator not started when stop called; {self}")
            logger.info(self.status())
            if self._upload_timer:  
                self.stop_upload_timer()
            self.reset_orchestrator_state()
            return

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

        # Stop all the dataprocessor threads
        for dpe in self._dpengines:
            dpe.stop()

        # Block until all Datastreams have exited
        for dpe in self._dpengines:
            if dpe.is_alive():
                logger.info(f"Waiting for datastream thread {dpe}")
                dpe.join()
            else:
                logger.info(f"Datastream thread {dpe} already stopped")

        # Trigger a flush_all on the CloudJournals so we save collected information 
        # before we kill everything
        jp = JournalPool.get(root_cfg.Mode.EDGE)
        jp.flush_journals()
        jp.stop()
        
        # Clear our thread lists
        self.reset_orchestrator_state()
        self._orchestrator_is_running = False
        logger.info("Stopped all sensors and datastreams")

    def is_stop_requested(self) -> bool:
        """Check if a stop has been manually requested by the user.
        This function is polled by the main thread every second to check if the user has requested a stop."""
        stop_requested = root_cfg.STOP_SENSOR_CORE_FLAG.exists()
        if stop_requested:
            logger.info("is_stop_requested = True")
            if not self.orchestrator_is_stopping:
                self.stop_all()

        return stop_requested

    @staticmethod
    def is_running() -> bool:
        """Check if the SensorCore is running"""
        # If the SENSOR_CORE_IS_RUNNING_FLAG exists and was touched within the last 2x _FREQUENCY seconds,
        # and the timestamp on the file is < than the timestamp on the STOP_SENSOR_CORE_FLAG file,
        # then we are running.
        # If the file doesn't exist, we are not running.
        # If the file exists, but was not touched within the last 2x _FREQUENCY seconds, we are not running.

        if not root_cfg.SENSOR_CORE_IS_RUNNING_FLAG.exists():
            return False
        
        if (root_cfg.STOP_SENSOR_CORE_FLAG.exists() and 
            (root_cfg.STOP_SENSOR_CORE_FLAG.stat().st_mtime >
             root_cfg.SENSOR_CORE_IS_RUNNING_FLAG.stat().st_mtime)):
                return False
        
        time_threshold = api.utc_now() - timedelta(seconds=2 * WATCHDOG_FREQUENCY)
        if root_cfg.SENSOR_CORE_IS_RUNNING_FLAG.stat().st_mtime < time_threshold.timestamp():
            return False
        
        # If we get here, the file exists, was touched within the last 2x _FREQUENCY seconds,
        # and the timestamp is > than the timestamp on the STOP_SENSOR_CORE_FLAG file.
        return True

    ########################################################################################################
    #
    # Data upload to cloud
    #
    ########################################################################################################
    def start_upload_timer(self) -> None:
        logger.debug("Start upload timer")
        self.check_upload_status()

    def stop_upload_timer(self) -> None:
        logger.debug("Stop upload timer")
        self._stop_upload_requested.set()
        if self._upload_timer:
            self._upload_timer.cancel()
            self._upload_timer = None

    def schedule_next_upload_run(self) -> None:
        logger.debug("Schedule next upload timer")
        if not self._stop_upload_requested.is_set():
            if self._upload_timer:
                self._upload_timer.cancel()
            self._upload_timer = threading.Timer(30 * 60, self.check_upload_status)
            self._upload_timer.name = "upload_timer"
            self._upload_timer.start()

    def check_upload_status(self) -> None:
        """Method called by a timer to check storage capacity and call upload_to_cloud if required

        We upload_to_cloud every 30mins or if storage space is running low"""

        logger.debug("Check upload status")
        self.upload_to_cloud()
        self.schedule_next_upload_run()

    def upload_to_cloud(self, dst_container: Optional[str] = None) -> None:
        """Method to zip up sensor data and upload it to the cloud, if it's not been 
        uploaded directly.

        Looks for all files in the root_cfg.EDGE_UPLOAD_DIR except zip files.
        """

        logger.debug("Upload from edge device to cloud")

        files_to_zip = list(root_cfg.EDGE_UPLOAD_DIR.glob("*"))

        # We only want to zip files that have not been written in the last 60 seconds
        # This is to avoid zipping files that are still being written to.
        # We also don't want to zip zip files
        for file in files_to_zip:
            if not file.is_file() or file.suffix.endswith("zip"):
                files_to_zip.remove(file)
                continue
            if file.stat().st_mtime > (api.utc_now() - timedelta(seconds=60)).timestamp():
                files_to_zip.remove(file)
                continue

        if not files_to_zip:
            logger.info("No files to zip in upload_to_cloud")
            return
        
        zip_filename = file_naming.get_zip_filename()
        with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file in files_to_zip:
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
        zip_files = list(root_cfg.EDGE_UPLOAD_DIR.glob("*.zip"))
        CloudConnector.get_instance().upload_to_container(dst_container, zip_files)

#############################################################################################################
# Orchestrator main loop
#
# Main loop called from crontab on boot up
#############################################################################################################
def _touch_running_file() -> None:
    """Touch the running file to indicate that the script is running"""
    root_cfg.SENSOR_CORE_IS_RUNNING_FLAG.touch()

def main() -> None:
    try:
        # Provide diagnostics
        logger.info(root_cfg.my_device.display())

        orchestrator = EdgeOrchestrator.get_instance()
        if orchestrator.is_running() or orchestrator._orchestrator_is_running:
            logger.warning("SensorCore is already running; exiting")
            return

        orchestrator.load_config()

        # Start all the sensor threads
        orchestrator.start_all()

        # Keep the main thread alive
        while not orchestrator.is_stop_requested():
            sleep(WATCHDOG_FREQUENCY)
            _touch_running_file()

            # Restart the re-load and re-start the EdgeOrchestrator if it fails.
            if not orchestrator._orchestrator_is_running:
                logger.error("Sensor manager failed; restarting")
                orchestrator.load_config()
                orchestrator.start_all()

    except Exception as e:
        logger.error(
            f"{root_cfg.RAISE_WARN()}(Sensor exception: {e!s}",
            exc_info=True,
        )
    finally:
        # To get here, we hit an exception on one thread or have been explicitly asked to stop.
        # Tell all threads to terminate so we can cleanly restart all via cron
        if orchestrator is not None:
            logger.info("Edge orchestrator exiting; stopping all sensors and datastreams")
            orchestrator.stop_all()
        logger.info("Sensor script finished")


#############################################################################################################
# Main
#
# Use cfg to determine which sensors are installed on this device, and start the appropriate threads
#############################################################################################################
# Main loop called from crontab on boot up
if __name__ == "__main__":
    print("Starting EdgeOrchestrator")
    main()
