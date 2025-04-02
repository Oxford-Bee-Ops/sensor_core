import sys
from time import sleep

from crontab import CronTab

from sensor_core import config_validator
from sensor_core import configuration as root_cfg
from sensor_core.device_health import DeviceHealth
from sensor_core.edge_orchestrator import EdgeOrchestrator, request_stop
from sensor_core.utils import dc, utils

logger = utils.setup_logger("sensor_core")

####################################################################################################
# SensorCore provides the public interface to the sensor_core module.
# It is the entry point for users to configure and start the sensor_core.
# Since the SensorCore may already be running (for example from boot in crontab), we can't assume
# that this is the only instance of SensorCore running.
# Therefore, all actions need to be taken indirectly via file flags or system calls.
####################################################################################################


class SensorCore:
    """
    SensorCore provides the public interface to the sensor_core module.
    """
    # We make the location of the keys file a public variable so that users can reference
    # it in their own code.
    KEYS_FILE = root_cfg.KEYS_FILE
    SC_RUN_CMD = f"{root_cfg.SC_CODE_DIR / 'scripts' / 'run_sensors.sh'} {sys.prefix}"

    def __init__(self, test_mode: bool = False) -> None:
        if test_mode:
            root_cfg.TEST_MODE = True

    def configure(self, fleet_config_py: str, force_update: bool = False) -> None:
        """
        Set the file location of the SensorCore configuration.
        This file will be accessed immediately and when SensorCore restarts.
        See the /examples folder for configuration file templates.

        Parameters:
        - fleet_config_py: Path to the fleet configuration which is a python file
        - force_update: If True, the configuration will be reloaded and the device rebooted
            even if SensorCore is already running.

        Raises:
        - Exception: If the sensor core is running (and force_update is not set).
        - Exception: If no configuration files are provided or the files do not exist.
        """
        if not force_update and self._is_running():
            raise Exception("SensorCore is running; either stop SensorCore or use force_update.")

        if fleet_config_py is None:
            raise Exception("No configuration files provided.")
        
        success, error = self.check_keys()
        if not success:
            raise Exception(error)

        # The fleet_config_py is a python file passed in as a class reference
        # Evaluate the class reference before we save it
        try:
            is_valid = False
            errors: list[str] = []
            fleet_config = root_cfg._load_inventory(fleet_config_py)
            assert fleet_config is not None
            is_valid, errors = config_validator.validate(fleet_config)
        except Exception as e:
            raise Exception(f"Error attempting to load fleet config: {e}")
        finally:
            if not is_valid:
                raise Exception(f"Configuration in {fleet_config_py} is not valid: {errors}")

        # Save the configuration file location so we can re-use it when we restart
        if root_cfg.system_cfg is None:
            root_cfg.system_cfg = root_cfg.SystemCfg()
        root_cfg.system_cfg.inventory_class = str(fleet_config_py)
        dc.save_settings_to_env(root_cfg.system_cfg, root_cfg.SYSTEM_CFG_FILE)

        # Load the configuration
        root_cfg.reload_inventory()

        # Restart the SensorCore if it is running
        if self._is_running():
            print("##########################################################")
            print("# SensorCore is running but force_update has been used.")
            print("# The system will reboot in 5 seconds.")
            print("##########################################################")
            self.stop()
            sleep(5)
            utils.run_cmd("sudo reboot")

    def start(self) -> None:
        """
        Start the sensor_core to begin data collection.

        Raises:
        - Exception: If the sensor core is not configured.
        """
        if not self._is_configured() or root_cfg.system_cfg is None:
            raise Exception("SensorCore must be configured before starting.")

        if root_cfg.running_on_rpi:
            # We start the SensorCore as a separate python process so that we 
            # don't want to kill it when we exit this thread.
            utils.run_cmd(SensorCore.SC_RUN_CMD)
            logger.info("SensorCore started.")

            # We want to make SensorCore persistent over reboot, so we add a cron job
            if root_cfg.system_cfg.auto_start_on_install == "Yes":
                # Ensure there is a cron job to restart SensorCore on reboot
                cron = CronTab(user=utils.get_current_user())
                job = cron.new(command=SensorCore.SC_RUN_CMD, comment='SensoreCore start on reboot')
                job.every_reboot()
                cron.write()
                logger.info("Cron job added to run the script on reboot.")

        elif root_cfg.running_on_windows:
            logger.info("SensorCore in test mode on Windows.")
            # Trigger the orchestrator to start the sensors
            # This will run the sensors in the current process
            # This is for testing purposes only
            orchestrator = EdgeOrchestrator.get_instance()
            orchestrator.load_sensors()
            orchestrator.start_all()
        else:
            raise Exception("SensorCore is not supported on this platform.")

    def stop(self) -> None:
        """
        Stop SensorCore.
        """
        # Set the STOP_SENSOR_CORE_FLAG file; this is polled by the main() method in 
        # the EdgeOrchestrator which will continue to restart the SensorCore until the flag is removed.
        request_stop()

        # Remove from crontab
        if root_cfg.running_on_rpi:
            # Remove the cron job to restart SensorCore on reboot
            cron = CronTab(user=utils.get_current_user())  # Use the current user's crontab
            cron.remove_all(command=SensorCore.SC_RUN_CMD, comment='SensoreCore start on reboot')
            cron.write()

        # Ask the EdgeOrchestrator to stop all sensors
        EdgeOrchestrator.get_instance().stop_all()
        print(f"SensorCore stopping - this may take up to {root_cfg.my_device.max_recording_timer}s.")


    def status(self) -> str:
        """
        Get the current status of the sensor core.

        Return:
        - A string message and a dictionary containing the status of the sensor core.
        """
        display_message = f"\nSensorCore running: {self._is_running()}"

        # Check config is clean
        success, error = self.check_keys()
        if not success:
            display_message += f"\n\n{error}"

        # Display the orchestrator status
        orchestrator = EdgeOrchestrator.get_instance()
        if orchestrator is not None:
            status = orchestrator.status()
            if status:
                display_message += "\n\n# SENSOR CORE STATUS\n"
                for key, value in status.items():
                    # Left pad the key to 24 characters
                    display_message += f"  {key:<24} {value}\n"

        # Get the device health
        health = DeviceHealth.get_health()

        if health:
            display_message += "\n\n# DEVICE HEALTH\n"
            for key, value in health.items():
                # Left pad the key to 24 characters
                display_message += f"  {key:<24} {value}\n"

        return display_message

    def check_keys(self) -> tuple[bool, str]:
        """Check the keys.env file exists and has loaded.  
        Provided a helpful display string if not."""
        root_cfg.CFG_DIR.mkdir(parents=True, exist_ok=True)
        success = False
        error = ""
        if not root_cfg.KEYS_FILE.exists():
            error = (f"Keys file {root_cfg.KEYS_FILE} does not exist. "
                     f"Please create it and set the 'cloud_storage_key' key.")
        elif (root_cfg.KEYS_FILE.exists()) and (
            (root_cfg.keys is None
            ) or (root_cfg.keys.cloud_storage_key is None
            ) or (root_cfg.keys.cloud_storage_key == root_cfg.FAILED_TO_LOAD)
            ):
            error = f"Keys file {root_cfg.KEYS_FILE} exists but 'cloud_storage_key' key not set."
        else:
            success = True
            error = "Keys loaded successfully."

        return success, error

    def display_configuration(self) -> str:
        """
        Display the current configuration of the sensor core.

        Return:
        - A string message containing the configuration of the sensor core.
        """
        display_message = f"\nConfiguration:\n{root_cfg.my_device.display()}"

        return display_message

    def update(self) -> None:
        """
        Update sensor_core to the latest released code.
        If the code is updated, the device will reboot to apply the changes.
        """
        print("@@@NOT IMPLEMENTED YET.")

    def _is_running(self)-> bool:
        """Check if an instance of SensorCore is running."""
        is_running = False

        if root_cfg.running_on_rpi:
            is_running = utils.is_already_running("sensor_core")
        elif root_cfg.running_on_windows:
            orchestrator = EdgeOrchestrator.get_instance()
            is_running = orchestrator.orchestrator_is_running

        return is_running

    @staticmethod
    def _is_configured() -> bool:
        """Check if SensorCore is configured."""
        # Test for the presence of the SC_CONFIG_FILE file
        return root_cfg.SYSTEM_CFG_FILE.exists()
