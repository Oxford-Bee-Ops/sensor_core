from pathlib import Path

from crontab import CronTab

from sensor_core import config_validator
from sensor_core import configuration as root_cfg
from sensor_core.config_objects import DeviceCfg
from sensor_core.device_health import DeviceHealth
from sensor_core.edge_orchestrator import EdgeOrchestrator
from sensor_core.utils import utils

logger = root_cfg.setup_logger("sensor_core")

####################################################################################################
# SensorCore provides the public interface to the sensor_core module.
# It is the entry point for users to configure and start the sensor_core.
# Since the SensorCore may already be running (for example from boot in crontab), we can't assume
# that this is the only instance of SensorCore on this device.
# Therefore, all actions need to be taken indirectly via file flags or system calls.
####################################################################################################


class SensorCore:
    """
    SensorCore provides the public interface to the sensor_core module.
    """
    # We make the location of the keys file a public variable so that users can reference
    # it in their own code.
    KEYS_FILE = root_cfg.KEYS_FILE

    def __init__(self, test_mode: bool = False) -> None:
        if test_mode:
            root_cfg.TEST_MODE = root_cfg.MODE.TEST

    def test_configuration(self, fleet_config: list[DeviceCfg]) -> tuple[bool, list[str]]:
        """ Validates that the configuration in fleet_config_py is valid.
        """
        is_valid = False
        errors: list[str] = []

        if not fleet_config:
            return (False, ["No configuration provided."])
        
        # The fleet_config_py is a python file passed in as a class reference
        # Evaluate the class reference before we save it
        try:
            is_valid, errors = config_validator.validate(fleet_config)
        except Exception as e:
            errors = [str(e)]

        return (is_valid, errors)                


    def configure(self, fleet_config: list[DeviceCfg]) -> None:
        """
        Set the SensorCore configuration.
        See the /examples folder for configuration file templates.

        Parameters:
        - fleet_config_py: Inventory class that implements get_invemtory()
        - force_update: If True, the configuration will be reloaded and the device rebooted
            even if SensorCore is already running.

        Raises:
        - Exception: If the sensor core is running (and force_update is not set).
        - Exception: If no configuration exists.
        """
        if not fleet_config:
            raise Exception("No configuration files provided.")
        
        success, error = root_cfg.check_keys()
        if not success:
            raise Exception(error)

        # The fleet_config_py is a python file passed in as a class reference
        # Evaluate the class reference before we save it
        try:
            is_valid = False
            errors: list[str] = []
            is_valid, errors = config_validator.validate(fleet_config)
        except Exception as e:
            raise Exception(f"Error attempting to load fleet config: {e}")
        finally:
            if not is_valid:
                raise Exception(f"Configuration in {fleet_config} is not valid:\n{errors}")

        # Load the configuration
        root_cfg.set_inventory(fleet_config)

    def start(self) -> None:
        """
        Start the sensor_core to begin data collection.

        Raises:
        - Exception: If the sensor core is not configured.
        """
        if not self._is_configured() or root_cfg.system_cfg is None:
            raise Exception("SensorCore must be configured before starting.")

        logger.info("Starting SensorCore")

        # Trigger the orchestrator to start the sensors
        # This will run the sensors in the current process
        EdgeOrchestrator.start_all_with_watchdog()


    def stop(self) -> None:
        """
        Stop SensorCore.
        And remove any crontab entries added by make_my_script_persistent.
        """
        # Ask the EdgeOrchestrator to stop all sensors
        print(f"SensorCore stopping - this may take up to {root_cfg.my_device.max_recording_timer}s.")
        EdgeOrchestrator.get_instance().stop_all()

        # Remove from crontab
        if root_cfg.running_on_rpi:
            # Remove the cron job to restart SensorCore on reboot
            cron = CronTab(user=utils.get_current_user())  # Use the current user's crontab
            cron.remove_all(comment='Run_my_script_on_reboot')
            cron.write()


    def status(self, verbose: bool = True) -> str:
        """
        Get the current status of the sensor core.

        Return:
        - A string describing the status of the sensor core.
        """
        display_message = "\n"

        # Check config is clean
        success, error = root_cfg.check_keys()
        if not success:
            display_message += f"\n\n{error}"

        # Display the orchestrator status
        orchestrator = EdgeOrchestrator.get_instance()
        if orchestrator is not None:
            display_message += f"\n\nSensorCore running: {orchestrator.is_running()}\n"

            if verbose:
                status = orchestrator.status()
                if status:
                    display_message += "\n\n# SENSOR CORE STATUS\n"
                    for key, value in status.items():
                        # Left pad the key to 24 characters
                        display_message += f"  {key:<24} {value}\n"

        # Get the device health
        health = DeviceHealth().get_health()

        if health:
            display_message += "\n\n# DEVICE HEALTH\n"
            for key, value in health.items():
                # Left pad the key to 24 characters
                display_message += f"  {key:<24} {value}\n"

        return display_message

    def enable_device_management(self) -> None:
        """
        Enable device management for the sensor core.
        This command can safely be re-run without causing duplicate effects.

        Starts the device_manager (and makes it persistent) to manage:
        - LED status
        - wifi

        Performs one-off operations to set up the device for long-running data collection.
        - creates crontab entry to auto update the OS
        - creates crontab entry to auto update the user code & dependencies (including SensorCore itself)
        - make log storage volatile to reduce wear on the SD card
        - enable predictable network interface names
        - enable the I2C interface
        - install and enable the UFW firewall

        """
        if not root_cfg.system_cfg:
            raise ValueError("SensorCore must be configured before enabling device management.")
        
        # We invoke the DeviceManager as a separate process so that it can persist when this
        # process exits.
        if root_cfg.running_on_rpi:
            utils.run_cmd(f"{Path.home()}/venv/bin/activate && nohup python3 -m sensor_core.device_manager &")
            logger.info("Device manager started.")

        ####################################
        # Auto-update the OS
        ####################################
        if root_cfg.my_device.auto_update_os and root_cfg.running_on_rpi:
            # Schedule the auto-update
            cron = CronTab(user=utils.get_current_user())
            cron.remove_all(comment="auto_update_os")
            job = cron.new(
                command="sudo apt update && sudo apt upgrade -y",
                comment="auto_update_os",
                pre_comment=True,
            )
            # Run every Sunday at 2am
            job.setall(root_cfg.my_device.auto_update_os_cron)
            cron.write()
            logger.info("Auto_update_os set in crontab")

        ####################################
        # Auto-update the user's code (and SensorCore code while under development)
        ####################################
        if root_cfg.my_device.auto_update_code and root_cfg.running_on_rpi:
            # Pre-checks
            check_passed = True
            venv_dir = root_cfg.system_cfg.venv_dir
            if not venv_dir:
                logger.error(f"{root_cfg.RAISE_WARN()}No virtual environment directory found")
                check_passed = False

            my_git_repo_url = root_cfg.system_cfg.my_git_repo_url
            if not my_git_repo_url:
                logger.error(f"{root_cfg.RAISE_WARN()}No git repository URL found")
                check_passed = False

            my_git_branch = root_cfg.system_cfg.my_git_branch
            if not my_git_branch:
                logger.error(f"{root_cfg.RAISE_WARN()}No git branch found")
                check_passed = False

            activate_script = root_cfg.HOME_DIR / root_cfg.system_cfg.venv_dir / "bin" / "activate"
            if not activate_script.exists():
                logger.error(f"{root_cfg.RAISE_WARN()}Virtual environment activation script not found:"
                             f" {activate_script}")
                check_passed = False

            # Run the commands to update the code
            if check_passed:
                cmd=f"source {activate_script} && pip install 'git+ssh://git@{my_git_repo_url}@{my_git_branch}'"
                cron = CronTab(user=utils.get_current_user())
                cron.remove_all(comment="auto_update_code")
                job = cron.new(
                    command=cmd,
                    comment="auto_update_code",
                    pre_comment=True,            
                )
                # Run every Sunday at 3am
                job.setall(root_cfg.my_device.auto_update_code_cron)
                cron.write()
                logger.info(f"Auto_update_code set in crontab; added {cmd}")
            else:
                logger.error(f"{root_cfg.RAISE_WARN()}Auto-update code failed pre-checks.")

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
        return EdgeOrchestrator.is_running()

    @staticmethod
    def _is_configured() -> bool:
        """Check if SensorCore is configured."""
        # Test for the presence of the SC_CONFIG_FILE file
        return root_cfg.SYSTEM_CFG_FILE.exists()

    @staticmethod    
    def update_my_device_id(new_device_id: str) -> None:
        """Function used in testing to change the device_id"""
        root_cfg.update_my_device_id(new_device_id)

    @staticmethod
    def make_my_script_persistent(my_script: Path | str) -> None:
        """Make this sensor persistent over reboot by adding a restart job in crontab.

        Parameters:
        - my_script: Path to the script to be made persistent.

        Raises:
        - ValueError: If the virtual environment is not found.
            This script assumes that a virtual environment has been created and that this script
            should be run in it's context. The virtual environment's location must be specified in system.cfg.
        """
        if not root_cfg.system_cfg:
            raise ValueError("SensorCore must be configured before making a script persistent.")

        # We assume that a virtual environment has been created and that this script
        # should be run in it's context.
        if not (Path.home() / root_cfg.system_cfg.venv_dir).exists():
            raise ValueError(f"Virtual environment not found at {Path.home() / root_cfg.system_cfg.venv_dir}"
                             "Please create venv before running this script.")

        if isinstance(my_script, str):
            my_script = Path(my_script)

        restart_on_reboot_cmd=(
            f"/bin/bash -c 'source {Path.home()}/{root_cfg.system_cfg.venv_dir}/bin/activate && "
            f"nohup python3 {my_script} 2>&1 | /usr/bin/logger -t SENSOR_CORE &'")
        try:
            from crontab import CronTab
            cron = CronTab(user=utils.get_current_user())
            cron.remove_all(comment='Run_my_script_on_reboot')
            job = cron.new(command=restart_on_reboot_cmd, 
                        comment='Run_my_script_on_reboot',
                        pre_comment=True)
            job.every_reboot()
            cron.write()
            logger.info("Cron job added to run this script on reboot.")
        except Exception as e:
            logger.error(f"{root_cfg.RAISE_WARN()}Failed to add cron job ({restart_on_reboot_cmd}): {e}", 
                         exc_info=True)
            raise ValueError(f"Failed to add cron job ({restart_on_reboot_cmd}): {e}")
