from pathlib import Path
from time import sleep

from crontab import CronTab

from sensor_core import config_validator
from sensor_core import configuration as root_cfg
from sensor_core.config_objects import Inventory
from sensor_core.device_health import DeviceHealth
from sensor_core.edge_orchestrator import EdgeOrchestrator, request_stop
from sensor_core.utils import utils

logger = utils.setup_logger("sensor_core")

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
            root_cfg.TEST_MODE = True

    def test_configuration(self, fleet_config_py: Inventory) -> tuple[bool, list[str]]:
        """ Validates that the configuration in fleet_config_py is valid.
        """
        is_valid = False
        errors: list[str] = []

        if fleet_config_py is None:
            return (False, ["No configuration files provided."])
        
        # The fleet_config_py is a python file passed in as a class reference
        # Evaluate the class reference before we save it
        try:
            fleet_config = root_cfg.load_inventory(fleet_config_py)
            is_valid, errors = config_validator.validate(fleet_config)
        except Exception as e:
            errors = [str(e)]

        return (is_valid, errors)                


    def configure(self, fleet_config_py: Inventory, force_update: bool = False) -> None:
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
        if not force_update and self._is_running():
            raise Exception("SensorCore is running; either stop SensorCore or use force_update.")

        if fleet_config_py is None:
            raise Exception("No configuration files provided.")
        
        success, error = root_cfg.check_keys()
        if not success:
            raise Exception(error)

        # The fleet_config_py is a python file passed in as a class reference
        # Evaluate the class reference before we save it
        try:
            is_valid = False
            errors: list[str] = []
            fleet_config = root_cfg.load_inventory(fleet_config_py)
            is_valid, errors = config_validator.validate(fleet_config)
        except Exception as e:
            raise Exception(f"Error attempting to load fleet config: {e}")
        finally:
            if not is_valid:
                raise Exception(f"Configuration in {fleet_config_py} is not valid:\n{errors}")

        # Load the configuration
        root_cfg.set_inventory(fleet_config_py)

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

        logger.info("Starting SensorCore")

        # Check there isn't already a SensorCore process running
        # @@@ How?!?

        # Trigger the orchestrator to start the sensors
        # This will run the sensors in the current process
        orchestrator = EdgeOrchestrator.get_instance()
        orchestrator.load_sensors()
        orchestrator.start_all()


    def stop(self) -> None:
        """
        Stop SensorCore.
        And remove any crontab entries added by make_my_script_persistent.
        """
        # Set the STOP_SENSOR_CORE_FLAG file; this is polled by the main() method in 
        # the EdgeOrchestrator which will continue to restart the SensorCore until the flag is removed.
        request_stop()

        # Ask the EdgeOrchestrator to stop all sensors
        EdgeOrchestrator.get_instance().stop_all()
        print(f"SensorCore stopping - this may take up to {root_cfg.my_device.max_recording_timer}s.")

        # Remove from crontab
        if root_cfg.running_on_rpi:
            # Remove the cron job to restart SensorCore on reboot
            cron = CronTab(user=utils.get_current_user())  # Use the current user's crontab
            cron.remove_all(comment='Run_my_script_on_reboot')
            cron.write()


    def status(self) -> str:
        """
        Get the current status of the sensor core.

        Return:
        - A string message and a dictionary containing the status of the sensor core.
        """
        display_message = f"\nSensorCore running: {self._is_running()}"

        # Check config is clean
        success, error = root_cfg.check_keys()
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
            update_script = Path(__file__) / "utils" / "update_my_code.py"
            cron = CronTab(user=utils.get_current_user())
            cron.remove_all(comment="auto_update_code")
            job = cron.new(
                command=(f"source {Path.home()}/{root_cfg.system_cfg.venv_dir}/bin/activate && "
                         f"python3 {update_script}"),
                comment="auto_update_code",
                pre_comment=True,            
            )
            # Run every Sunday at 3am
            job.setall(root_cfg.my_device.auto_update_code_cron)
            cron.write()
            logger.info("Auto_update_code set in crontab")

        ####################################
        # Make log storage volatile to reduce wear on the SD card
        ####################################
        if root_cfg.system_cfg.enable_volatile_logs == "Yes" and root_cfg.running_on_rpi:
            # Set the systemd journal to use volatile storage
            # Ensure that the /etc/systemd/journald.conf file exists
            # That the journald_Storage key is not prefixed with a # and is set to "volatile"
            # That the journald_SystemMaxUse key is not prefixed with a # and is set to 50M
            if not Path("/etc/systemd/journald.conf").exists():
                raise FileNotFoundError("The /etc/systemd/journald.conf file does not exist.")
            
            update_required: bool = False
            with open("/etc/systemd/journald.conf", "r") as f:
                lines = f.readlines()
                for line in lines:
                    if (((line.startswith("#Storage=")) or ((line.startswith("Storage=")) 
                            and line.strip() != "Storage=volatile"))):
                        update_required = True
                        break
                    elif (((line.startswith("#SystemMaxUse=")) or ((line.startswith("SystemMaxUse=")) 
                            and line.strip() != "SystemMaxUse=50M"))):
                        update_required = True
                        break
                
            if update_required:
                with open("/etc/systemd/journald.conf", "w") as f:
                    for line in lines:
                        if (line.startswith("#Storage=")) or (line.startswith("Storage=")):
                            f.write("Storage=volatile\n")
                        elif (line.startswith("#SystemMaxUse=")) or (line.startswith("SystemMaxUse=")):
                            f.write("SystemMaxUse=50M\n")
                        else:
                            f.write(line)
                # Restart the systemd-journald service to apply the changes
                utils.run_cmd("sudo systemctl restart systemd-journald")
                logger.info("Systemd journal set to use volatile storage.")

        ####################################
        # Set predictable network interface names
        #
        # Runs: sudo raspi-config nonint do_net_names 0
        ####################################
        if root_cfg.system_cfg.enable_predictable_interface_names == "Yes" and root_cfg.running_on_rpi:
            # Set predictable network interface names
            utils.run_cmd("sudo raspi-config nonint do_net_names 0")
            logger.info("Predictable network interface names set.")

        ####################################
        # Enable the I2C interface
        #
        # Runs:	sudo raspi-config nonint do_i2c 0
        ####################################
        if root_cfg.system_cfg.enable_i2c == "Yes" and root_cfg.running_on_rpi:
            # Enable the I2C interface
            utils.run_cmd("sudo raspi-config nonint do_i2c 0")
            logger.info("I2C interface enabled.")

        ######################################
        # Install the UFW firewall
        #
        # Reset the firewall to default settings
        #  sudo ufw --force reset
        # Allow IGMP broadcast traffic
        #  sudo ufw allow proto igmp from any to 224.0.0.1
        #  sudo ufw allow proto igmp from any to 224.0.0.251
        # Allow SSH on 22
        #  sudo ufw allow 22
        # Allow HTTPS on 443
        #  sudo ufw allow 443
        # Re-enable the firewall
        #  sudo ufw --force enable
        ######################################
        if root_cfg.system_cfg.enable_firewall == "Yes" and root_cfg.running_on_rpi:
            # Install the UFW firewall
            # Check if UFW is already installed
            if not utils.run_cmd("sudo ufw status", ignore_errors=True).startswith("Status: active"):
                # Install UFW
                utils.run_cmd("sudo apt install ufw -y")
                logger.info("UFW firewall installed.")
            else:
                logger.info("UFW firewall already installed.")

            # Reset the firewall to default settings
            utils.run_cmd("sudo ufw --force reset")
            utils.run_cmd("sudo ufw allow proto igmp from any to 224.0.0.1")
            utils.run_cmd("sudo ufw allow proto igmp from any to 224.0.0.251")
            utils.run_cmd("sudo ufw allow 22")
            utils.run_cmd("sudo ufw allow 443")
            utils.run_cmd("sudo ufw --force enable")
            # Check if UFW is enabled
            if not utils.run_cmd("sudo ufw status", ignore_errors=True).startswith("Status: active"):
                logger.error("UFW firewall not enabled.")
            else: 
                logger.info("UFW firewall installed and enabled.")

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
            is_running = utils.is_already_running(".sensor_core")
        elif root_cfg.running_on_windows:
            orchestrator = EdgeOrchestrator.get_instance()
            is_running = orchestrator.orchestrator_is_running

        return is_running

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

        from crontab import CronTab
        cron = CronTab(user=utils.get_current_user())
        cron.remove_all(comment='Run_my_script_on_reboot')
        restart_on_reboot_cmd=(f"source {Path.home()}/{root_cfg.system_cfg.venv_dir}/bin/activate && "
                               f"python3 {my_script}"),
        job = cron.new(command=restart_on_reboot_cmd, 
                       comment='Run_my_script_on_reboot',
                       pre_comment=True)
        job.every_reboot()
        cron.write()
        logger.info("Cron job added to run this script on reboot.")
