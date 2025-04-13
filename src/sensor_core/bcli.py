####################################################################################################
# Description: This script is used to run the bcli command.
####################################################################################################
import queue
import subprocess
import sys
import threading
import time

import click
from crontab import CronTab

from sensor_core import SensorCore
from sensor_core import configuration as root_cfg
from sensor_core.utils import utils
from sensor_core.utils.utils import disable_console_logging

logger = utils.setup_logger("common")

###################################################################################################
# Utility functions
###################################################################################################

# Wrapper for utils.run_cmd so that we can display error rather than throwing an exception
def run_cmd(cmd: str) -> str:
    """Run a command and return its output or an error message."""
    if not root_cfg.running_on_rpi:
        return "This command only works on a Raspberry Pi"
    try:
        return utils.run_cmd(cmd, ignore_errors=True)
    except Exception as e:
        return f"Error: {e}"


def reader(proc: subprocess.Popen, queue: queue.Queue) -> None:
    """
    Read 'stdout' from the subprocess and put it into the queue.

    Args:
        proc: The subprocess to read from.
        queue: The queue to store the output lines.
    """
    if proc.stdout:
        for line in iter(proc.stdout.readline, b""):
            queue.put(line)


def run_cmd_live_echo(cmd: str) -> str:
    """
    Run a command and echo its output in real-time.

    Args:
        cmd: The command to run.

    Returns:
        A string indicating success or an error message.
    """
    if not root_cfg.running_on_rpi:
        return "This command only works on a Raspberry Pi"
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        q: queue.Queue = queue.Queue()
        reader_thread = threading.Thread(target=reader, args=(process, q))
        reader_thread.start()

        while True:
            try:
                line = q.get(timeout=2)
                click.echo(line.decode("utf-8").strip())
            except queue.Empty:
                if process.poll() is not None:
                    break
    except Exception as e:
        return f"Error: {e}"
    return "Command executed successfully."


def check_if_setup_required() -> None:
    """Check if setup is required by verifying keys and Git repo."""
    while not check_keys_env():
        click.getchar()


def check_keys_env() -> bool:
    """
    Check if the keys.env exists in ./sensor_core and is not empty.

    Returns:
        True if the keys.env file exists and is valid, False otherwise.
    """
    success, error = root_cfg.check_keys()
    if success:
        return True
    else:    
        # Help the user setup keys
        click.echo("\n###############################################")
        click.echo(f"# {error}")
        click.echo("# ")
        click.echo(f"# Create a file called {root_cfg.KEYS_FILE} in {root_cfg.CFG_DIR}.")
        click.echo("# Add a key called 'cloud_storage_key'.")
        click.echo("# The value should be the Shared Access Signature for your Azure storage account.")
        click.echo("# You'll find this in portal.azure.com > Storage accounts > Security + networking.")
        click.echo("# ")
        click.echo("# The final line will look like:")
        click.echo("# cloud_storage_key=\"DefaultEndpointsProtocol=https;AccountName=mystorageprod;"
                   "AccountKey=UnZzSivXKjXl0NffCODRGqNDFGCwSBHDG1UcaIeGOdzo2zfFs45GXTB9JjFfD/"
                   "ZDuaLH8m3tf6+ASt2HoD+w==;EndpointSuffix=core.windows.net;\"")
        click.echo("# ")
        click.echo("# Press any key to continue once you have done so")
        click.echo("# ")
        click.echo("\n###############################################")
        return False


####################################################################################################
# Main menu functions
####################################################################################################
@click.group()
def cli() -> None:
    """
    Main command line interface for the sensor core.
    """
    # Disable console logging during CLI execution
    with disable_console_logging("sensor_core"):
        # Check if we need to setup keys or git repo
        check_if_setup_required()


@click.command()
def view_status() -> None:
    """View the current status of the device."""
    try:
        sc = SensorCore()
        click.echo(sc.status())
    except Exception as e:
        click.echo(f"Error in script start up: {e}")


@click.command()
def update_software() -> None:
    """Update the software to the latest version."""
    click.echo("Running update to get latest code...")
    # First check if the /home/bee-ops/code/dua directory exists
    # If it does, we run the update script from there
    # Otherwise we run the update script from the /home/bee-ops/code directory
    if root_cfg.SCRIPTS_DIR.exists():
        run_cmd_live_echo(f"sudo -u $USER {root_cfg.SCRIPTS_DIR}/rpi_installer.sh")
    else:
        click.echo(f"Error: scripts directory does not exist at {root_cfg.SCRIPTS_DIR}. "
                   f"Please check your installation.")
        return


@click.command()
def view_sensor_core_config() -> None:
    """View the sensor core configuration."""
    # Check we have bloc storage access
    if not check_keys_env():
        return
    
    # This function allows the user to set the fully qualified class ref for the sensor core config   
    sc = SensorCore()
    click.echo(f"\n{sc.display_configuration()}")


####################################################################################################
# Debug menu functions
####################################################################################################
@click.command()
def journalctl() -> None:
    """Continuously display journal logs in real time."""
    # Ask if the user wants to specify a grep filter
    click.echo("Do you want to filter the logs? (y/n)")
    char = click.getchar()
    click.echo(char)
    if char == "y":
        click.echo("Enter the grep filter string:")
        filter = input()
    else:
        filter = ""
    click.echo("Press Ctrl+C to exit...\n")
    if root_cfg.running_on_windows:
        click.echo("This command only works on Linux. Exiting...")
        return
    try:
        if filter != "":
            process = subprocess.Popen(
                ["journalctl", "-f", "|", "grep -i", filter],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        else:
            process = subprocess.Popen(["journalctl", "-f"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        while True:
            if process.stdout is not None:
                line = process.stdout.readline().decode("utf-8").strip()
                # Filter out the dull spam
                if "pam_unix" in line:
                    line = ""
                if line != "":
                    click.echo(line)
                    sys.stdout.flush()  # Flush the output to ensure real-time display
                time.sleep(0.1)  # Adjust the refresh interval as needed
    except KeyboardInterrupt:
        click.echo("\nExiting...")


@click.command()
def parse_and_display_log(tag: str = "device_status") -> None:
    """Parse and display logs based on a specific tag."""
    # @@@ Update with python systemd
    pass


@click.command()
def parse_and_display_log_env_measurement(sensor_tag: str, measure_tag: str) -> None:
    """Parse and display environmental measurement logs."""
    # @@@ Update with python systemd
    pass


@click.command()
def check_telemetry_logs() -> None:
    """Check and display telemetry logs."""
    click.echo("Displaying the most recent instance of each sort of telemetry log...\n")
    # @@@ Update with python systemd
    pass


@click.command()
def display_env_telemetry_logs() -> None:
    """Display the most recent environmental telemetry logs."""
    click.echo("Displaying the most recent instance of each sort of env measurement log...\n")
    # @@@ Update with python systemd
    pass


@click.command()
def journalctl_errors() -> None:
    """Display journalctl errors from the last 4 hours."""
    # @@@ Update with python systemd
    pass


####################################################################################################
# Maintenance menu functions
####################################################################################################
@click.command()
def set_hostname() -> None:
    """Set the hostname of the Raspberry Pi."""
    click.echo("Enter the new hostname:")
    new_hostname = input()
    click.echo("Are you sure you want to set the hostname to " + new_hostname + "?")
    click.echo("  y: yes")
    click.echo("  n: no")
    char = click.getchar()
    click.echo(char)
    if char == "y":
        click.echo("Setting hostname... (ignore temporary error message)")
        run_cmd_live_echo("sudo hostnamectl set-hostname " + new_hostname)

        # Delete the line starting with '127.0.1.1' from /etc/hosts
        run_cmd_live_echo("sudo sed -i '/^127.0.1.1/d' /etc/hosts")
        # Add a new line with the new hostname
        run_cmd_live_echo(f"echo '127.0.1.1 {new_hostname}' | sudo tee -a /etc/hosts")
        click.echo("\nHostname set to " + new_hostname + ".\n")
    else:
        click.echo("Exiting...")


####################################################################################################
# Sensor menu functions
####################################################################################################
@click.command()
def display_sensors() -> None:
    """Display the list of configured sensors."""
    click.echo("########################################################\n")
    click.echo("Sensors configured:")
    click.echo("   ")
    click.echo("USB devices discovered:")
    click.echo(run_cmd("lsusb") + "\n")
    click.echo("Associated sounds cards:")
    click.echo(run_cmd("find /sys/devices/ -name id | grep usb | grep sound") + "\n")
    click.echo("Camera:")
    camera_info = run_cmd("libcamera-hello --list-cameras")
    if camera_info == "":
        click.echo("No camera found.\n")
    else:
        click.echo(camera_info + "\n")


@click.command()
def test_audio() -> None:
    """Test the audio sensor using the 'arecord' command."""
    pass


@click.command()
def test_video() -> None:
    """Test the camera function by recording a video."""
    num_configured = root_cfg.my_device.camera_installed
    if num_configured == 0:
        click.echo("No camera configured.")
        return

    click.echo("This test uses the configuration defined for your device_type and with the current settings.")
    click.echo("\nWhat duration of video do you want to capture? (in seconds)")
    try:
        duration = input()
        click.echo(
            "\nRecording video for "
            + duration
            + " seconds... this prompt will not return until capture is complete."
        )

        #@@@vs = video_sensor.VideoSensor(bcli_test_mode=True, bcli_test_duration=int(duration))
        #vs.run()
        click.echo("Video capture completed.")
        click.echo("The video will be saved to /bee-ops/video_capture/ as an H264 file.")
    except Exception as e:
        click.echo(f"Error: {e}")
        click.echo("WARNING: This failure may be because you have not paused recording or set manual mode.")
        click.echo("You also need to wait up to 180s after setting pause recording or manual mode.")


@click.command()
def test_still() -> None:
    """Test the camera function by capturing a still image."""
    num_configured = root_cfg.my_device.camera_installed
    if num_configured == 0:
        click.echo("No camera configured.")
        return

    click.echo("This test uses the configuration defined for your device_type and with the current settings.")

    #@@@vs = video_sensor.VideoSensor(bcli_test_mode=True)
    try:
        #vs.run_single_image()
        click.echo("Image capture completed.")
        click.echo("The image will be saved to /bee-ops/tmp/video/ as a jpg.")
    except Exception as e:
        click.echo(f"Error: {e}")
        click.echo(
            "WARNING: This failure was likely because you have not paused recording or set manual mode."
        )
        click.echo("You also need to wait up to 180s after setting pause recording or manual mode.")


####################################################################################################
# Testing menu functions
####################################################################################################
@click.command()
def run_network_test() -> None:
    """Run a network test and display the results."""
    click.echo("########################################################")
    click.echo("# NETWORK INFO                                         #")
    click.echo("########################################################")
    run_cmd_live_echo(f"sudo {root_cfg.SCRIPTS_DIR}/network_test.sh q")
    click.echo("\n########################################################")


@click.command()
def show_crontab_entries() -> None:
    """Display the crontab entries for the user."""
    click.echo("########################################################")
    click.echo("# CRONTAB ENTRIES                                     #")
    click.echo("########################################################")
    # Get the crontab entries for the user 'bee-ops'
    cron = CronTab(user=utils.get_current_user())
    for job in cron:
        click.echo(job)
    click.echo("\n########################################################")


@click.command()
def self_test() -> None:
    """Run a self-test on the system."""
    # First ask if they want quick or full test
    click.echo("Choose a test:")
    click.echo("  q: quick test")
    click.echo("  f: full test")
    char = click.getchar()
    click.echo(char)
    try:
        if not root_cfg.running_on_rpi:
            click.echo("This command only works on a Raspberry Pi")
            return
        if char == "q":
            run_cmd_live_echo(
                "python -m pytest -s -m quick /home/bee-ops/code/bee_ops_code/common "
                "/home/bee-ops/code/bee_ops_code/rpi_sensor/"
            )
        elif char == "f":
            run_cmd_live_echo(
                "python -m pytest -s /home/bee-ops/code/bee_ops_code/common "
                "/home/bee-ops/code/bee_ops_code/rpi_sensor/"
            )

        # Check that the journal is volatile. Check for the line "Storage=volatile".  Mustn't start with #.
        journald_conf = run_cmd("cat /etc/systemd/journald.conf | grep '#Storage='")
        if journald_conf != "":
            click.echo("ERROR: Journal is not volatile. Check /etc/systemd/journald.conf.")
        journald_conf = run_cmd("cat /etc/systemd/journald.conf | grep 'Storage=volatile'")
        if journald_conf == "":
            click.echo("ERROR: Journal is not volatile. Check /etc/systemd/journald.conf.")

    except Exception as e:
        click.echo(f"ERROR: {e}")


####################################################################################################
# Interactive menu functions
####################################################################################################
@click.command()
def interactive_menu() -> None:
    """Interactive menu for navigating commands."""
    click.clear()

    # Check if we need to setup keys or git repo
    check_if_setup_required()

    # Display status
    click.echo("########################################################")
    click.echo(f"# SensorCore CLI on {root_cfg.my_device_id} {root_cfg.my_device.name}")
    click.echo("########################################################")
    view_status()
    while True:
        click.echo("########################################################")
        click.echo("\nSensorCore Main Menu:")
        click.echo("1. View Config")
        click.echo("2. Sensor Commands")
        click.echo("3. Debugging Commands")
        click.echo("4. Maintenance Commands")
        click.echo("5. Testing Commands")
        click.echo("6. Exit")
        choice = click.prompt("Enter your choice", type=int)

        if choice == 1:
            view_sensor_core_config()
        elif choice == 2:
            sensors_menu()
        elif choice == 3:
            debug_menu()
        elif choice == 4:
            maintenance_menu()
        elif choice == 5:
            testing_menu()
        elif choice == 6:
            click.echo("Exiting...")
            break
        else:
            click.echo("Invalid choice. Please try again.")


def debug_menu() -> None:
    """Menu for debugging commands."""
    while True:
        click.echo("\nDebugging Menu:")
        click.echo("1. Journalctl")
        click.echo("2. Parse and Display Log")
        click.echo("3. Parse and Display Log (Env Measurement)")
        click.echo("4. Check Telemetry Logs")
        click.echo("5. Display Env Telemetry Logs")
        click.echo("6. Journalctl Errors")
        click.echo("7. Back to Main Menu")
        choice = click.prompt("Enter your choice", type=int)

        if choice == 1:
            journalctl()
        elif choice == 2:
            parse_and_display_log()
        elif choice == 3:
            parse_and_display_log_env_measurement()
        elif choice == 4:
            check_telemetry_logs()
        elif choice == 5:
            display_env_telemetry_logs()
        elif choice == 6:
            journalctl_errors()
        elif choice == 7:
            break
        else:
            click.echo("Invalid choice. Please try again.")


def maintenance_menu() -> None:
    """Menu for maintenance commands."""
    while True:
        click.echo("\nMaintenance Menu:")
        click.echo("1. Update Software")
        click.echo("2. Set Hostname")
        click.echo("3. Show Crontab Entries")
        click.echo("4. Back to Main Menu")
        choice = click.prompt("Enter your choice", type=int)

        if choice == 1:
            update_software()
        elif choice == 2:
            set_hostname()
        elif choice == 3:
            show_crontab_entries()
        elif choice == 4:
            break
        else:
            click.echo("Invalid choice. Please try again.")


def sensors_menu() -> None:
    """Menu for sensor commands."""
    while True:
        click.echo("\nSensor Menu:")
        click.echo("1. Display Sensors")
        click.echo("2. Test Audio")
        click.echo("3. Test Video")
        click.echo("4. Test Still")
        click.echo("5. Back to Main Menu")
        choice = click.prompt("Enter your choice", type=int)

        if choice == 1:
            display_sensors()
        elif choice == 2:
            test_audio()
        elif choice == 3:
            test_video()
        elif choice == 4:
            test_still()
        elif choice == 5:
            break
        else:
            click.echo("Invalid choice. Please try again.")


def testing_menu() -> None:
    """Menu for testing commands."""
    while True:
        click.echo("\nTesting Menu:")
        click.echo("1. Run Network Test")
        click.echo("2. Self Test")
        click.echo("3. Back to Main Menu")
        choice = click.prompt("Enter your choice", type=int)

        if choice == 1:
            run_network_test()
        elif choice == 2:
            self_test()
        elif choice == 3:
            break
        else:
            click.echo("Invalid choice. Please try again.")


####################################################################################################
# Define sub-groups for alternative CLI structure
####################################################################################################
@click.group()
def debug() -> None:
    """Debugging commands."""
    pass


@click.group()
def maintenance() -> None:
    """Maintenance commands."""
    pass


@click.group()
def sensors() -> None:
    """Sensor-related commands."""
    pass


@click.group()
def testing() -> None:
    """Testing commands."""
    pass


# Add commands to the debug group
debug.add_command(journalctl)
debug.add_command(parse_and_display_log)
debug.add_command(parse_and_display_log_env_measurement)
debug.add_command(check_telemetry_logs)
debug.add_command(display_env_telemetry_logs)
debug.add_command(journalctl_errors)

# Add commands to the maintenance group
maintenance.add_command(update_software)
maintenance.add_command(set_hostname)
maintenance.add_command(show_crontab_entries)

# Add commands to the sensors group
sensors.add_command(display_sensors)
sensors.add_command(test_audio)
sensors.add_command(test_video)
sensors.add_command(test_still)

# Add commands to the testing group
testing.add_command(run_network_test)
testing.add_command(self_test)

# Add the command groups to the CLI
cli.add_command(interactive_menu)
cli.add_command(debug)
cli.add_command(maintenance)
cli.add_command(sensors)
cli.add_command(testing)


# Main just calls the interactive menu
if __name__ == "__main__":
    # Disable console logging during CLI execution
    with disable_console_logging("sensor_core"):
        interactive_menu()