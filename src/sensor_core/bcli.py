####################################################################################################
# Description: This script is used to run the bcli command.
####################################################################################################
import queue
import subprocess
import sys
import threading
import time
from datetime import timedelta

import click
from crontab import CronTab

from sensor_core import SensorCore, api, device_health
from sensor_core import configuration as root_cfg
from sensor_core.utils import utils
from sensor_core.utils.utils import disable_console_logging

logger = utils.setup_logger("common")

dash_line = "########################################################"
header = dash_line + "\n\n"

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
    finally:
        if process and process.poll() is None:
            process.terminate()  # Ensure the process is terminated

    return "Command executed successfully."


def check_if_setup_required() -> None:
    """Check if setup is required by verifying keys and Git repo."""
    attempts = 0
    max_attempts = 3
    while not check_keys_env():
        attempts += 1
        if attempts >= max_attempts:
            click.echo("Setup not completed. Exiting...")
            sys.exit(1)
        click.echo("Press any key to retry setup...")
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
        click.echo(f"{dash_line}")
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
        click.echo(f"{dash_line}")
        return False


####################################################################################################
# Main menu functions
####################################################################################################
def view_status() -> None:
    """View the current status of the device."""
    try:
        sc = SensorCore()
        click.echo(sc.status())
    except Exception as e:
        click.echo(f"Error in script start up: {e}")


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

def display_logs(logs: list[dict]) -> None:
    for log in logs:
        # Nicely format the log by printing the timestamp and message
        log["timestamp"] = api.utc_to_iso_str(log["time_logged"])
        click.echo(f"{log['timestamp']} - {log['priority']} - {log['message']}")

def display_errors() -> None:
    """Display error logs."""
    if root_cfg.running_on_windows:
        click.echo("This command only works on Linux. Exiting...")
        return
    since_time = api.utc_now() - timedelta(hours=4)
    click.echo("\n")
    click.echo(f"{dash_line}")
    click.echo("# ERROR LOGS")
    click.echo("# Displaying error logs from the last 4 hours")
    click.echo(f"{dash_line}")
    logs = device_health.get_logs(since=since_time, min_priority=4)
    display_logs(logs)


def display_sensor_core_logs() -> None:
    """Display regular sensor_core logs."""
    if root_cfg.running_on_windows:
        click.echo("This command only works on Linux. Exiting...")
        return
    since_time = api.utc_now() - timedelta(minutes=15)
    click.echo(f"{dash_line}")
    click.echo("# SensorCore logs")
    click.echo("# Displaying sensor_core logs for the last 15 minutes")
    click.echo(f"{dash_line}")
    logs = device_health.get_logs(since=since_time, min_priority=6, grep_str="sensor_core")
    display_logs(logs)


def display_sensor_logs() -> None:
    if root_cfg.running_on_windows:
        click.echo("This command only works on Linux. Exiting...")
        return
    since_time = api.utc_now() - timedelta(minutes=30)
    click.echo(f"{dash_line}")
    click.echo("# SensorCore logs")
    click.echo("# Displaying sensor_core logs for the last 30 minutes")
    click.echo(f"{dash_line}")
    logs = device_health.get_logs(since=since_time, min_priority=6, grep_str=api.TELEM_TAG)
    display_logs(logs)


####################################################################################################
# Maintenance menu functions
####################################################################################################
def update_software() -> None:
    """Update the software to the latest version."""
    click.echo("Running update to get latest code...")
    if root_cfg.running_on_windows:
        click.echo("This command only works on Linux. Exiting...")
        return
    # First check if the /home/bee-ops/code/dua directory exists
    # If it does, we run the update script from there
    # Otherwise we run the update script from the /home/bee-ops/code directory
    if root_cfg.SCRIPTS_DIR.exists():
        run_cmd_live_echo(f"sudo -u $USER {root_cfg.SCRIPTS_DIR}/rpi_installer.sh")
    else:
        click.echo(f"Error: scripts directory does not exist at {root_cfg.SCRIPTS_DIR}. "
                   f"Please check your installation.")
        return


def start_sensor_core() -> None:
    """Start the SensorCore service."""
    click.echo("Starting SensorCore...")

    # If run_my_sensor.py is a resolvable module in this environment, then we use that to start the service
    # using that user-provided script.
    try:
        import run_my_sensor  # noqa: F401
    except ImportError:
        click.echo("No configuration script found (run_my_sensor.py)")
        click.echo("Do you want to start SensorCore using the default configuration? (y/n)")
        char = click.getchar()
        click.echo(char)
        if char != "y":
            click.echo("Exiting...")
            return
        click.echo("Starting SensorCore using default configuration...")
        sc = SensorCore()
        sc.start()
    else:
        click.echo("Found run_my_sensor.py. Starting SensorCore with that script...")
        if root_cfg.running_on_windows:
            click.echo("This command only works on Linux. Exiting...")
            return
        if root_cfg.SCRIPTS_DIR.exists():
            run_cmd(f"nohup sudo -u $USER {root_cfg.SCRIPTS_DIR}/run_sensor_core.sh &")
        else:
            click.echo(f"Error: scripts directory does not exist at {root_cfg.SCRIPTS_DIR}. "
                       f"Please check your installation.")

    click.echo("SensorCore started.")
    return


def stop_sensor_core() -> None:
    """Stop the SensorCore service."""
    click.echo("Stopping SensorCore... this may take up to 180s to complete.")
    # We just need to "touch" the stop file to stop the service
    root_cfg.STOP_SENSOR_CORE_FLAG.touch()
    return


def set_hostname() -> None:
    """Set the hostname of the Raspberry Pi."""
    click.echo("Enter the new hostname:")
    new_hostname = input()
    click.echo("Are you sure you want to set the hostname to " + new_hostname + "?")
    click.echo("  y: yes")
    click.echo("  n: no")
    char = click.getchar()
    click.echo(char)
    if not root_cfg.running_on_rpi:
        click.echo("This command only works on a Raspberry Pi")
        return
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


def show_crontab_entries() -> None:
    """Display the crontab entries for the user."""
    click.echo(f"{dash_line}")
    click.echo("# CRONTAB ENTRIES")
    click.echo(f"{dash_line}")
    if not root_cfg.running_on_rpi:
        click.echo("This command only works on a Raspberry Pi")
        return
    # Get the crontab entries for the user 'bee-ops'
    cron = CronTab(user=utils.get_current_user())
    for job in cron:
        click.echo(job)
    click.echo(f"{dash_line}")


####################################################################################################
# Sensor menu functions
####################################################################################################
def display_sensors() -> None:
    """Display the list of configured sensors."""
    click.echo(f"{dash_line}")
    click.echo("\nSensors & their primary datastreams configured:\n")
    for i, sensor_ds in enumerate(root_cfg.my_device.sensor_ds_list):
        click.echo(f"{i}> {sensor_ds.sensor_cfg.sensor_type}: {sensor_ds.sensor_cfg.sensor_index} "
                   f"- {sensor_ds.sensor_cfg.sensor_class_ref}")
        for ds in sensor_ds.datastream_cfgs:
            click.echo(f"  {ds.ds_type_id}: - {ds.archived_data_description}")
    click.echo("\nUSB devices discovered:")
    click.echo(run_cmd("lsusb") + "\n")
    click.echo("Associated sounds cards:")
    click.echo(run_cmd("find /sys/devices/ -name id | grep usb | grep sound"))
    click.echo("\nCamera:")
    camera_info = run_cmd("libcamera-hello --list-cameras")
    if camera_info == "":
        click.echo("No camera found.\n")
    else:
        click.echo(camera_info + "\n")


def test_audio() -> None:
    """Test the audio sensor using the 'arecord' command."""
    pass


def test_video() -> None:
    """Test the camera function by recording a video."""
    # Parse the output of libcamera-hello --list-cameras to get the camera names
    # and to see how many cameras are configured
    camera_info = run_cmd("libcamera-hello --list-cameras")
    if camera_info == "":
        click.echo("No camera found.\n")
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


def test_still() -> None:
    """Test the camera function by capturing a still image."""
    camera_info = run_cmd("libcamera-hello --list-cameras")
    if camera_info == "":
        click.echo("No camera found.\n")
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
def run_network_test() -> None:
    """Run a network test and display the results."""
    click.echo(f"{dash_line}")
    click.echo("# NETWORK INFO")
    click.echo(f"{dash_line}")
    if not root_cfg.running_on_rpi:
        click.echo("This command only works on a Raspberry Pi")
        return
    run_cmd_live_echo(f"sudo {root_cfg.SCRIPTS_DIR}/network_test.sh q")
    click.echo(f"{dash_line}")


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
    except Exception as e:
        click.echo(f"ERROR: {e}")


####################################################################################################
# Interactive menu functions
####################################################################################################
def interactive_menu() -> None:
    """Interactive menu for navigating commands."""
    click.clear()

    # Check if we need to setup keys or git repo
    check_if_setup_required()

    # Display status
    click.echo(f"{dash_line}")
    click.echo(f"# SensorCore CLI on {root_cfg.my_device_id} {root_cfg.my_device.name}")
    click.echo(f"{dash_line}")
    view_status()
    while True:
        click.echo(f"{header}Main Menu:")
        click.echo("1. View Config")
        click.echo("2. Sensor Commands")
        click.echo("3. Debugging Commands")
        click.echo("4. Maintenance Commands")
        click.echo("5. Testing Commands")
        click.echo("6. Exit")
        try:
            choice = click.prompt("\nEnter your choice", type=int)
            click.echo("\n")
        except ValueError:
            click.echo("Invalid input. Please enter a number.")
            continue

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
        click.echo(f"{header}Debugging Menu:")
        click.echo("1. Journalctl")
        click.echo("2. Display errors")
        click.echo("3. Display SensorCore Logs")
        click.echo("4. Display logs from sensors")
        click.echo("5. Back to Main Menu")
        try:
            choice = click.prompt("\nEnter your choice", type=int)
            click.echo("\n")
        except ValueError:
            click.echo("Invalid input. Please enter a number.")
            continue

        if choice == 1:
            journalctl()
        elif choice == 2:
            display_errors()
        elif choice == 3:
            display_sensor_core_logs()
        elif choice == 4:
            display_sensor_logs()
        elif choice == 5:
            break
        else:
            click.echo("Invalid choice. Please try again.")


def maintenance_menu() -> None:
    """Menu for maintenance commands."""
    while True:
        click.echo(f"{header}Maintenance Menu:")
        click.echo("1. Update Software")
        click.echo("2. Start SensorCore")
        click.echo("3. Stop SensorCore")
        click.echo("4. Set Hostname")
        click.echo("5. Show Crontab Entries")
        click.echo("6. Back to Main Menu")
        try:
            choice = click.prompt("\nEnter your choice", type=int)
            click.echo("\n")
        except ValueError:
            click.echo("Invalid input. Please enter a number.")
            continue

        if choice == 1:
            update_software()
        elif choice == 2:
            start_sensor_core()
        elif choice == 3:
            stop_sensor_core()
        elif choice == 4:
            set_hostname()
        elif choice == 5:
            show_crontab_entries()
        elif choice == 6:
            break
        else:
            click.echo("Invalid choice. Please try again.")


def sensors_menu() -> None:
    """Menu for sensor commands."""
    while True:
        click.echo(f"{header}Sensor Menu:")
        click.echo("1. Display Sensors")
        click.echo("2. Test Audio")
        click.echo("3. Test Video")
        click.echo("4. Test Still")
        click.echo("5. Back to Main Menu")
        try:
            choice = click.prompt("\nEnter your choice", type=int)
            click.echo("\n")
        except ValueError:
            click.echo("Invalid input. Please enter a number.")
            continue

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
        click.echo(f"{header}Testing Menu:")
        click.echo("1. Run Network Test")
        click.echo("2. Self Test")
        click.echo("3. Back to Main Menu")
        try:
            choice = click.prompt("\nEnter your choice", type=int)
            click.echo("\n")
        except ValueError:
            click.echo("Invalid input. Please enter a number.")
            continue

        if choice == 1:
            run_network_test()
        elif choice == 2:
            self_test()
        elif choice == 3:
            break
        else:
            click.echo("Invalid choice. Please try again.")

#################################################################################
# Main function to run the CLI
# Main just calls the interactive menu
#################################################################################
def main():
    # Disable console logging during CLI execution
    with disable_console_logging("sensor_core"):
        interactive_menu()

if __name__ == "__main__":
    main()