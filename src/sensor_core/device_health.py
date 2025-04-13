import os
import socket
import subprocess
from datetime import datetime
from typing import Any, Optional

import psutil
from git import Repo  # Requires GitPython library

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.datastream import Datastream
from sensor_core.utils import utils

if root_cfg.running_on_rpi:
    from systemd import journal  # type: ignore
    def get_logs(since: Optional[datetime] = None, 
                 min_priority: Optional[int] = None,
                 grep_str: Optional[str] = None,
                 max_logs: int = 1000) -> list[dict[str, Any]]:
        """
        Fetch logs from the system journal.

        Args:
            since (datetime): A timestamp to fetch logs since.
            min_priority (int): The priority level (e.g., 6 for informational, 4 for warnings).
            grep_str (str): A string to filter logs by message content.
            max_logs (int): Maximum number of logs to fetch.

        Returns:
            list[dict[str, Any]]: A list of log entries.
        """
        logs:list[dict] = []
        try:
            reader = journal.Reader()
        except Exception as e:
            logger.error(f"Failed to initialize journal reader: {e}")
            return logs

        # Set filters
        if since:
            if isinstance(since, datetime):
                reader.seek_realtime(since.timestamp())
            else:
                raise ValueError("The 'since' argument must be a datetime object.")

        # Iterate through the logs
        for i, entry in enumerate(reader):
            priority = int(entry.get("PRIORITY", 0))
            if ((min_priority is None or priority <= min_priority) and
                (grep_str is None or grep_str in entry.get("MESSAGE", ""))):
                if i >= max_logs:
                    break
                time_logged: datetime = entry.get("__REALTIME_TIMESTAMP")
                log_entry = {
                    "time_logged": time_logged,
                    "message": entry.get("MESSAGE", "No message"),
                    "process_id": entry.get("_PID"),
                    "process_name": entry.get("_COMM"),
                    "executable_path": entry.get("_EXE"),
                    "priority": entry.get("PRIORITY"),
                }
                logs.append(log_entry)
        logger.info(f"Fetched {len(logs)} logs from the journal.")

        return logs

logger = utils.setup_logger("sensor_core")

class DeviceHealth():
    """Monitors device health and provides telemetry data as a SensorCore datastream.
    Produces the following data:
    - HEART (DS type ID) provides periodic heartbeats with device health data up to cloud storage.
    - WARNINGS (DS type ID) captures warning and error logs produced by any component, aggregates
      them, and sends them up to cloud storage.
    """

    def __init__(self) -> None:

        ###############################
        # Telemetry tracking
        ###############################
        self.last_ran = api.utc_now()
        self.device_id = root_cfg.my_device_id
        self.disk_writes = 0
        self.disk_writes_timestamp = api.utc_now()
        self.log_counter = 0
        self.client_wlan = "wlan0"
    
    def log_health(self, heart_ds: Datastream) -> None:
        """Logs device health data to the HEART datastream."""
        health = DeviceHealth.get_health()
        heart_ds.log(health)

    def log_warnings(self, warning_ds: Datastream) -> None:
        """Capture warning and error logs to the WARNING datastream.
        We get these from the system journal and log them to the WARNING datastream.
        We capture logs tagged with the RAISE_WARN_TAG and all logs with priority <=4 (Warning)."""

        if root_cfg.running_on_rpi:
            since_time = self.last_ran
            self.last_ran = api.utc_now()
            logs = get_logs(since=since_time, min_priority=6)

            for log in logs:
                if api.RAISE_WARN_TAG in log:
                    warning_ds.log(log)
                elif log["priority"] <= 4:
                    warning_ds.log(log)
            

    ############################################################################################################
    # Diagnostics utility functions
    ############################################################################################################
    @staticmethod
    def get_health() -> dict[str, Any]:
        """Get the health of the device."""
        health: dict[str, Any] = {}
        try:
            disk_writes = 0
            cpu_temp = 0
            sc_mount_size = ""
            get_throttled_output = ""
            process_list_str = ""
            ssid = ""
            if root_cfg.running_on_rpi:
                # Get the connected SSID
                ssid = DeviceHealth.get_wifi_ssid()

                # We need to call the "vcgencmd get_throttled" command to get the current throttled state
                # Output is "throttled=0x0"
                get_throttled_output = utils.run_cmd(
                    "sudo vcgencmd get_throttled", ignore_errors=True, grep_strs=["throttled"]
                )
                get_throttled_output = get_throttled_output.replace("throttled=", "")

                # Get CPU data
                cpu_temp = psutil.sensors_temperatures()["cpu_thermal"][0].current  # type: ignore

                # Get the number of disk writes
                if os.path.exists("/sys/block/mmcblk0/stat"):
                    disk_str = "/sys/block/mmcblk0/stat"
                else:
                    disk_str = "/sys/block/sda/stat"

                disk_stats = utils.run_cmd(
                    "sudo cat " + disk_str,
                    ignore_errors=True,
                )
                if disk_stats:
                    disk_writes = int(disk_stats.split()[5])
                else:
                    logger.info("Failed to get disk writes: no mmcblk0 or sda")

                # Get the size of the /sensor_core mount
                # Parse the output to get the size of the mount (equivalent to "awk 'NR==2{print $2}'")
                sc_mount_size = utils.run_cmd("sudo df -h /sensor_core", ignore_errors=True)
                if sc_mount_size != "":
                    sc_mount_size = sc_mount_size.split("\n")[1].split()[1]

                # Running processes
                # for each process in the list, strip any text before "sensor_core" or "dua"
                # Drop any starting / or . characters
                # And convert the process list to a simple comma-seperated string with no {} or ' or " 
                # characters
                process_set = utils.check_running_processes(search_string="sensor_core")
                process_list = [x.split("sensor_core")[-1] for x in process_set]
                process_list = [x.split("dua")[-1] for x in process_list]
                process_list = [x.lstrip("/").lstrip(".") for x in process_list]
                process_list_str = (
                    str(process_list).replace("{", 
                                              "").replace("}", "").replace("'", "").replace('"', "").strip()
                )
                process_list_str = process_list_str.replace("mode", "")


            # Check update status by getting the last modified time of the rpi_installer_ran file
            # This file is created when the rpi_installer.sh script is run
            # and is used to track the last time the system was updated
            last_update_timestamp: float = 0
            rpi_installer_file = root_cfg.CFG_DIR / "rpi_installer_ran"
            if os.path.exists(rpi_installer_file):
                last_update_timestamp = os.path.getmtime(rpi_installer_file)
            else:
                last_update_timestamp = 0

            # Get the IP address of the device
            def get_ip_address(target_interface: str) -> str:
                for interface, snics in psutil.net_if_addrs().items():
                    if interface == target_interface:
                        for snic in snics:
                            if snic.family == socket.AF_INET:
                                return snic.address
                return "No IP address"

            if root_cfg.running_on_rpi:
                target_interface = "wlan0"
            else:
                target_interface = "WiFi"
            ip_addresses = get_ip_address(target_interface)

            # Grab the commit hash of the current code
            try:
                repo = Repo(root_cfg.SC_CODE_DIR)
                git_commit_hash = repo.head.commit.hexsha[:7]
            except Exception:
                git_commit_hash = "unknown"

            # Total memory
            total_memory = psutil.virtual_memory().total
            total_memory_gb = round(total_memory / (1024**3), 2)

            # Memory usage - if greater than 60% then generate some diagnostics
            memory_usage = psutil.virtual_memory().percent
            if memory_usage > 50:
                if root_cfg.running_on_rpi:
                    DeviceHealth.log_top_memory_processes()
                    if memory_usage > 95:
                        logger.error(utils.RAISE_WARN() + "Memory usage >95%, rebooting")
                        utils.run_cmd("sudo reboot", ignore_errors=True)

            health = {
                "timestamp": api.utc_to_iso_str(),
                "boot_time": api.utc_to_iso_str(psutil.boot_time()),
                "last_update_timestamp": str(last_update_timestamp),
                "cpu_percent": str(psutil.cpu_percent(2)),
                "cpu_idle": str(int(psutil.cpu_times().idle)),
                "cpu_user": str(int(psutil.cpu_times().user)),
                "total_memory_gb": str(total_memory_gb),
                "memory_percent": str(memory_usage),
                "memory_free": str(int(psutil.virtual_memory().free / 1000000)) + "M",
                "disk_percent": str(psutil.disk_usage("/").percent),
                "disk_writes_in_period": str(disk_writes),
                "sc_mount_size": str(sc_mount_size),
                "sc_ram_percent": str(
                    psutil.disk_usage(str(root_cfg.ROOT_WORKING_DIR)).percent
                ),  # Need to parse the output of sensors_temperatures() to get the current CPU temperature
                # Output is {'cpu_thermal': [shwtemp(label='', current=46.251, high=110.0, critical=110.0)]}
                "cpu_temperature": str(cpu_temp),
                "ssid": ssid,
                "ip_address": str(ip_addresses),
                "power_status": str(get_throttled_output),
                "process_list": process_list_str,
                "git_commit_hash": str(git_commit_hash),  # "git_branch_cfgd" : git_branch_cfgd, \
            }

        except Exception as e:
            logger.error(utils.RAISE_WARN() + "Failed to get telemetry: " + str(e), exc_info=True)

        return health

    # Function to get diagnostics on the top 3 memory-using processes
    @staticmethod
    def log_top_memory_processes(num_processes: int=5) -> None:
        # Create a list of all processes with their memory usage
        # It's possible for processes to disappear between the time we get the list and the time we log it
        # so we need to be careful about this
        processes = []
        for proc in psutil.process_iter(attrs=["pid", "name", "memory_info", "cmdline"]):
            # The memory_info is in a pmem object, so we need to extract the rss value
            rss = proc.info["memory_info"].rss
            processes.append((rss, proc.info))
            
        # Sort the list of processes by memory usage (rss) in descending order
        all_processes = sorted(processes, key=lambda x: x[0], reverse=True)
        top_processes = all_processes[:num_processes]

        # Format the information for the top processes
        log_string = f"Memory at {psutil.virtual_memory().percent}%; top processes: "
        for rss, info in top_processes:
            # Combine the command line arguments into a single string, but drop any words starting with "-"
            if root_cfg.running_on_rpi:
                cmd_line = " ".join([arg for arg in info["cmdline"] if not arg.startswith("-")])
            else:
                cmd_line = info["name"]
            log_string += f"[{cmd_line}]({info['pid']})={info['memory_info'].rss / (1024**2):.2f}MB, "
        logger.warning(log_string)

    @staticmethod
    def get_wifi_ssid() -> str:
        if root_cfg.running_on_rpi:
            try:
                return subprocess.check_output(["iwgetid", "-r"], universal_newlines=True).strip()
            except subprocess.CalledProcessError:
                return "Not connected"
        elif root_cfg.running_on_windows:
            try:
                output = subprocess.check_output(["netsh", "wlan", "show", "interfaces"], 
                                                 universal_newlines=True)
                for line in output.split("\n"):
                    if "SSID" in line and "BSSID" not in line:
                        return line.split(":")[1].strip()
                return "Not connected"
            except subprocess.CalledProcessError:
                return "Not connected"
        else:
            return "Unsupported platform"