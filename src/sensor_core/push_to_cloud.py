####################################################################################################
# Push the sensor data to the cloud
####################################################################################################
import time
import zipfile
from pathlib import Path

import psutil

from sensor_core import api, cloud_connector
from sensor_core import configuration as root_cfg
from sensor_core.utils import file_naming, utils

logger = utils.setup_logger("sensor_core")


def push_to_cloud() -> None:
    """Creates a ZIP file with the contents of the TMP_DIR and pushes it to the cloud."""

    # Get a properly formatted zip file name
    zip_fname = file_naming.get_zip_filename()

    # Use journalctl to get the last 35 mins of logs.  We should be called every 30 mins but we want
    # to make sure we have overlap in case we're called a bit late - and the process will deduplicate later.
    # We use the --since option to get the logs since 35 mins ago
    # We use the --utc option to get the logs in UTC
    # We use the --priority option to get the logs at priority 6 (info) and above
    try:
        log_file = file_naming.get_log_filename()
        utils.run_cmd(
            f"journalctl --utc --since -35min --priority 6 -o short-iso-precise --no-hostname > {log_file}",
            ignore_errors=True,
        )
    except Exception as e:
        logger.error(f"{utils.RAISE_WARN()}Error creating or zipping log file {log_file}: {e}")

    # Zip the full contents of /tmp and remove the original files
    # We only zip files that are more than 1 minute old to avoid zipping files that are still being written to
    # Except the log & ls_dump.txt file which we want to upload immediately
    # In shell this is: sudo find /sensor_core/tmp/* -mmin +1 -type f -exec zip -m $upload_file_name {} \;
    # We use the -m option to move the files into the zip file
    # Implement natively in python using the zipfile module
    current_time = time.time()
    try:
        with zipfile.ZipFile(zip_fname, "w") as zip_file:
            for file in Path(root_cfg.TMP_DIR).rglob("*"):
                if file.is_file():
                    # Check the file age using the stat() method
                    file_mod_time = file.stat().st_mtime
                    file_age = current_time - file_mod_time
                    if file_age > 60:
                        zip_file.write(file, arcname=file.relative_to(root_cfg.TMP_DIR))
                        file.unlink()  # Remove the original file
            if log_file.exists():
                zip_file.write(log_file, log_file)
                log_file.unlink()  # Remove the original log file
    except Exception as e:
        logger.error(f"{utils.RAISE_WARN()}Error zipping file {file}: {e}")

    # Upload all / any zip files to the cloud
    try:
        zip_files = list(Path(root_cfg.EDGE_UPLOAD_DIR).rglob("*.zip"))
        cc = cloud_connector.CloudConnector()
        cc.upload_to_container(root_cfg.my_device.cc_for_upload, zip_files, delete_src=True)
        logger.info(f"Uploaded zip files {zip_files} to cloud")
    except Exception as e:
        logger.error(f"{utils.RAISE_WARN()}Error uploading zip file {zip_fname} to cloud: {e}")

        # Check whether we're running short on disk space and if we are, delete the zip file
        # We delete all the contents of upload because we haven't implemented any
        # other mechanic for cleaning up the files if this fails.
        # Get the % of space used in the /sensor_core mount and check whether it's >75%
        mount = "/sensor_core"
        disk_usage = psutil.disk_usage(mount)
        if disk_usage.percent > 75:
            logger.warning(
                f"Disk space on {mount} is {disk_usage}%, deleting all files in /sensor_core/upload"
            )
            for file in Path(root_cfg.EDGE_UPLOAD_DIR).rglob("*"):
                if file.is_file():
                    file.unlink()

    # Record successful upload in the persistent KV store
    try:
        # Timestamp needs to be equivalent to shell "+%s"
        utils.record_persistent_key_value("last_upload_timestamp", str(api.utc_now().timestamp()))
    except Exception as e:
        logger.error(f"{utils.RAISE_WARN()}Error recording upload in persistent KV store: {e}")


if __name__ == "__main__":
    push_to_cloud()
