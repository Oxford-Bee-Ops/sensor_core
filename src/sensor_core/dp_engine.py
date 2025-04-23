from __future__ import annotations

from datetime import datetime
from pathlib import Path
from threading import Thread
from time import sleep
from typing import Optional

import pandas as pd
import yaml

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.cloud_connector import CloudConnector
from sensor_core.dp_tree_node_types import DataProcessorCfg, SensorCfg
from sensor_core.configuration import Mode
from sensor_core.data_processor import DataProcessor
from sensor_core.device_health import DEVICE_HEALTH_CFG
from sensor_core.dp_tree import DPtree
from sensor_core.dp_tree_node import DPtreeNode
from sensor_core.utils import file_naming

logger = root_cfg.setup_logger("sensor_core")

# Run frequency of the Datastream worker thread
# Normally 60 seconds, but overridden in tests
RUN_FREQUENCY_SECS = 60


class DPengine(Thread):
    """A DPengine is the thread that processes data through a DPtree.
    Note: the Sensor has a separate thread.
    """

    def __init__(
        self,
        dp_tree: DPtree,
    ) -> None:
        """Initialise the DPengine."""
        super().__init__()
        logger.debug(f"Initialising DPengine {self}")

        self._stop_requested = False

        # sensor_cfg is a SensorCfg object describing the sensor that produces this datastream.
        self.dp_tree: DPtree = dp_tree

        # device_id is the machine ID (mac address) of the device that produces this datastream.
        self.device_id = root_cfg.my_device_id

        # sensor_id is an index (eg port number) identifying the sensor that produces this datastream.
        # This is not unique on the device, but must be unique in combination with the datastream_type_id.
        self.sensor_index = dp_tree.sensor.config.sensor_index

        # start_time is a datetime object that describes the time the DPengine was started.
        # This should be set by calling the start() method, and not set during initialization.
        self.dpe_start_time: Optional[datetime] = None


    #########################################################################################################
    #
    # Public methods called by the Sensor or DataProcessor to log or save data
    #
    #########################################################################################################

    def save_FAIR_record(self) -> None:
        """Save a FAIR record describing this Sensor and associated data processing to the FAIR archive.
        """
        type_id = self.dp_tree.sensor.config.type_id
        # We don't save FAIR records for system datastreams
        if type_id == DEVICE_HEALTH_CFG.type_id:
            return

        # Wrap the "record" data in a FAIR record
        wrap: dict[str, dict | str | list] = {}
        wrap[api.RECORD_ID.VERSION.value] = "V3"
        wrap[api.RECORD_ID.DATA_TYPE_ID.value] = type_id
        wrap[api.RECORD_ID.DEVICE_ID.value] = self.device_id
        wrap[api.RECORD_ID.SENSOR_INDEX.value] = str(self.sensor_index)
        wrap[api.RECORD_ID.TIMESTAMP.value] = api.utc_to_iso_str()
        wrap["record"] = self.dp_tree.export()

        # We always include the list of mac addresses for all devices in this experiment (fleet_config)
        # This enables the dashboard to check that all devices are present and working.
        wrap["fleet"] = list(root_cfg.INVENTORY.keys())

        # Save the FAIR record as a YAML file to the FAIR archive
        tmp_file = file_naming.get_temporary_filename(suffix="yaml")
        Path(tmp_file).parent.mkdir(parents=True, exist_ok=True)
        with open(tmp_file, "w") as f:
            yaml.dump(wrap, f)
        CloudConnector.get_instance().upload_to_container(root_cfg.my_device.cc_for_fair,
                                                          [tmp_file], delete_src=True)

    def log_sample_data(self, sample_period_start_time: datetime, dpnode: DPtreeNode) -> None:
        """Provide the count & duration of data samples recorded (environmental, media, etc)
        since the last time log_sample_data was called.

        This is used by EdgeOrchestrator to periodically log observability data
        """
        # We need to traverse all nodes in the tree and call log_sample_data on each node
        for node in self.dp_tree._nodes.values():
            node.log_sample_data(sample_period_start_time, dpnode)

    def get_sensor_cfg(self) -> Optional[SensorCfg]:
        """Return the SensorCfg object for this Datastream"""
        return self.dp_tree.sensor.config


    #########################################################################################################
    #
    # DPengine worker thread methods
    #
    #########################################################################################################

    def start(self) -> None:
        """Start the Datastream worker thread.

        In EDGE mode this is called by the Sensor class when the Sensor is started.
        In ETL mode this is called by DatastreamFactory when the ETL process is scheduled.
        """
        if self.dpe_start_time is None:
            self.dpe_start_time = api.utc_now()
            # Call our superclass Thread start() method which schedule our run() method
            super().start()
        else:
            logger.warning(f"{root_cfg.RAISE_WARN()}Datastream {self} already started.")

    def stop(self) -> None:
        """Stop the Datastream worker thread"""

        self._stop_requested = True

    def run(self) -> None:
        """Main Datastream thread that persistently processes files, logs or data generated by Sensors"""

        try:
            logger.info(f"Invoking run() on {self!r} in {root_cfg.get_mode()} mode")
            if root_cfg.get_mode() == Mode.EDGE:
                self.edge_run()
            else:
                self.etl_run()
        except Exception as e:
            logger.error(f"{root_cfg.RAISE_WARN()}Error running {self!r}: {e!s}", 
                         exc_info=True)
        # @@@ Should we add recovery code? eg call stop_all?

    def edge_run(self) -> None:
        """Main Datastream loop processing files, logs or data generated by Sensors"""

        # Create the FAIR record for this sensor and associated processing
        self.save_FAIR_record()

        # If there are no data processors, we can exit the thread because data will be saved 
        # directly to the cloud
        if len(self.dp_tree.get_processors() == 0):
            logger.debug(f"No DataProcessors registered; exiting DPengine loop; {self!r}")
            return

        while not self._stop_requested:
            start_time = api.utc_now()
            input_df: Optional[pd.DataFrame] = None
            dp: DataProcessor
            dp_config: DataProcessorCfg

            for dp in enumerate(self.dp_tree.get_processors()):
                try:
                    exec_start_time = api.utc_now()
                    output_df: Optional[pd.DataFrame] = None
                    dp_config = dp.get_config()

                    #########################################################################################
                    # Invoke the DataProcessor
                    #
                    # Standard chaining involves passing a Dataframe along the DP chain.
                    # The first DP may be invoked with recording files (jpg, h264, wav, etc) or a CSV
                    # as defined in the dp_config
                    #########################################################################################
                    if dp_config.input_stream.format == "df":
                        assert input_df is not None, "input_df is null on df run"
                        logger.debug(f"Invoking {dp} with {input_df}")
                        output_df = dp.process_data(self, input_df, self._get_context(dp_config))
                    elif dp_config.input_stream.format == "csv":
                        assert input_df is None, "input_df not null on csv run"
                        # Find and load CSVs as DFs
                        input_df = self._get_csv_as_df()
                        if input_df is not None:
                            logger.debug(f"Invoking {dp} with {input_df}")
                            output_df = dp.process_data(self, input_df, self._get_context(dp_config))
                    else:
                        assert input_df is None, "input_df not null on file run"
                        # DPs may process recording files
                        input_files = self._get_ds_files(dp)
                        if input_files is not None and len(input_files) > 0:
                            logger.debug(f"Invoking {dp} with {input_files}")
                            output_df = dp.process_data(self,
                                                            input_data=input_files, 
                                                            context=self._get_context(dp_config))
                            # Clear up the files now they've been processed.
                            # Any files that were meant to be uploaded will have been moved directly
                            # to the upload directory.
                            # Sampling is done on initial save_recording.
                            for f in input_files:
                                if f.exists():
                                    try:
                                        f.unlink()
                                    except Exception as e:
                                        logger.error(f"{root_cfg.RAISE_WARN()}Failed to unlink {f} {e!s}", 
                                                     exc_info=True)

                    # Validate the output_df before passing it to the next DP in the chain
                    if output_df is not None:
                        input_df = self._validate_output(output_df, dp)

                    # Log the processing time
                    exec_time = api.utc_now() - exec_start_time
                    self._scorp_dp.log(
                        {
                            "mode": Mode.EDGE.value,
                            "observed_type_id": dp_config.type_id,
                            "observed_sensor_index": self.sensor_index,
                            "duration": exec_time.total_seconds(),
                        }
                    )
                except Exception as e:
                    logger.error(
                        f"{root_cfg.RAISE_WARN()}Error processing files for {self}. e={e!s}",
                        exc_info=True,
                    )

            # We've exited the DataProcessor chain.  Save any resulting data.
            if input_df is not None and len(input_df) > 0:
                logger.debug(f"Saving data from {dp_config.type_id} to journal")
                self.journal_pool.add_rows_from_df(self.ds_config, input_df, api.utc_now())

            # We want to run this loop every minute, so see how long it took us since the start_time
            sleep_time = RUN_FREQUENCY_SECS - (api.utc_now() - start_time).total_seconds()
            logger.debug(f"DataProcessor loop sleeping for {sleep_time} seconds")
            if sleep_time > 0:
                sleep(sleep_time)

    #########################################################################################################
    #
    # Private methods used in support of DataProcessors
    #
    #########################################################################################################
    def _validate_output(
        self, output_data: Optional[pd.DataFrame], dp: DataProcessor
    ) -> Optional[pd.DataFrame]:
        if output_data is None or output_data.empty:
            logger.debug(f"No output from {dp}")
            return None

        # Output DFs must always contain the core RECORD_ID fields
        # If not already present, add the RECORD_ID fields to the output_df
        for field in api.REQD_RECORD_ID_FIELDS:
            if field not in output_data.columns:
                if field == api.RECORD_ID.VERSION.value:
                    output_data[field] = "V3"
                elif field == api.RECORD_ID.TIMESTAMP.value:
                    output_data[field] = api.utc_to_iso_str()
                elif field == api.RECORD_ID.DEVICE_ID.value:
                    output_data[field] = self.device_id
                elif field == api.RECORD_ID.SENSOR_INDEX.value:
                    output_data[field] = self.sensor_index
                elif field == api.RECORD_ID.DATA_TYPE_ID.value:
                    output_data[field] = self.ds_config.type_id

        # Check the values in the RECORD_ID are not nan or empty
        for field in api.REQD_RECORD_ID_FIELDS:
            if not output_data[field].notna().all():
                err_str = f"{root_cfg.RAISE_WARN()}{field} contains NaN or empty values in output_df {dp}"
                logger.error(err_str)
                raise Exception(err_str)

        # Warn about superfluous fields that will get dropped
        for field in output_data.columns:
            if (
                (dp.config.output_fields is not None)
                and (field not in dp.config.output_fields)
                and (field not in api.ALL_RECORD_ID_FIELDS)
            ):
                logger.warning(
                    f"{field} in output from {dp} but not in defined fields: {dp.config.output_fields}"
                )

        # Output DF should contain the fields defined by the DP's output_fields list.
        assert dp.config.output_fields is not None and len(dp.config.output_fields) > 0
        for field in dp.config.output_fields:
            if field not in output_data.columns:
                err_str = (f"{root_cfg.RAISE_WARN()}{field} missing from output_df on "
                           f"{dp}: {output_data.columns}")
                logger.error(err_str)
                raise Exception(err_str)

        return output_data

    def _get_ds_files(self, dp: DataProcessor) -> Optional[list[Path]]:
        """Find any files that match the requested Datastream (type, device_id & sensor_index)"""
        if root_cfg.get_mode() == Mode.EDGE:
            src = root_cfg.EDGE_PROCESSING_DIR
        else:
            src = root_cfg.ETL_PROCESSING_DIR
        files = list(src.glob(f"*{self.ds_id}*.{dp.config.input_format}"))

        # We must return only files that are not currently being written to
        # Do not return files modified in the last few seconds
        now = api.utc_now().timestamp()
        files = [f for f in files if (now - f.stat().st_mtime) > 5]

        logger.debug(f"_get_ds_files returning {files}")
        return files

    def _get_csv_as_df(self) -> Optional[pd.DataFrame]:
        """Get the first CSV file that matches this Datastream's DatastreamType as a DataFrame"""
        if root_cfg.get_mode() == Mode.EDGE:
            src = root_cfg.EDGE_PROCESSING_DIR
        else:
            src = root_cfg.ETL_PROCESSING_DIR

        csv_files = src.glob(f"*{self.ds_config.type_id}*.csv")

        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                return df
            except Exception as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Error reading CSV file {csv_file}: {e}", exc_info=True)
        return None

    @staticmethod
    def _set_scorp_dp(scorp_dp: DPtreeNode) -> None:
        """Called by EdgeOrchestrator to set the observability Datastream that monitors activity."""
        DPengine._scorp_dp = scorp_dp
