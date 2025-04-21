from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Thread
from time import sleep
from typing import Callable, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import yaml

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.cloud_connector import CloudConnector
from sensor_core.config_objects import DataProcessorCfg, SensorCfg, DatastreamCfg
from sensor_core.configuration import Mode
from sensor_core.data_processor import DataProcessor
from sensor_core.system_datastreams import (
    FAIRY_DS_TYPE_ID,
    SCORE_DS_TYPE_ID,
    SCORP_DS_TYPE_ID,
    SYSTEM_DS_TYPES,
)
from sensor_core.utils import file_naming
from sensor_core.utils.journal_pool import JournalPool
from sensor_core.utils.sc_test_emulator import ScEmulator
from sensor_core.dp_tree import DPtree

logger = root_cfg.setup_logger("sensor_core")

# Run frequency of the Datastream worker thread
# Normally 60 seconds, but overridden in tests
RUN_FREQUENCY_SECS = 60


@dataclass
class DPengineStats:
    timestamp: datetime
    count: int
    duration: float = 0.0


class DPengine(Thread):
    """A DPengine is the thread that processes data through a DPtree.
    Note: the Sensor has a separate thread.
    """

    _system_dpe: DPengine = None

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

        # Create the Journals that we will use to store this DPtree's output.
        self.journal_pool: JournalPool = JournalPool.get(mode=root_cfg.get_mode())

        # Record the number of datapoints recorded by this Datastream
        self._datastream_stats: list[DPengineStats] = []

    def get_system_dpe() -> DPengine:
        """Return the system DPengine object."""
        if DPengine._system_dpe is None:
            raise Exception("System DPengine not initialised")
        return DPengine._system_dpe

    #########################################################################################################
    #
    # Public methods called by the Sensor or DataProcessor to log or save data
    #
    #########################################################################################################
    def log(self, sensor_data: dict, data_id: str) -> None:
        """Called by Sensors to log a single 'row' of Sensor-generated data."""
        logger.debug(f"Log sensor_data: {sensor_data} to DS:{self}")

        # Check that the Datastream has been started
        if self.dpe_start_time is None:
            logger.warning(f"Log arrived before Datastream {self} started.")

        # Check that the fields defined for this DatastreamType are present in the sensor_data
        # If any fields are missing, raise an exception
        log_data = {}
        config = self.dp_tree.get_node(data_id)._dpnode_config
        if config.input_fields:
            for field in config.input_fields:
                if field in api.REQD_RECORD_ID_FIELDS:
                    continue
                elif field in sensor_data:
                    log_data[field] = sensor_data[field]
                else:
                    raise Exception(
                        f"Field {field} missing from data logged to {data_id}; "
                        f"Expected:{config.input_fields}; "
                        f"Received the following fields:{sensor_data.keys()}"
                    )

        # Add the Datastream indices (datastream_type_id, device_id, sensor_id) and a
        # timestamp to the log_data
        log_data[api.RECORD_ID.VERSION.value] = "V3"
        log_data[api.RECORD_ID.DATA_TYPE_ID.value] = config.type_id
        log_data[api.RECORD_ID.DEVICE_ID.value] = self.device_id
        log_data[api.RECORD_ID.SENSOR_INDEX.value] = self.sensor_index
        log_data[api.RECORD_ID.TIMESTAMP.value] = api.utc_to_iso_str()
        log_data[api.RECORD_ID.NAME.value] = root_cfg.my_device.name

        self.journal_pool.add_rows(config, [log_data], api.utc_now())

        # Track the number of measurements recorded
        # These data points don't have a duration - that only applies to recordings.
        self._datastream_stats.append(DPengineStats(api.utc_now(), 1))

        # We also spam the data to the logger for easy debugging and display in the bcli
        if config.type_id not in SYSTEM_DS_TYPES:
            logger.info(f"Save log: {log_data!s}")
        else:
            logger.debug(f"Save log: {log_data!s}")

    def save_data(self, sensor_data: pd.DataFrame, data_id: str) -> None:
        """Called by Sensors to save 1 or more 'rows' of Sensor-generated data.

        save_data() is used to save Pandas dataframes to the datastore defined in the DatastreamType.
        The input_format field of the DatastreamType object must be set to df or csv for this to be used.
        """
        logger.debug(f"Saving data on sensor {self!r}")
        config = self.dp_tree.get_node(data_id)._dpnode_config

        self.journal_pool.add_rows_from_df(config, sensor_data)

        # Track the number of measurements recorded
        # These data points don't have a duration - that only applies to recordings.
        self._datastream_stats.append(DPengineStats(api.utc_now(), 1))

    def save_recording(
        self,
        temporary_file: Path,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        data_id: Optional[str] = "") -> Path:
        """Called by a Sensor or DataProcessor to save a recording file to the appropriate datastore.
        This should only be used by Sensors or **primary** datastreams.

        Note: save_recording() will *rename* (ie move) the supplied temporary_file.
        This method will manage storage and subsequent processing of the temporary_file
        in line with the definition of this DatastreamType.
        The file name of the saved recording will be as per the naming convention defined in 
        Datastream.parse_filename().
        Do not use to save dataframes - see Datastream.save_data().

        Parameters
        ----------
        temporary_file: Path
            The path to the file that should be saved.
        start_time: datetime
            The time that the recording started.
        end_time:datetime
            Tthe time that the recording ended.
        """

        config = self.dp_tree.get_node(data_id)._dpnode_config

        # If on EDGE, files are either saved to the root_cfg.EDGE_PROCESSING_DIR if there are DPs registered,
        # or to the root_cfg.EDGE_UPLOAD_DIR if not.
        # On the ETL, files are saved to the root_cfg.ETL_PROCESSING_DIR
        if root_cfg.get_mode() == Mode.EDGE:
            if isinstance(config, DatastreamCfg):
                save_dir = root_cfg.EDGE_UPLOAD_DIR
            else:
                save_dir = root_cfg.EDGE_PROCESSING_DIR
        else:
            assert False, "save_recording() should not be called in ETL mode"

        new_fname = self._save_recording(
            src_file=temporary_file,
            dst_dir=save_dir,
            start_time=start_time,
            suffix=config.input_format,
            end_time=end_time,
        )

        return new_fname

    def save_FAIR_record(self, record: dict) -> None:
        """Save a supplementary FAIR record describing this Datastream to the FAIR archive.

        FAIR records are automatically saved for the Datastream configuration,
        so this is for supplementary details from the subclassed Sensor or DataProcessor.

        Parameters
        ----------
        record: dict
            The supplementary FAIR record to be saved.  Must be a dictionary and will be
            converted to YAML.
        """
        # We don't save FAIR records for system datastreams
        if self.ds_config.type_id in SYSTEM_DS_TYPES:
            return

        # Wrap the "record" data in a FAIR record
        wrap: dict[str, dict | str | list] = {}
        wrap[api.RECORD_ID.VERSION.value] = "V3"
        wrap[api.RECORD_ID.DATA_TYPE_ID.value] = self.ds_config.type_id
        wrap[api.RECORD_ID.DEVICE_ID.value] = self.device_id
        wrap[api.RECORD_ID.SENSOR_INDEX.value] = str(self.sensor_index)
        wrap[api.RECORD_ID.TIMESTAMP.value] = api.utc_to_iso_str()
        wrap["record"] = record
        # We always include the list of mac addresses for all devices in this experiment (fleet_config)
        # This enables the dashboard to check that all devices are present and working.
        wrap["fleet"] = list(root_cfg.INVENTORY.keys())

        # Dump the config record to a YAML file
        tmp_file = file_naming.get_temporary_filename(suffix="yaml")
        Path(tmp_file).parent.mkdir(parents=True, exist_ok=True)
        with open(tmp_file, "w") as f:
            yaml.dump(wrap, f)

        # In order to get the right DS_ID for the FAIR record, we need to over-ride the 
        # FAIRY_DS_TYPE with the correct DS_ID
        DPengine._fairy_ds._save_recording(
            src_file=tmp_file, 
            dst_dir=root_cfg.EDGE_UPLOAD_DIR,
            start_time=api.utc_now(),
            suffix="yaml",
            override_ds_id=self.ds_id)

    def log_sample_data(self, sample_period_start_time: datetime) -> dict:
        """Provide the count & duration of data samples recorded (environmental, media, etc)
        since the last time log_sample_data was called.

        This is used by EdgeOrchestrator to periodically log observability data
        """
        count = sum(x.count for x in self._datastream_stats)
        duration = sum(x.duration for x in self._datastream_stats)

        # Reset the datastream stats for the next period
        self._datastream_stats = []

        # Log sample data
        DPengine._score_ds.log({
            "observed_type_id": self.ds_config.type_id,
            "observed_sensor_index": self.sensor_index,
            "sample_period": api.utc_to_iso_str(sample_period_start_time),
            "count": str(count),
            "duration": str(duration),
        })

        return {"count": count, "duration": duration}

    def get_temporary_filename(self, suffix: str) -> Path:
        """Generate a temporary filename in the TMP_DIR with the specified suffix."""
        return file_naming.get_temporary_filename(suffix)
    
    def get_sensor_cfg(self) -> Optional[SensorCfg]:
        """Return the SensorCfg object for this Datastream"""
        return self.dp_tree

    #########################################################################################################
    #
    # Data functions called by DataProcessors to save files
    #
    # Normally a DataProcessor returns a DataFrame to be passed to the next DP, but in some cases the
    # DP needs to save a sub-sampled recording.
    #########################################################################################################
    def save_sub_recording(
        self,
        data_processor: DataProcessor,
        temporary_file: Path,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        offset_index: Optional[int] = None,
        secondary_offset_index: Optional[int] = None,
    ) -> Path:
        """Called by DataProcessors to save sub-sample recording files to the appropriate datastore.
        Note: save_sub_recording() will *rename* (ie delete) the supplied temporary_file
        This method will manage storage and subsequent processing of the temporary_file
        in line with the definition of this DatastreamType.
        The file name of the saved recording will be as per the naming convention defined in 
        Datastream.parse_filename().
        Do not use to save dataframes - see Datastream.save_data().

        Parameters
        ----------
        data_processor: DataProcessor
            The DataProcessor object that is saving the file.
        temporary_file: Path
            The path to the file that should be saved.
        start_time: datetime
            The time that the recording started.
        end_time:datetime
            The time that the recording ended.
        offset_index: optional int
            Typically a frame number in the recording, if applicable.
        secondary_offset_index: optional int
            An index that can be used to differentiate between multiple subsamples from a given frame.
        """

        # We save the recording with the suffix defined in the DataProcessorConfig object as the DP's output
        dp_config, _, is_last = data_processor._get_dp_config()
        assert dp_config.output_format is not None, (
            f"output_format must be specified on dp_config {dp_config}"
        )
        suffix = dp_config.output_format

        # We save the recording to the EDGE|ETL_PROCESSING_DIR if there are more DPs to run, 
        # otherwise we save it to the EDGE|ETL_UPLOAD_DIR
        if root_cfg.get_mode() == Mode.EDGE:
            if is_last:
                save_dir = root_cfg.EDGE_UPLOAD_DIR
            else:
                save_dir = root_cfg.EDGE_PROCESSING_DIR
        else:
            if is_last:
                save_dir = root_cfg.ETL_PROCESSING_DIR
            else:
                save_dir = root_cfg.ETL_ARCHIVE_DIR

        new_fname = self._save_recording(
            src_file=temporary_file,
            dst_dir=save_dir,
            start_time=start_time,
            suffix=suffix,
            end_time=end_time,
            offset_index=offset_index,
            secondary_offset_index=secondary_offset_index,
        )

        return new_fname

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
                    if dp_config.input_format == "df":
                        assert input_df is not None, "input_df is null on df run"
                        logger.debug(f"Invoking {dp} with {input_df}")
                        output_df = dp.process_data(self, input_df, self._get_context(dp_config))
                    elif dp_config.input_format == "csv":
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
                    self.get_system_dpe().log(
                        {
                            "mode": Mode.EDGE.value,
                            "observed_type_id": ds_type.type_id,
                            "observed_sensor_index": self.sensor_index,
                            "data_processor_id": dp_config.dp_class_ref,
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
                logger.debug(f"Saving data from {self.ds_id} to journal")
                self.journal_pool.add_rows_from_df(self.ds_config, input_df, api.utc_now())

            # We want to run this loop every minute, so see how long it took us since the start_time
            sleep_time = RUN_FREQUENCY_SECS - (api.utc_now() - start_time).total_seconds()
            logger.debug(f"DataProcessor loop sleeping for {sleep_time} seconds")
            if sleep_time > 0:
                sleep(sleep_time)

    def etl_run(self) -> None:
        ds_type: DPengine = self.ds_config

        # All DataProcessors should be registered by now.
        # If now are registered we can exit the thread
        if ds_type.cloud_processors is None or len(self._cloud_dps) == 0:
            logger.debug(f"No DataProcessors registered; exiting; {self!r}; {root_cfg.get_mode()}")
            return

        while not self._stop_requested:
            start_time = api.utc_now()
            input_df: Optional[pd.DataFrame] = None
            dp: DataProcessor
            dp_config: DataProcessorCfg

            for dp_index, dp in enumerate(self._cloud_dps):
                try:
                    exec_start_time = api.utc_now()
                    output_df: Optional[pd.DataFrame] = None
                    dp_config, i, is_last_dp = dp._get_dp_config()
                    assert i == dp_index, f"DP index mismatch {i} != {dp_index}"
                    assert self.ds_config.cloud_processors is not None

                    #########################################################################################
                    # Invoke the DataProcessor
                    #
                    # Standard chaining involves passing a Dataframe along the DP chain.
                    # The first DP may be invoked with recording files (jpg, h264, wav, etc) or a CSV
                    # as defined in the dp_config
                    #########################################################################################
                    if dp_config.input_format == "df":
                        # Second or subsequent DP in chain
                        assert dp_index != 0, "input_format is df, but this is the first DP"
                        if input_df is not None:
                            logger.debug(f"Invoking {dp} with {input_df}")
                            output_df = dp.process_data(self, input_df, self._get_context(dp_config))
                    elif dp_config.input_format == "csv":
                        assert dp_index == 0, f"Only the first DP can load a CSV file: {dp_index!s}"
                        assert input_df is None, "input_df not null on csv run"
                        # Find and load CSVs as DFs
                        input_df = self._get_csv_as_df()
                        if input_df is not None:
                            logger.debug(f"Invoking {dp} with {input_df}")
                            output_df = dp.process_data(self, input_df, self._get_context(dp_config))
                    else:
                        assert input_df is None, "input_df not null on file run"
                        # First DP may process recording files
                        input_files = self._get_ds_files(dp)
                        if input_files is not None:
                            logger.debug(f"Invoking {dp} with {input_files}")
                            output_df = dp.process_data(self, input_files, self._get_context(dp_config))
                            # Clear up the files now they've been processed.
                            # Any files that were meant to be uploaded will have been moved directly
                            # to the upload directory.
                            # Sampling is done on initial save_recording.
                            for f in input_files:
                                if f.exists():
                                    f.unlink()

                    # Validate the output_df before passing it to the next DP in the chain
                    if output_df is not None:
                        input_df = self._validate_output(output_df, dp)

                    # Log the processing time
                    exec_time = api.utc_now() - exec_start_time
                    self._scorp_ds.log(
                        {
                            "mode": Mode.ETL.value,
                            "observed_type_id": ds_type.type_id,
                            "observed_sensor_index": self.sensor_index,
                            "data_processor_id": dp_config.dp_class_ref,
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
                # Split the input_df by day so we can save it to the appropriate journal
                ts: str = api.RECORD_ID.TIMESTAMP.value
                input_df[ts] = pd.to_datetime(input_df[ts])
                input_df["date"] = input_df[ts].dt.date
                grouped = input_df.groupby("date")

                # Create a dictionary of DataFrames, each corresponding to a unique date
                dfs_by_date = {date: group.drop(columns=["date"]) for date, group in grouped}

                # Now dfs_by_date is a dictionary where keys are unique dates and values are DataFrames
                for date, df in dfs_by_date.items():
                    ts_date = pd.to_datetime(str(date))
                    self.journal_pool.add_rows_from_df(self.ds_config, df, ts_date)

            # We want to run this loop every minute, so see how long it took us since the start_time
            sleep_time = RUN_FREQUENCY_SECS - (api.utc_now() - start_time).total_seconds()
            logger.info(f"DataProcessor loop sleeping for {sleep_time} seconds")
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

    #########################################################################################################
    #
    # Private methods in support of Sensors
    #
    #########################################################################################################
    def _save_recording(
        self,
        src_file: Path,
        dst_dir: Path,
        start_time: datetime,
        suffix: str,
        end_time: Optional[datetime] = None,
        offset_index: Optional[int] = None,
        secondary_offset_index: Optional[int] = None,
        override_ds_id: Optional[str] = None,
    ) -> Path:
        """Private method that handles saving of recordings from Datastreams or DataProcessors.

        Parameters
        ----------
        temporary_file: Path
            The path to the file that should be saved.
        start_time: datetime
            The time that the recording started.
        suffix: str
            The file extension of the recording.
        end_time:datetime
            Tthe time that the recording ended.
        offset_index: optional int
            Typically a frame number in the recording, if applicable.
        secondary_offset_index: optional int
            An index that can be used to differentiate between multiple subsamples from a given frame.
        override_ds_id: optional str
            Only for use with FAIR records, where we over-ride the FAIR DS's ds_id with that of the sensor.
        """
        # Check that the file is present and not empty
        if not src_file.exists():
            raise FileNotFoundError(f"File {src_file} not found.")

        # Check that the file is of the correct format.
        # This should match the suffix provided.
        if not src_file.suffix.endswith(suffix):
            raise ValueError(f"File format {src_file.suffix} does not match expected suffix {suffix}.")
        
        # Check that the start_time and end_time are valid
        if not isinstance(start_time, datetime):
            raise ValueError("Start_time must be a valid datetime object.")
        if end_time is not None:
            if not isinstance(end_time, datetime):
                raise ValueError("End_time must be a valid datetime object.")
            
        # Check that the start_time and end_time are both timezone aware
        if start_time.tzinfo is None:
            logger.warning(f"{root_cfg.RAISE_WARN}start_time must be timezone aware. "
                           "Use api.utc_now() to get the current time.")
            start_time = start_time.replace(tzinfo=ZoneInfo("UTC"))
        if end_time is not None and end_time.tzinfo is None:
            logger.warning(f"{root_cfg.RAISE_WARN}end_time must be timezone aware. "
                           "Use api.utc_now() to get the current time.")
            end_time = end_time.replace(tzinfo=ZoneInfo("UTC"))

        if end_time is not None:
            if start_time > end_time:
                raise ValueError(f"Start_time ({start_time}) must be before end_time ({end_time}).")

        # Check that the Datastream has been started
        if self.dpe_start_time is None:
            # This is most likely a race condition on start up.
            logger.warning(f"Datastream has not been started: {self}; race condition on start up?")

        # If override_type_id is provided, check this is the FAIRY DS.
        ds_id = self.ds_id
        if override_ds_id is not None:
            if self.ds_config.type_id != FAIRY_DS_TYPE_ID:
                raise ValueError(f"override_type_id can only be used with FAIRY_DS_TYPE: "
                                 f"{self.ds_config.type_id}")
            ds_id = override_ds_id

        # Generate the filename for the recording
        new_fname: Path = file_naming.get_record_filename(
            dst_dir, ds_id, suffix, start_time, end_time, offset_index, secondary_offset_index
        )

        # If we're in test mode, we may cap the number of recordings we save.
        if root_cfg.TEST_MODE == root_cfg.MODE.TEST:
            if not ScEmulator.get_instance().ok_to_save_recording(self.ds_id):
                logger.info(f"Test mode recording cap hit; deleting {src_file.name}")
                if src_file.exists():
                    src_file.unlink()
                return new_fname

        # Move the file to the dst_dir (EDGE_UPLOAD_DIR or EDGE_PROCESSING_DIR)
        # This will be the first step in the processing of the file
        # After processing, the file will be moved to the appropriate datastore
        if new_fname != src_file:
            if new_fname.exists():
                # Increment the new_fname to avoid overwriting existing files
                new_fname = file_naming.increment_filename(new_fname)
            new_fname = src_file.rename(new_fname)

        # If the dst_dir is EDGE_UPLOAD_DIR, we can use direct upload to the cloud
        if dst_dir == root_cfg.EDGE_UPLOAD_DIR:
            assert self.ds_config.cloud_container is not None
            CloudConnector.get_instance().upload_to_container(self.ds_config.cloud_container, 
                                                              [new_fname], delete_src=True)

        # Track the number of measurements recorded
        if end_time is None:
            self._datastream_stats.append(DPengineStats(api.utc_now(), 1))
        else:
            # Track duration if this file represents a period
            self._datastream_stats.append(
                DPengineStats(api.utc_now(), 1, (end_time - start_time).total_seconds())
            )

        logger.debug(f"Saved recording {src_file.name} as {new_fname.name}")

        return new_fname

    @staticmethod
    def _set_system_dpe(sys_dptree: DPtree) -> None:
        """Called by EdgeOrchestrator to set the observability Datastream that monitors activity."""
        DPengine._system_dpe = sys_dptree
