
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.cloud_connector import CloudConnector
from sensor_core.utils import file_naming
from sensor_core.utils.journal_pool import JournalPool
from sensor_core.utils.sc_test_emulator import ScEmulator
from sensor_core.dp_tree_node_types import DPtreeNodeCfg

logger = root_cfg.setup_logger("sensor_core")


@dataclass
class DPnodeStats:
    timestamp: datetime
    count: int
    duration: float = 0.0

class DPtreeNode(ABC):
    """Abstract base class for nodes in the DPtree.
    Sensor, DataProcessor, and Datastream all inherit from this class.
    """
    _scorp_dp = None  # Special Datastream for recording performance of the data pipeline.
    _score_dp = None  # Special Datastream for recording sample count / duration from the data pipeline.

    def __init__(self, config: DPtreeNodeCfg, sensor_index: int) -> None:
        """
        Initializes a DPtreeNode with the given configuration.

        Args:
            config: The configuration object for this node, which can be a Sensor, DataProcessor, or
                    Datastream.
        """
        self._dpnode_config: DPtreeNodeCfg = config
        self.sensor_index: int = sensor_index

        self._dpnode_children: dict[int, DPtreeNode] = {}  # Dictionary mapping output streams to child nodes.

        # Record the number of datapoints recorded by this Datastream
        self._dpnode_stats: list[DPnodeStats] = []

        # Create the Journals that we will use to store this DPtree's output.
        self.journal_pool: JournalPool = JournalPool.get(mode=root_cfg.get_mode())


    @abstractmethod
    def get_data_id(self) -> str:
        """
        Returns the unique identifier for this node.  Used in filenaming and other data management.

        Returns:
            The unique identifier for this node.
        """
        raise NotImplementedError("get_data_id() must be implemented in subclasses.")

    def is_leaf(self) -> bool:
        """Check if this node is a leaf node (i.e., it has no children).

        Returns:
            True if this node is a leaf node, False otherwise.
        """
        return len(self._dpnode_children) == 0

    def get_config(self) -> DPtreeNodeCfg:
        """Return the configuration for this node."""
        return self._dpnode_config

    def export(self) -> dict:
        """Export the configuration of this node and all its descendants as a dictionary.

        Returns:
            A dictionary representing the configuration.
            The keys in the dictionary are the indices used in Connect() to connect the nodes.
        """
        # Export the configuration of this node as a dictionary.
        # The config is a dataclass, so we can use the __dict__ attribute to get the fields.
        cfg_export = {}
        cfg_export["node_cfg"] = asdict(self.get_config())
        cfg_export["children"] = {}
        for child_index, child in self._dpnode_children.items():
            # Recursively export the configuration of each child node.
            cfg_export["children"][child_index] = child.export()
        return cfg_export

    #########################################################################################################
    #
    # Public methods called by Sensor or DataProcessor to log data or save recordings.
    #
    #########################################################################################################
    def log(self, stream_index: int, sensor_data: dict) -> None:
        """Called by Sensor/DataProcessor to log a single 'row' of Sensor-generated data."""
        data_id = self.get_data_id()
        config = self._dpnode_config

        logger.debug(f"Log sensor_data: {sensor_data} to DPnode:{data_id} stream {stream_index}")

        # Check that the fields defined for this DatastreamType are present in the sensor_data
        # If any fields are missing, raise an exception
        log_data = {}
        if config.outputs[stream_index] is not None:
            for field in config.outputs[stream_index].fields:
                if field in api.REQD_RECORD_ID_FIELDS:
                    continue
                elif field in sensor_data:
                    log_data[field] = sensor_data[field]
                else:
                    raise Exception(
                        f"Field {field} missing from data logged to {data_id}; "
                        f"Expected:{config.input_stream.fields}; "
                        f"Received the following fields:{sensor_data.keys()}"
                    )

        # Add the Datastream indices (datastream_type_id, device_id, sensor_id) and a
        # timestamp to the log_data
        log_data[api.RECORD_ID.VERSION.value] = "V3"
        log_data[api.RECORD_ID.DATA_TYPE_ID.value] = config.type_id
        log_data[api.RECORD_ID.DEVICE_ID.value] = root_cfg.my_device_id
        log_data[api.RECORD_ID.SENSOR_INDEX.value] = self.sensor_index
        log_data[api.RECORD_ID.TIMESTAMP.value] = api.utc_to_iso_str()
        log_data[api.RECORD_ID.NAME.value] = root_cfg.my_device.name

        self.journal_pool.add_rows(config, stream_index, [log_data], api.utc_now())

        # Track the number of measurements recorded
        # These data points don't have a duration - that only applies to recordings.
        self._dpnode_stats.append(DPnodeStats(api.utc_now(), 1))

        # We also spam the data to the logger for easy debugging and display in the bcli
        if config.type_id not in api.SYSTEM_DS_TYPES:
            logger.info(f"Save log: {log_data!s}")
        else:
            logger.debug(f"Save log: {log_data!s}")


    def save_data(self, stream_index: int, sensor_data: pd.DataFrame) -> None:
        """Called by Sensors to save 1 or more 'rows' of Sensor-generated data.

        save_data() is used to save Pandas dataframes to the datastore defined in the DatastreamType.
        The input_format field of the DatastreamType object must be set to df or csv for this to be used.
        """
        config = self._dpnode_config

        self.journal_pool.add_rows_from_df(config, stream_index, sensor_data)

        # Track the number of measurements recorded
        # These data points don't have a duration - that only applies to recordings.
        self._dpnode_stats.append(DPnodeStats(api.utc_now(), 1))


    def save_recording(
        self,
        stream_index: int,
        temporary_file: Path,
        start_time: datetime,
        end_time: Optional[datetime] = None,
    ) -> Path:
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
        
        config = self._dpnode_config

        # If on EDGE, files are either saved to the root_cfg.EDGE_PROCESSING_DIR if there are DPs registered,
        # or to the root_cfg.EDGE_UPLOAD_DIR if not.
        # On the ETL, files are saved to the root_cfg.ETL_PROCESSING_DIR
        if root_cfg.get_mode() == root_cfg.Mode.EDGE:
            if self.is_leaf():
                save_dir = root_cfg.EDGE_UPLOAD_DIR
            else:
                save_dir = root_cfg.EDGE_PROCESSING_DIR
        else:
            assert False, "save_recording() should not be called in ETL mode"

        new_fname = self._save_recording(
            src_file=temporary_file,
            dst_dir=save_dir,
            start_time=start_time,
            suffix=config.outputs[stream_index].format,
            end_time=end_time,
        )

        return new_fname


    #########################################################################################################
    #
    # Data functions called by DataProcessors to save files
    #
    # Normally a DataProcessor returns a DataFrame to be passed to the next DP, but in some cases the
    # DP needs to save a sub-sampled recording.
    #########################################################################################################
    def save_sub_recording(
        self,
        stream_index: int,
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
        config = self._dpnode_config
        assert config.outputs[stream_index] is not None, (
            f"output_format must be specified on dp_config {config}"
        )
        suffix = config.outputs[stream_index].format

        # We save the recording to the EDGE|ETL_PROCESSING_DIR if there are more DPs to run, 
        # otherwise we save it to the EDGE|ETL_UPLOAD_DIR
        if root_cfg.get_mode() == root_cfg.Mode.EDGE:
            if self.is_leaf():
                save_dir = root_cfg.EDGE_UPLOAD_DIR
            else:
                save_dir = root_cfg.EDGE_PROCESSING_DIR
        else:
            if self.is_leaf():
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

    
    def log_sample_data(self, sample_period_start_time: datetime, score_dp: "DPtreeNode") -> None:
        """Provide the count & duration of data samples recorded (environmental, media, etc)
        since the last time log_sample_data was called.

        This is used by EdgeOrchestrator to periodically log observability data
        """
        # We need to traverse all nodes in the tree and call log_sample_data on each node
        count = sum(x.count for x in self._dpnode_stats)
        duration = sum(x.duration for x in self._dpnode_stats)

        # Reset the datastream stats for the next period
        self._dpnode_stats = []

        # Log sample data
        score_dp.log({
            "observed_type_id": self.get_config().type_id,
            "observed_sensor_index": self.sensor_index,
            "sample_period": api.utc_to_iso_str(sample_period_start_time),
            "count": str(count),
            "duration": str(duration),
        })

    #########################################################################################################
    #
    # Private methods in support of Sensors
    #
    #########################################################################################################
    def _save_recording(
        self,
        stream_index: int,
        src_file: Path,
        dst_dir: Path,
        start_time: datetime,
        suffix: str,
        end_time: Optional[datetime] = None,
        offset_index: Optional[int] = None,
        secondary_offset_index: Optional[int] = None,
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

        # Generate the filename for the recording
        new_fname: Path = file_naming.get_record_filename(
            dst_dir, self.get_data_id(), suffix, start_time, end_time, offset_index, secondary_offset_index
        )

        # If we're in test mode, we may cap the number of recordings we save.
        if root_cfg.TEST_MODE == root_cfg.MODE.TEST:
            if not ScEmulator.get_instance().ok_to_save_recording(self.get_data_id):
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
            cloud_container = self.get_config().outputs[stream_index].cloud_container
            assert cloud_container is not None
            CloudConnector.get_instance().upload_to_container(cloud_container, 
                                                              [new_fname], delete_src=True)

        # Track the number of measurements recorded
        if end_time is None:
            self._dpnode_stats.append(DPnodeStats(api.utc_now(), 1))
        else:
            # Track duration if this file represents a period
            self._dpnode_stats.append(
                DPnodeStats(api.utc_now(), 1, (end_time - start_time).total_seconds())
            )

        logger.debug(f"Saved recording {src_file.name} as {new_fname.name}")

        return new_fname
