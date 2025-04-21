# DPtree represents the flow of data from the sensor to the cloud.
# - the root node is the sensor
# - the leaf nodes are output data in cloud storage described by Datastream
# - the internal nodes are DataProcessors that process the data in some way
# - the edges are the data flow between the nodes
#
# DPtree supports the following methods for building the tree:
# - connect(from, to): connect two nodes in the tree
# - chain(): connects multiple nodes along a single edge
#
# DPtree supports the following methods for traversing the tree:
# - get_processors(): returns a list of all processors in the tree
# - get_datastreams(): returns a list of all datastreams in the tree
# - search(): searches the tree for a node with the given attributes
#
# connect(from, to) and chain() are used to build the tree.
# - 'from' accepts either a Sensor or a DataProcessor object.
# - 'to' accepts either a DataProcessor or Datastream object.
#
# The first call to connect() or chain() must supply a Sensor object which
# will create the root node of the tree.
# The last call to connect() or chain() must supply a Datastream object which
# will create the leaf node of the tree.
#
# The DPtree is built in a top-down fashion, starting from the root node and working down to the leaf nodes.
# The DPtree creates DPtreeNode objects for each node in the tree.

from typing import List, Optional, Union, Tuple
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import pandas as pd

from sensor_core import api
from sensor_core.sensor import Sensor
from sensor_core.data_processor import DataProcessor
from sensor_core.datastream import Datastream
from sensor_core.dp_engine import DPengine
from sensor_core import configuration as root_cfg

logger = root_cfg.setup_logger("sensor_core")

@dataclass
class DPtreeNodeCfg:
    """Defines the configuration for a node in the DPtree."""
    # In combination, the sensor_id+type_id must be unique on this device.
    # In combination with the device_id, the sensor_id+type_id should be globally unique.
    # Sensor_id is a unique identifier for the physical sensor.  
    # For example, if there are multiple USB mics, it corresponds to the USB port number.
    sensor_id: int
    # The type of sensor, DP or Datastream.  
    # This is used to identify the type & purpose of data being processed.
    # In combination with the sensor_id, this will be unique if this is a Datastream.
    # If this is a Datastreams, the combination of device_id, sensor_id and type_id must be globally unique.
    type_id: str
    # DataProcessors require further indexing to be unique - and this is the node_index.
    node_index: int

    # Human-meaningful description of the node.
    description: str
    input_format: Optional[api.FILE_FORMATS] = None
    output_format: Optional[api.FILE_FORMATS] = None
    input_fields: Optional[list[str]] = None
    output_fields: Optional[list[str]] = None
    
    # Some sources support saving of sample raw recordings to the archive.
    # This string is interpreted by the Sensor or DataProcessor to determine the frequency of 
    # raw data sampling. The format of this string is specific to the Sensor or DataProcessor.
    # The default implementation interprets this string as a float sampling probability (0.0-1.0)
    sample_probability: Optional[str] = None
    # If sampling is enabled, a sample_container must be specified and exist in the cloud storage.
    sample_container: Optional[str] = None


class DPtreeNode(ABC):
    def __init__(self) -> None:
        """
        Initializes a DPtreeNode with the given configuration.

        Args:
            config: The configuration object for this node, which can be a Sensor, DataProcessor, or
                    Datastream.
        """
        self._dpnode_children: dict[int, DPtreeNode] = {}  # Dictionary mapping output streams to child nodes.

    @abstractmethod
    def get_data_id(self) -> str:
        """
        Returns the unique identifier for this node.  Used in filenaming and other data management.

        Returns:
            The unique identifier for this node.
        """
        raise NotImplementedError("get_data_id() must be implemented in subclasses.")

    def set_config(self, config: DPtreeNodeCfg) -> None:
        self._dpnode_config = config

    def get_config(self) -> DPtreeNodeCfg:
        """Return the configuration for this node."""
        return self._dpnode_config

    def set_dp_engine(self, dp_engine: DPengine) -> None:
        """Set the DPengine for this sensor.

        Parameters:
        ----------
        dp_engine: DPengine
            The DPengine to set for this sensor.
        """
        self.dp_engine = dp_engine


    #########################################################################################################
    #
    # Public methods called by Sensor or DataProcessor to log data or save recordings.
    #
    #########################################################################################################
    def log(self, sensor_data: dict) -> None:
        """Called by Sensor/DataProcessor to log a single 'row' of Sensor-generated data."""
        self.dp_engine.log(sensor_data, self.get_data_id())

    def save_data(self, sensor_data: pd.DataFrame) -> None:
        """Called by Sensors to save 1 or more 'rows' of Sensor-generated data.

        save_data() is used to save Pandas dataframes to the datastore defined in the DatastreamType.
        The input_format field of the DatastreamType object must be set to df or csv for this to be used.
        """
        self.dp_engine.save_data(sensor_data, self.get_data_id())

    def save_recording(
        self,
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
        return self.dp_engine.save_recording(
            temporary_file, start_time, end_time, self.get_data_id()
        )
    

class DPtree:
    def __init__(self):
        """
        Initializes an empty DPtree.
        """
        # The sensor is the root node of the tree.
        self.sensor: Optional[Sensor] = None

        # Nodes are stored in a dictionary indexed by data_id
        # The data_id represents the edge between the source and the recipient node.
        self._nodes: dict[str, DPtreeNode] = {}

    def connect(
        self,
        source: Tuple[Union[Sensor, DataProcessor], int],
        sink: Union[DataProcessor, Datastream],
    ) -> None:
        """
        Connects two nodes in the tree.
        Sensor & DataProcessor objects support an out() method that returns an appropriate tuple.
        The index supplied in the tuple is the index of the output stream from the source node.

        Usage example:
            dp_tree.connect(my_sensor_cfg.out(0), data_processor_cfg)
            dp_tree.connect(my_dp_cfg.out(2), datastream_cfg)

        Args:
            output: A tuple where the first element is the source node configuration (Sensor or DataProcessor),
                    and the second element is an integer representing the output stream index.
            input: The destination node configuration, which must be a DataProcessor or Datastream.
        """
        src_instance, stream_index = source  # Unpack the tuple to get the configuration object and output index.
        if not self.sensor:
            if not isinstance(src_instance, Sensor):
                raise ValueError("The first connect() call must provide a Sensor object for the 'from' field.")
            self.sensor = DPtreeNode(src_instance)
        else:
            # The source should already exist in the tree.
            src_data_id = src_instance.get_data_id()
            if src_data_id not in self._nodes:
                raise ValueError(f"{src_data_id} is not connected.")

        data_id = sink.get_data_id()
        if data_id in self._nodes:
            raise ValueError(f"{sink.get_data_id()} is already connected.")

        self._nodes[data_id] = sink

        if stream_index in src_instance._dpnode_children:
            raise ValueError(f"Output stream {stream_index} is already connected from {src_data_id}.")
        
        # Build the tree by storing the child node with the output index as the key.
        src_instance._dpnode_children[stream_index] = sink


    def chain(self, *configs: Union[Sensor, DataProcessor, Datastream]) -> None:
        """
        Connects multiple nodes in a single chain.

        Args:
            configs: A sequence of configuration objects to connect in order.
        """
        for i in range(len(configs) - 1):
            self.connect((configs[i], 0), configs[i + 1])  # Default metadata value is 0.

    def get_node(self, data_id: str) -> DPtreeNode:
        """
        Retrieves a node from the tree by its data_id.

        Args:
            data_id: The unique identifier for the node.

        Returns:
            The DPtreeNode object.

        Raises:
            KeyError: If the node with the specified data_id does not exist in the tree.
        """
        return self._nodes.get(data_id)
    
    def get_processors(self) -> List[DataProcessor]:
        """
        Retrieves all processor nodes in the tree.

        Returns:
            A list of DataProcessor objects representing the processors in the tree.
        """
        return [node for node in self._nodes if isinstance(node, DataProcessor)]

    def get_datastreams(self) -> List[Datastream]:
        """
        Retrieves all datastream nodes in the tree.

        Returns:
            A list of Datastream objects representing the datastreams in the tree.
        """
        return [node for node in self._nodes if isinstance(node, Datastream)]

    def search(self, **attributes) -> Optional[Union[Sensor, DataProcessor, Datastream]]:
        """
        Searches the tree for a node with the given attributes.

        Args:
            attributes: Key-value pairs of attributes to match against node configurations.

        Returns:
            The configuration object of the matching node, or None if no match is found.
        """
        for node in self._nodes:
            if all(getattr(node, key, None) == value for key, value in attributes.items()):
                return node
        return None

    ###############################################################################################
    # Validate methods
    ###############################################################################################
    def validate(self) -> None:
        """
        Validates the tree structure.

        Raises:
            ValueError: If the tree is invalid (e.g., missing a root node, invalid leaf nodes, or disconnected nodes).
        """
        if not self.sensor:
            raise ValueError("The tree must have a root node.")
        if not isinstance(self.sensor, Sensor):
            raise ValueError("The root node must be a Sensor object.")
        if not self._validate_leaf_nodes():
            raise ValueError("All leaf nodes must be Datastream objects.")
        if not self._validate_all_nodes_connected():
            raise ValueError("All nodes must be connected.")

    def _validate_leaf_nodes(self) -> bool:
        """
        Checks that all leaf nodes in the tree are Datastream objects.

        Returns:
            True if all leaf nodes are valid, False otherwise.
        """
        for node in self._nodes.values():
            if not node._dpnode_children and not isinstance(node, Datastream):
                return False
        return True

    def _validate_all_nodes_connected(self) -> bool:
        """
        Checks that all nodes in the tree are connected.

        Returns:
            True if all nodes are connected, False otherwise.
        """
        visited = set()

        def dfs(node: DPtreeNode) -> None:
            if node in visited:
                return
            visited.add(node)
            for child in node._dpnode_children.values():  # Iterate over child nodes in the dictionary.
                dfs(child)

        dfs(self.sensor)
        return len(visited) == len(self._nodes)
