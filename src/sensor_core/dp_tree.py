from typing import List, Optional, Tuple

from sensor_core import configuration as root_cfg
from sensor_core.dp_tree_node import DPtreeNode
from sensor_core.sensor import Sensor

logger = root_cfg.setup_logger("sensor_core")

###############################################################################################
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
#################################################################################################
class DPtree:
    """Represents a tree structure for data processing nodes.
    The tree consists of a root node (Sensor) and various child nodes (DataProcessors) with leaf nodes 
    being Datastreams.

    The tree is used to manage the flow of data from the sensor to the cloud storage.
    The tree supports the following operations:
    - connect: Connects two nodes in the tree.
    - chain: Connects multiple nodes in a single chain.
    - validate: Validates the tree structure to ensure all nodes are connected and valid.
    - get_node: Retrieves a node from the tree by its data_id.
    - get_processors: Retrieves all processor nodes in the tree.
    - get_datastreams: Retrieves all datastream nodes in the tree.
    """
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
        source: Tuple[DPtreeNode, int],
        sink: DPtreeNode,
    ) -> None:
        """
        Connects two nodes in the tree.
        Sensor & DataProcessor objects support an out() method that returns an appropriate tuple.
        The index supplied in the tuple is the index of the output stream from the source node.

        Usage example:
            dp_tree.connect(my_sensor_cfg.out(0), data_processor_cfg)
            dp_tree.connect(my_dp_cfg.out(2), datastream_cfg)

        Args:
            output: A tuple where the first element is the source node configuration 
                    (Sensor or DataProcessor), and the second element is an integer representing the 
                    output stream identifier.
            input: The destination node configuration, which must be a DataProcessor or Datastream.
        """
        src_instance, stream_index = source
        if not self.sensor:
            if not isinstance(src_instance, Sensor):
                raise ValueError("The first connect() call must provide a Sensor object "
                                 "for the 'from' field.")
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


    def chain(self, *configs: DPtreeNode) -> None:
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
    
    def get_processors(self) -> List[DPtreeNode]:
        """
        Retrieves all processor nodes in the tree.

        Returns:
            A list of DataProcessor objects representing the processors in the tree.
        """
        return [node for node in self._nodes if self.is_instance_of_type(node, "DataProcessor")]

    def get_datastreams(self) -> List[DPtreeNode]:
        """
        Retrieves all datastream nodes in the tree.

        Returns:
            A list of Datastream objects representing the datastreams in the tree.
        """
        return [node for node in self._nodes if self.is_instance_of_type(node, "Datastream")]

    def export(self) -> dict:
        """
        Exports the tree structure as a dictionary.

        Returns:
            A dictionary representing the tree structure.
        """
        # DptreeNode.export() is recursive - so start with the root node.
        return self.sensor.export()

    ###############################################################################################
    # Validate methods
    ###############################################################################################
    def validate(self) -> None:
        """
        Validates the tree structure.

        Raises:
            ValueError: If the tree is invalid.
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
            if (not node._dpnode_children) and (not self.is_instance_of_type(node, "Datastream")):
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

    @staticmethod
    def is_instance_of_type(obj, type_name: str) -> bool:
        """
        Checks if the given object is of a specific type using the type's name.

        Args:
            obj: The object to check.
            type_name: The name of the type as a string.

        Returns:
            True if the object is of the specified type, False otherwise.
        """
        return type(obj).__name__ == type_name