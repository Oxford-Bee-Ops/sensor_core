import pytest
from sensor_core.dp_tree import DPtree, DPtreeNode
from sensor_core.config_objects import SensorCfg, DataProcessorCfg, Datastream


@pytest.fixture
def sample_tree():
    """
    Creates a sample DPtree for testing.
    """
    tree = DPtree()
    sensor = SensorCfg(name="sensor1")
    processor1 = DataProcessorCfg(name="processor1")
    processor2 = DataProcessorCfg(name="processor2")
    datastream = Datastream(name="datastream1")

    tree.connect(sensor.out(0), processor1)
    tree.connect(processor1.out(0), processor2)
    tree.connect(processor2.out(0), datastream)

    return tree, sensor, processor1, processor2, datastream


def test_connect(sample_tree):
    """
    Test the connect() method.
    """
    tree, sensor, processor1, processor2, datastream = sample_tree

    assert tree.root.config == sensor
    assert processor1 in [child.config for child in tree.root.children.values()]
    assert processor2 in [child.config for child in tree._find_or_create_node(processor1).children.values()]
    assert datastream in [child.config for child in tree._find_or_create_node(processor2).children.values()]


def test_chain():
    """
    Test the chain() method.
    """
    tree = DPtree()
    sensor = SensorCfg(name="sensor1")
    processor1 = DataProcessorCfg(name="processor1")
    processor2 = DataProcessorCfg(name="processor2")
    datastream = Datastream(name="datastream1")

    tree.chain(sensor, processor1, processor2, datastream)

    assert tree.sensor._dpnode_instance == sensor
    assert processor1 in [child._dpnode_instance for child in tree.sensor._dpnode_children.values()]
    assert processor2 in [child._dpnode_instance for child in tree._find_or_create_node(processor1)._dpnode_children.values()]
    assert datastream in [child._dpnode_instance for child in tree._find_or_create_node(processor2)._dpnode_children.values()]


def test_validate(sample_tree):
    """
    Test the validate() method.
    """
    tree, _, _, _, _ = sample_tree

    try:
        tree.validate()
    except ValueError:
        pytest.fail("validate() raised ValueError unexpectedly!")


def test_instantiate(sample_tree):
    """
    Test the instantiate() method.
    """
    tree, _, _, _, _ = sample_tree

    result = tree.instantiate()
    assert result == "Tree instantiated with configuration."


def test_get_sensors(sample_tree):
    """
    Test the get_sensors() method.
    """
    tree, sensor, _, _, _ = sample_tree

    sensors = tree.get_sensors()
    assert len(sensors) == 1
    assert sensors[0] == sensor


def test_get_processors(sample_tree):
    """
    Test the get_processors() method.
    """
    tree, _, processor1, processor2, _ = sample_tree

    processors = tree.get_processors()
    assert len(processors) == 2
    assert processor1 in processors
    assert processor2 in processors


def test_get_datastreams(sample_tree):
    """
    Test the get_datastreams() method.
    """
    tree, _, _, _, datastream = sample_tree

    datastreams = tree.get_datastreams()
    assert len(datastreams) == 1
    assert datastream in datastreams


def test_search(sample_tree):
    """
    Test the search() method.
    """
    tree, _, processor1, _, _ = sample_tree

    result = tree.search(name="processor1")
    assert result == processor1
