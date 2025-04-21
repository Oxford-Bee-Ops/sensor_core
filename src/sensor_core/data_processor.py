from __future__ import annotations

from abc import abstractmethod, ABC
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Callable, Union

import pandas as pd

if TYPE_CHECKING:
    from sensor_core.dp_engine import DPengine
from sensor_core import configuration as root_cfg
from sensor_core.config_objects import DataProcessorCfg, DpContext
from sensor_core.dp_tree import DPtreeNode
from sensor_core.utils import file_naming

logger = root_cfg.setup_logger("sensor_core")


####################################################################################################
#
# Class: DataProcessor
#
#####################################################################################################
class DataProcessor(DPtreeNode, ABC):
    """DataProcessors are invoked by the Datastream to process data from a Sensor.

    The DataProcessor implements the process_data() function to process the Sensor data.
    DataProcessors are commonly chained together to process data in sequence.
    The chain is defined in the Datastream configuration in configuration.py as a list of
    DataProcessorConfig objects.

    DataProcessors can define 'derived' Datastreams to enable forking of the data pipeline by
    implementing the define_derived_datastreams.
    """
    def __init__(self, 
                 config: DataProcessorCfg,         
    ) -> None:
        self.set_config(config)

    def get_data_id(self):
        return file_naming.create_data_id(root_cfg.my_device_id, 
                                        self.config.type_id, 
                                        self.config.sensor_id, 
                                        self.config.node_index)

    @abstractmethod
    def process_data(
        self, 
        datastream: DPengine, 
        input_data: pd.DataFrame | list[Path],
        context: DpContext
    ) -> Optional[pd.DataFrame]:
        """This function processes data as described in the Datastream.

        In simple chaining, the DataProcessor is provided with an input_data DataFrame and returns an output
        DataFrame that will be passed to the next DataProcessor defined in the chain, or archived if this is
        the last DP.

        DPs on File-type Datastreams may be passed lists of files as input.
        DPs on File-type Datastreams may also save processed recordings (using ds.save_sub_recordings()) 
        rather than return a DataFrame.

        A DP may also save data via a derived Datastream if previously registered 
        (via define_derived_datastreams).

        Every row in a DataFrame returned by this method must contain the bapi.RECORD_ID fields.
        If input_data was a DataFrame, these fields will be present.
        if input_data was a list of files, the DP can use Datastream.parse_filename(f) to get a dict
        with the required fields (as keys) and values.

        All DataProcessors must subclass this method.
        """

        assert False, "DataProcessor subclass must implement process_data()"
