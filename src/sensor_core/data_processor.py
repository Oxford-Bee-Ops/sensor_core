from __future__ import annotations

from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import pandas as pd

if TYPE_CHECKING:
    from sensor_core.datastream import Datastream
from sensor_core import configuration as root_cfg
from sensor_core.config_objects import DataProcessorCfg, DpContext

logger = root_cfg.setup_logger("sensor_core")


####################################################################################################
#
# Class: DataProcessor
#
#####################################################################################################
class DataProcessor:
    """DataProcessors are invoked by the Datastream to process data from a Sensor.

    The DataProcessor implements the process_data() function to process the Sensor data.
    DataProcessors are commonly chained together to process data in sequence.
    The chain is defined in the Datastream configuration in configuration.py as a list of
    DataProcessorConfig objects.

    DataProcessors can define 'derived' Datastreams to enable forking of the data pipeline by
    implementing the define_derived_datastreams.
    """

    @abstractmethod
    def process_data(
        self, 
        datastream: Datastream, 
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


    def get_derived_datastreams(self, ds_type_id: Optional[str] = None) -> list[Datastream]:
        """Return a list of derived Datastreams.
        This function is called by the DataProcessor subclass during process_data.

        Parameters
        ----------
        ds_id : str, optional
            The Datastream ID to return. If None, all derived Datastreams are returned.
        """
        if ds_type_id is None:
            return self._derived_datastreams  
        else:
            return [ds for ds in self._derived_datastreams if ds.ds_config.ds_type_id == ds_type_id]


    def _set_derived_datastreams(self, derived_datastreams: list[Datastream]) -> None:
        """Set the derived Datastreams.

        This function is called by the Datastream during initialisation.
        """
        self._derived_datastreams = derived_datastreams


    def _set_dp_config(self, dp_config: DataProcessorCfg, dp_index: int, is_last: bool) -> None:
        """Set the DataProcessor configuration.

        This function is called by the Datastream during initialisation.
        """
        self.dp_config = dp_config
        self.dp_index = dp_index
        self.is_last = is_last


    def _get_dp_config(self) -> tuple[DataProcessorCfg, int, bool]:
        """Return the DataProcessor configuration."""
        return (self.dp_config, self.dp_index, self.is_last)
