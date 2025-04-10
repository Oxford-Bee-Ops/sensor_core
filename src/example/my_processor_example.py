from pathlib import Path
from typing import Optional

import pandas as pd

from example.my_config_object_defs import EXAMPLE_DF_DS_TYPE_ID, EXAMPLE_FILE_DS_TYPE_ID
from sensor_core import DataProcessor, Datastream, DpContext
from sensor_core import configuration as root_cfg
from sensor_core.utils import file_naming, utils

logger = utils.setup_logger("sensor_core")




#############################################################################################################
# Define the DataProcessor for the ExampleSensor
#
# The DataProcessor is responsible for processing the data from the Datastream.
# It must implement the process_data() method.
#
# This data processor:
# - processes files into DataFrames (primary Datastream)
# - creates data that it records into a derived Datastream
#############################################################################################################
class ExampleProcessor(DataProcessor):
    def process_data(
        self, 
        datastream: Datastream, 
        input_data: pd.DataFrame | list[Path],
        context: DpContext
    ) -> Optional[pd.DataFrame]:
        """This implementation of the process_data method is used in testing:
        - so has an excess number of asserts!
        - demonstrates a file DP converting a file list to a DataFrame
        - demonstrates a DF DP returning a DataFrame"""
        assert datastream.ds_sensor_index == 1
        assert datastream.ds_device_id == root_cfg.my_device_id
        assert datastream.ds_config.ds_type_id == EXAMPLE_FILE_DS_TYPE_ID
        assert input_data is not None
        assert isinstance(input_data, list)

        logger.debug(f"process_data:{input_data} for {datastream.ds_config}")

        output_data: list[dict] = []    
        if len(input_data) > 0:
            for f in input_data:
                # Generate output to the primary datastream
                fields_dict = file_naming.parse_record_filename(f)
                fields_dict.update({"pixel_count": 25})
                output_data.append(fields_dict)

                # Generate data for the derived datastream
                derived_dss = self.get_derived_datastreams(EXAMPLE_DF_DS_TYPE_ID)
                assert len(derived_dss) == 1
                derived_dss[0].log({"pixel_count_transformed": 25*25})

        return pd.DataFrame(output_data)