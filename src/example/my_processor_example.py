from pathlib import Path
from typing import Optional

import pandas as pd
from sensor_core import DataProcessor, DpContext, DPengine
from sensor_core import configuration as root_cfg
from sensor_core.utils import file_naming

from example.my_config_object_defs import EXAMPLE_FILE_DS_TYPE_ID

logger = root_cfg.setup_logger("sensor_core")




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
        input_data: pd.DataFrame | list[Path]
    ) -> Optional[pd.DataFrame]:
        """This implementation of the process_data method is used in testing:
        - so has an excess number of asserts!
        - demonstrates a file DP converting a file list to a DataFrame
        - demonstrates a DF DP returning a DataFrame"""
        assert input_data is not None
        assert isinstance(input_data, list)

        logger.debug(f"process_data:{input_data} for {self.get_data_id()}")

        output_data: list[dict] = []    
        if len(input_data) > 0:
            for f in input_data:
                # Generate output to the primary datastream
                fields_dict = file_naming.parse_record_filename(f)
                fields_dict.update({"pixel_count": 25})
                output_data.append(fields_dict)

                # Generate data for the derived datastream
                self.save_data(fields_dict)
                derived_dss[0].log({"pixel_count_transformed": 25*25})

        return pd.DataFrame(output_data)