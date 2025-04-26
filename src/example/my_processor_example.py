from pathlib import Path

import pandas as pd
from sensor_core import DataProcessor, api, file_naming
from sensor_core import configuration as root_cfg
from sensor_core.dp_config_object_defs import DataProcessorCfg, Stream

logger = root_cfg.setup_logger("sensor_core")

EXAMPLE_DF_DS_TYPE_ID = "DUMMD"
EXAMPLE_DF_STREAM_INDEX = 0
EXAMPLE_FILE_PROCESSOR_CFG = DataProcessorCfg(
    description="Example file processor for testing",
    outputs=[Stream(description="Example dataframe stream",
                    type_id=EXAMPLE_DF_DS_TYPE_ID, 
                    index=EXAMPLE_DF_STREAM_INDEX, 
                    format=api.FORMAT.DF, 
                    fields=["pixel_count"],
                    #sample_probability = str(1.0), # Always upload - we assert this in the UnitTest
                    #sample_container = "sensor-core-upload"
                    ),
            ],
)

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
    ) -> None:
        """This implementation of the process_data method is used in testing:
        - so has an excess number of asserts!
        - demonstrates a file DP converting a file list to a DataFrame
        - demonstrates a DF DP returning a DataFrame"""
        assert input_data is not None
        assert isinstance(input_data, list)

        logger.debug(f"process_data:{input_data} for {__name__}")

        output_data: list[dict] = []    
        if len(input_data) > 0:
            for f in input_data:
                # Generate output to the primary datastream
                fields_dict = file_naming.parse_record_filename(f)
                fields_dict.update({"pixel_count": 25})
                output_data.append(fields_dict)

        # Generate data for the derived datastream
        self.save_data(stream_index=EXAMPLE_DF_STREAM_INDEX,
                        sensor_data=pd.DataFrame(output_data))
                