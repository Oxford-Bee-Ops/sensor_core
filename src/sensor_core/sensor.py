####################################################################################################
# Sensor classes
#  - EdgeOrchestrator: Manages the state of the sensor threads
#  - SensorConfig: Dataclass for sensor configuration, specified in sensor_cac.py
#  - Sensor: Super class for all sensor classes
####################################################################################################
import threading
from abc import ABC, abstractmethod
from random import random
from typing import Optional

from sensor_core import configuration as root_cfg
from sensor_core.config_objects import DatastreamCfg, SensorDsCfg
from sensor_core.datastream import Datastream
from sensor_core.utils import file_naming, utils

logger = utils.setup_logger("sensor_core")


#############################################################################################################
# Super class that implements a thread to read the sensor data
#
# sensor_id must be a unique identifier for the physical sensor.
# If it is already in use, __init__ will raise a ValueError.
#############################################################################################################
class Sensor(threading.Thread, ABC):
    def __init__(self, context: SensorDsCfg) -> None:
        """Initialise the Sensor superclass.

        Parameters:
        ----------
        sensor_index: int
            The index of the sensor in the list of sensors.
        sensor_config: SensorConfig
            The configuration for the sensor.
        """
        super().__init__()

        logger.info(f"Initialise sensor {self!r}")

        self.sds_config = context
        self._sensor_type = context.sensor_cfg.sensor_type
        self._sensor_index = context.sensor_cfg.sensor_index

        # A dictionary of Datastreams associated with this sensor, indexed by ds_type_id
        self._datastreams: dict[str, Datastream] = {}

        # We set the daemon status to true so that the thread continues to run in the background
        self.daemon = False
        self.stop_requested = False

    def start(self) -> None:
        """Start the sensor thread - this method must not be subclassed"""
        logger.info(f"Starting sensor thread {self!r}")
        super().start()

    def stop(self) -> None:
        """Stop the sensor thread - this method must not be subclassed"""
        logger.info(f"Stop sensor thread {self!r}")
        self.stop_requested = True

    def sensor_failed(self) -> None:
        """Called by a subclass when the Sensor fails and needs to be restarted.

        The Sensor superclass notifies the EdgeOrchestrator which will stop & restart all Sensors."""
        from sensor_core.edge_orchestrator import EdgeOrchestrator

        EdgeOrchestrator.get_instance().sensor_failed(self)

    def create_datastreams(self, ds_cfg_list: list[DatastreamCfg]) -> list[Datastream]:
        """Called by the EdgeOrchestrator to get the Datastreams associated with this sensor.
        
        This is recursive as we look for derived Datastreams on the DataProcessors."""

        datastreams: list[Datastream] = []

        for ds_config in ds_cfg_list:
            # We pass the save_sample() method to the Datastream so it can check whether to save a sample
            # We want to pass the save_sample() from the subclass so we can override it if necessary
            # The following works because Python uses method resolution order.
            datastream = Datastream(
                datastream_config=ds_config,
                device_id=root_cfg.my_device_id,
                sensor_index=self._sensor_index,
                sensor_config=self.sds_config.sensor_cfg,
                save_sample_callback=self.save_sample,
            )
            datastreams.append(datastream)

            # For each datastream, see if it has derived datastreams
            if root_cfg.get_mode() == root_cfg.Mode.EDGE:
                processors = datastream._edge_dps
            else:
                processors = datastream._cloud_dps

            for dp in processors:
                if dp.dp_config.derived_datastreams:
                    derived_datastreams = self.create_datastreams(dp.dp_config.derived_datastreams)
                    # Set the datastreams on the DP object so it has access when processing data
                    dp._set_derived_datastreams(derived_datastreams)
                    # Add the datastreams to the list for returning to the orchestrator
                    datastreams.extend(derived_datastreams)

        # We keep a dictionary of Datastreams indexed by ds_id so we can easily find them later
        # when requested by a Sensor or DataProcessor
        for ds in datastreams:
            self._datastreams[ds.ds_id] = ds

        return datastreams

    # We expect the Sensor subclass to call this from the run() method
    def get_datastreams(self, ds_type_id: Optional[str] = None) -> dict[str, Datastream]:
        """Called by the Sensor subclass to get the Datastreams associated with this sensor.
        The dictionary is indexed by the datastream's ds_id.

        Parameters:
        ----------
        ds_type_id: Optional str
            The ds_type_id of the Datastream(s) to return. If None, return all Datastreams.
        """

        if ds_type_id is not None:
            return {
                ds_type_idx: ds
                for ds_type_idx, ds in self._datastreams.items()
                if ds.ds_config.ds_type_id == ds_type_id
            }
        else:
            return self._datastreams

    # We expect the Sensor subclass to call this from the run() method
    def get_datastream(self, 
                       ds_type_id: Optional[str]=None, 
                       sensor_index: Optional[int]=None,
                       format: Optional[str]=None) -> Optional[Datastream]:
        """Called by the Sensor subclass to get a specific Datastream associated with this sensor.
        All parameters are treated as optional parts of a filter.  
        Only datastreams that match all parts of the filter will be returned.

        Parameters:
        ----------
        ds_type_id: str
            The ds_type_id of the Datastream to return.
        sensor_index: int
            The sensor_index of the Datastream to return.
        format: Optional str
            The format of the Datastream to return.
        """
        # Filter by each non-None parameter in turn
        # This made more difficult by the fact that the sensor_index is not explicitly available
        # in the Datastream but it is part of the ds_id

        if ds_type_id is not None:
            # If we have both ds_type_id and sensor_index, we can filter by ds_id
            if sensor_index is not None:
                ds_id = file_naming.create_ds_id(
                        root_cfg.my_device_id, ds_type_id, sensor_index)
                if ds_id in self._datastreams:
                    return self._datastreams[ds_id]
                else:
                    return None
            else:
                # If we have ds_type_id but not sensor_index, we can filter by ds_type_id
                matching_datastreams: list[Datastream] = []
                matching_datastreams = [
                    ds
                    for ds in self._datastreams.values()
                    if ds.ds_config.ds_type_id == ds_type_id
                ]
                # Now check raw_format
                if format is not None:
                    matching_datastreams = [
                        ds
                        for ds in matching_datastreams
                        if ds.ds_config.raw_format == format
                    ]

                if len(matching_datastreams) == 0:
                    return None
                elif len(matching_datastreams) == 1:
                    return matching_datastreams[0]
                else:
                    logger.error(
                        f"{utils.RAISE_WARN()}get_datastream() found multiple Datastreams for ds_type_id="
                        f"{ds_type_id}, sensor_index={sensor_index}, format={format}"
                    )
                    return None

        # If we have the sensor_index but not ds_type_id, we have to walk the list to check sensor_index
        elif sensor_index is not None:
            for ds_id, ds in self._datastreams.items():
                would_be_ds_id = file_naming.create_ds_id(
                    root_cfg.my_device_id, ds.ds_config.ds_type_id, sensor_index)
                if ds_id == would_be_ds_id:
                    if (format is not None) and (ds.ds_config.raw_format != format):
                        continue
                    return ds
                
        elif format is not None:
            # If we have format but not ds_type_id or sensor_index, we can filter by format
            matching_datastreams = [
                ds
                for ds in self._datastreams.values()
                if ds.ds_config.raw_format == format
            ]
            if len(matching_datastreams) == 0:
                return None
            elif len(matching_datastreams) == 1:
                return matching_datastreams[0]
            else:
                logger.error(
                    f"{utils.RAISE_WARN()}get_datastream() found multiple Datastreams for format={format}"
                )
                return None

        return None

    # All Sensor sub-classes must implement this method
    # Implementations should respect the stop_requested flag and terminate within a reasonable time (~3min)
    @abstractmethod
    def run(self) -> None:
        """The run method is where the sensor does its work of sensing and logging data"""
        assert False, "Sub-classes must override this method"

    # This is typically used when a sensor sub-class calls save_recording()
    def save_sample(self, ds_type_id: str) -> bool:
        """Function to check whether this recording should be save as a sample.
        This is a default implementation that can be over-ridden if more complex function is required.

        This implementation checks the record_sample_probability in the sensor configuration and
        returns True if the random number generated is less than the probability.
        """
        # Get the configuration for the Datastream
        ds_config = self.sds_config.get_datastream_cfg(ds_type_id)

        # If there are no Edge DataProcessors, we don't need to save a sample because we're saving everything
        if not ds_config.edge_processors:
            return False

        # The sample_probability is a string that can be evaluated to a float
        sample_probability: float = 0.0
        if ds_config.sample_probability is not None:
            try:
                sample_probability = float(ds_config.sample_probability)
            except ValueError:
                logger.error(
                    f"{utils.RAISE_WARN()}Invalid sample_probability in Datastream config: "
                    f"{ds_config.sample_probability}"
                )
                return False

        if (sample_probability > 0) and (ds_config.sample_container is None):
            logger.error(
                f"{utils.RAISE_WARN()}Datastream {ds_type_id} has sample_probability "
                f"but no sample_container defined"
            )
            return False

        return random() < sample_probability
