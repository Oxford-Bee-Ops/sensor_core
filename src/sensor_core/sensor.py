####################################################################################################
# Sensor classes
#  - EdgeOrchestrator: Manages the state of the sensor threads
#  - SensorConfig: Dataclass for sensor configuration, specified in sensor_cac.py
#  - Sensor: Super class for all sensor classes
####################################################################################################
import threading
from abc import ABC, abstractmethod

from sensor_core import configuration as root_cfg
from sensor_core.dp_config_objects import SensorCfg
from sensor_core.dp_node import DPnode

logger = root_cfg.setup_logger("sensor_core")


#############################################################################################################
# Super class that implements a thread to read the sensor data
#############################################################################################################
class Sensor(threading.Thread, DPnode, ABC):
    
    def __init__(self, config: SensorCfg) -> None:
        """Initialise the Sensor superclass.

        Parameters:
        ----------
        sensor_index: int
            The index of the sensor in the list of sensors.
        sensor_config: SensorConfig
            The configuration for the sensor.
        """
        threading.Thread.__init__(self)
        DPnode.__init__(self, config, config.sensor_index)

        logger.info(f"Initialise sensor {self!r}")

        self.config = config

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
 
    # All Sensor sub-classes must implement this method
    # Implementations should respect the stop_requested flag and terminate within a reasonable time (~3min)
    @abstractmethod
    def run(self) -> None:
        """The run method is where the sensor does its work of sensing and logging data"""
        assert False, "Sub-classes must override this method"
