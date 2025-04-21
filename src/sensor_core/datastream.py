from dataclasses import dataclass
from sensor_core.dp_tree import DPtreeNode
from sensor_core.config_objects import DatastreamCfg
from sensor_core import configuration as root_cfg
from sensor_core.utils import file_naming

@dataclass
class Datastream(DPtreeNode):
    config: DatastreamCfg

    def __init__(self, config: DatastreamCfg) -> None:
        self.set_config(config)

    def get_data_id(self):
        return file_naming.create_data_id(root_cfg.my_device_id, self.config.type_id, self.config.sensor_id)
    
