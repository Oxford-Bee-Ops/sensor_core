####################################################################################################
# The config_validator is used to validate SensorCore configuration files (eg fleet_config.py).
####################################################################################################
from abc import ABC, abstractmethod

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.cloud_connector import CloudConnector
from sensor_core.dp_tree import DPtree
from sensor_core.dp_tree_node import DPtreeNode
from sensor_core.sensor import Sensor, SensorCfg

logger = root_cfg.setup_logger("sensor_core")

class ValidationRule(ABC):
    """ Base class for validation rules. Extend this class to implement specific rules. """
    @abstractmethod
    def validate(self, dpnode: DPtreeNode) -> tuple[bool, str]:
        """
        Validate the configuration.

        Args:
            dpnode (DPtreeNode): The configuration to validate.

        Returns:
            tuple: (bool, str) where the boolean indicates success (True) or failure (False),
                   and the string contains an error message if validation fails.
        """
        raise NotImplementedError("Subclasses must implement the validate method.")

###########################################################################################################
# Start with the device-level validation rules.
###########################################################################################################

# Rule 1: check that the outputs list is not empty
class Rule1_outputs_not_empty(ValidationRule):
    def validate(self, dpnode: DPtreeNode) -> tuple[bool, str]:
        outputs: list = dpnode.get_config().outputs
        if outputs is None or len(outputs) == 0:
            return False, (
                f"Outputs list is empty in {dpnode}: "
                "all DPtree nodes must have at least one output.")
        return True, ""

# Rule 2: check that the sensor model is set for all datastreams
class Rule2_sensor_model_set(ValidationRule):
    def validate(self, dpnode: DPtreeNode) -> tuple[bool, str]:
        config = dpnode.get_config()
        if isinstance(config, SensorCfg):
            if config.sensor_model is None or config.sensor_model == root_cfg.FAILED_TO_LOAD:
                return False, (
                    f"Sensor model not set in {config.sensor_type} {config}"
                )
        return True, ""

# Rule 3: no _ in any stream type_id
class Rule3_no_underscore_in_type_id(ValidationRule):
    def validate(self, dpnode: DPtreeNode) -> tuple[bool, str]:
        outputs = dpnode.get_config().outputs
        if outputs:
            for stream in outputs:
                if "_" in stream.type_id:
                    return False, (
                        f"Underscore found in type_id {stream.type_id} in {dpnode}. "
                        f"type_id must not contain underscores."
                    )
        return True, ""

# Rule4: check that cloud_container is set on all datastreams other than type log / df / csv
class Rule4_cloud_container_specified(ValidationRule):
    def validate(self, dpnode: DPtreeNode) -> tuple[bool, str]:
        outputs = dpnode.get_config().outputs
        if outputs:
            for stream in outputs:
                if stream.format not in ["log", "df", "csv"]:
                    if (stream.cloud_container is None or 
                        len(stream.cloud_container) < 2 or
                        stream.cloud_container == "Not set"):
                        return False, (
                            f"cloud_container not set in {dpnode}"
                        )
        return True, ""

# Rule5: check that all cloud_containers exist in the blobstore using cloud_connector.container_exists()
class Rule5_cloud_container_exists(ValidationRule):
    def validate(self, dpnode: DPtreeNode) -> tuple[bool, str]:
        cc = CloudConnector.get_instance()
        outputs = dpnode.get_config().outputs
        if outputs:
            for stream in outputs:
                if stream.format not in ["log", "df", "csv"]:
                    # Check the Datastream's cloud_container exists
                    if (stream.cloud_container is not None and 
                        not cc.container_exists(stream.cloud_container)):
                        return False, (
                            f"cloud_container {stream.cloud_container} does not exist in "
                            f"{dpnode}"
                        )
        return True, ""

# Rule 6: any datastream of type log, csv or df must have output fields set
class Rule6_csv_output_fields(ValidationRule):
    def validate(self, dpnode: DPtreeNode) -> tuple[bool, str]:
        outputs = dpnode.get_config().outputs
        if outputs:
            for stream in outputs:
                if stream.format in ["log", "df", "csv"]:
                    if stream.fields is None or len(stream.fields) == 0:
                        return False, (
                            f"output fields not set in {dpnode} for {stream.type_id}"
                        )
        return True, ""

# Rule 7: don't declare field names that are in use for record_id fields
class Rule7_reserved_fieldnames(ValidationRule):
    def validate(self, dpnode: DPtreeNode) -> tuple[bool, str]:
        outputs = dpnode.get_config().outputs
        if outputs:
            for stream in outputs:
                fields = stream.fields
                if fields is not None:
                    for field in fields:
                        if field in api.ALL_RECORD_ID_FIELDS:
                            return False, (
                                f"output field {field} is reserved in {dpnode} for {stream.type_id}"
                            )
        return True, ""


RULE_SET: list[ValidationRule] = [
    Rule1_outputs_not_empty(),
    Rule2_sensor_model_set(),
    Rule4_cloud_container_specified(),
    Rule5_cloud_container_exists(),
    Rule6_csv_output_fields(),
    Rule7_reserved_fieldnames(),
]

def validate(dptree: DPtree) -> tuple[bool, list[str]]:
    """
    Validate the configuration using all added rules.

    Args:
        config (dict): The configuration to validate.

    Returns:
        tuple: (bool, list) where the boolean indicates overall success (True) or failure (False),
                and the list contains error messages for all failed rules.
    """
    is_valid = True
    errors = []

    if not dptree:
        return False, ["No tree provided for validation."]
    
    if not isinstance(dptree, DPtree):
        return False, ["dptree is not a DPtree."]
    
    for rule in RULE_SET:
        try:
            for dpnode in dptree._nodes.values():
                success, error_message = rule.validate(dpnode)
                if not success:
                    is_valid = False
                    errors.append(error_message)
        except Exception as e:
            is_valid = False
            errors.append(
                f"Error validating rule {rule.__class__.__name__}: {e!s}"
            )

    return is_valid, errors

