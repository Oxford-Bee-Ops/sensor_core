####################################################################################################
# The config_validator is used to validate SensorCore configuration files (eg fleet_config.py).
####################################################################################################
import importlib
from abc import ABC, abstractmethod

from sensor_core import api
from sensor_core.cloud_connector import CloudConnector
from sensor_core.config_objects import DeviceCfg
from sensor_core.dp_tree import DPtree
from sensor_core.dp_tree_node import DPtreeNode
from sensor_core.dp_tree_node_types import Stream


class ValidationRule(ABC):
    """ Base class for validation rules. Extend this class to implement specific rules. """
    @abstractmethod
    def validate(self, inventory: list[DeviceCfg]) -> tuple[bool, str]:
        """
        Validate the configuration.

        Args:
            config (dict): The configuration to validate.

        Returns:
            tuple: (bool, str) where the boolean indicates success (True) or failure (False),
                   and the string contains an error message if validation fails.
        """
        raise NotImplementedError("Subclasses must implement the validate method.")

###########################################################################################################
# Start with the device-level validation rules.
###########################################################################################################
class Rule1_device_id(ValidationRule):
    def validate(self, inventory: list[DeviceCfg]) -> tuple[bool, str]:
        for device in inventory:
            if not device.device_id:
                return (False, f"Device ID missing for ({device}).")
            if len(device.device_id) != 12:
                return (False, f"Device ID ({device.device_id}) must be 12 characters long.")
        return True, ""
    
class Rule2_not_none(ValidationRule):
    def validate(self, inventory: list[DeviceCfg]) -> tuple[bool, str]:
        for device in inventory:
            if not device.dp_trees:
                return False, (
                    f"sensor_ds_list missing for ({device.device_id})"
                    f" in DeviceCfg ({device.name})."
                    )
        return True, ""

# Rule4: check that cloud_container is set on all datastreams other than type CSV
class Rule4_cloud_container_specified(ValidationRule):
    def validate(self, inventory: list[DeviceCfg]) -> tuple[bool, str]:
        for device in inventory:
            dptree: DPtree
            for dptree in device.dp_trees:
                pass
        return True, ""

# Rule5: check that all cloud_containers exist in the blobstore using cloud_connector.container_exists()
class Rule5_cloud_container_exists(ValidationRule):
    def validate(self, inventory: list[DeviceCfg]) -> tuple[bool, str]:
        cc = CloudConnector.get_instance()
        for device in inventory:

            if not cc.container_exists(device.cc_for_upload):
                return False, (
                    f"cc_for_upload {device.cc_for_upload} for {device.device_id} does not exist"
                    )
            if not cc.container_exists(device.cc_for_journals):
                return False, (
                    f"cc_for_journals {device.cc_for_journals} for {device.device_id} does not exist"
                    )
            if not cc.container_exists(device.cc_for_system_records):
                return False, (
                    f"cc_for_system_records {device.cc_for_system_records} for "
                    f"{device.device_id} does not exist"
                    )
            if not cc.container_exists(device.cc_for_fair):
                return False, (
                    f"cc_for_fair {device.cc_for_fair} for {device.device_id} does not exist"
                    )
            
            # Check the Datastream's cloud_container
            # @@@
        return True, ""

# Rule 6: any datastream of type CSV or DF must have output_fields set
class Rule6_csv_output_fields(ValidationRule):
    def validate(self, inventory: list[DeviceCfg]) -> tuple[bool, str]:
        return True, ""

# Rule 7: don't declare field names that are in use for record_id fields
class Rule7_reserved_fieldnames(ValidationRule):
    def validate(self, inventory: list[DeviceCfg]) -> tuple[bool, str]:
        
        return True, ""


RULE_SET: list[ValidationRule] = [
    Rule1_device_id(),
    Rule2_not_none(),
    Rule4_cloud_container_specified(),
    Rule5_cloud_container_exists(),
    Rule6_csv_output_fields(),
    Rule7_reserved_fieldnames(),
]

def validate(inventory: list[DeviceCfg]) -> tuple[bool, list[str]]:
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

    if not inventory:
        return False, ["No items in the inventory provided; empty list."]
    
    if not isinstance(inventory, list):
        return False, ["Inventory is not a list."]
    
    if not all(isinstance(device, DeviceCfg) for device in inventory):
        return False, ["Inventory contains non-DeviceCfg objects."]

    for rule in RULE_SET:
        try:
            success, error_message = rule.validate(inventory)
            if not success:
                is_valid = False
                errors.append(error_message)
        except Exception as e:
            is_valid = False
            errors.append(
                f"Error validating rule {rule.__class__.__name__}: {e!s}"
            )

    return is_valid, errors

