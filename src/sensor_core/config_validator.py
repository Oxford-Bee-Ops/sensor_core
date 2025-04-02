####################################################################################################
# The config_validator is used to validate SensorCore configuration files (eg fleet_config.py).
####################################################################################################
import importlib
from abc import ABC, abstractmethod

from sensor_core.cloud_connector import CloudConnector
from sensor_core.config_objects import DeviceCfg


class ValidationRule(ABC):
    """ Base class for validation rules. Extend this class to implement specific rules. """
    @abstractmethod
    def validate(self, inventory: dict[str, DeviceCfg]) -> tuple[bool, str]:
        """
        Validate the configuration.

        Args:
            config (dict): The configuration to validate.

        Returns:
            tuple: (bool, str) where the boolean indicates success (True) or failure (False),
                   and the string contains an error message if validation fails.
        """
        raise NotImplementedError("Subclasses must implement the validate method.")

# Example of a specific validation rule
class Rule1_device_id(ValidationRule):
    def validate(self, inventory: dict[str, DeviceCfg]) -> tuple[bool, str]:
        for device_id, device in inventory.items():
            if device.device_id != device_id:
                return False, (
                    f"Device ID used as key ({device_id}) does not match device_id"
                    f" in DeviceCfg ({device.device_id})."
                    )
            if len(device_id) != 12:
                return (False, f"Device ID ({device_id}) must be 12 characters long.")
        return True, ""
    
class Rule2_not_none(ValidationRule):
    def validate(self, inventory: dict[str, DeviceCfg]) -> tuple[bool, str]:
        for device_id, device in inventory.items():
            if not device.sensor_ds_list:
                return False, (
                    f"sensor_ds_list missing for ({device_id})"
                    f" in DeviceCfg ({device.name})."
                    )
        return True, ""

class Rule3_validate_class_refs(ValidationRule):
    def validate(self, inventory: dict[str, DeviceCfg]) -> tuple[bool, str]:
        for device_id, device in inventory.items():
            for sensor_ds in device.sensor_ds_list:

                # Check the SensorCfg.sensor_class_ref
                sensor_class_ref = sensor_ds.sensor_cfg.sensor_class_ref
                if sensor_class_ref:
                    # try to resolve the class reference
                    succeeded = False
                    try:
                        module_path, class_name = sensor_class_ref.rsplit(".", 1)
                        module = importlib.import_module(module_path)
                        cls = getattr(module, class_name)
                        if cls is not None:
                            succeeded = True
                    finally:
                        if not succeeded:
                            return False, (
                                f"sensor_class_ref {sensor_class_ref} for {device_id} could not be resolved"
                                )
            
                # Check the dp_config.sensor_class_ref
                for ds in sensor_ds.datastream_cfgs:
                    if not ds.edge_processors:
                        continue
                    for dp in ds.edge_processors:
                        dp_class_ref = dp.dp_class_ref
                        if dp_class_ref:
                            # try to resolve the class reference
                            succeeded = False
                            try:
                                module_path, class_name = dp_class_ref.rsplit(".", 1)
                                module = importlib.import_module(module_path)
                                cls = getattr(module, class_name)
                                if cls is not None:
                                    succeeded = True
                            finally:
                                if not succeeded:
                                    return False, (
                                        f"dp_class_ref {dp_class_ref} for {device_id} "
                                        f"{ds.ds_type_id} could not be resolved"
                                        )
        return True, ""

# Rule4: check that cloud_container is set on all datastreams other than type CSV
class Rule4_cloud_container_specified(ValidationRule):
    def validate(self, inventory: dict[str, DeviceCfg]) -> tuple[bool, str]:
        for device_id, device in inventory.items():
            for sensor_ds in device.sensor_ds_list:
                for ds in sensor_ds.datastream_cfgs:
                    if ds.archived_format != "csv" and not ds.cloud_container:
                        return False, (
                            f"cloud_container not set for {device_id} {ds.ds_type_id}"
                            ) 
        return True, ""

# Rule5: check that all cloud_containers exist in the blobstore using cloud_connector.container_exists()
class Rule5_cloud_container_exists(ValidationRule):
    def validate(self, inventory: dict[str, DeviceCfg]) -> tuple[bool, str]:
        cc = CloudConnector()
        for device_id, device in inventory.items():

            if not cc.container_exists(device.cc_for_upload):
                return False, (
                    f"cc_for_upload {device.cc_for_upload} for {device_id} does not exist"
                    )
            if not cc.container_exists(device.cc_for_journals):
                return False, (
                    f"cc_for_journals {device.cc_for_journals} for {device_id} does not exist"
                    )
            if not cc.container_exists(device.cc_for_system_records):
                return False, (
                    f"cc_for_system_records {device.cc_for_system_records} for {device_id} does not exist"
                    )
            if not cc.container_exists(device.cc_for_fair):
                return False, (
                    f"cc_for_fair {device.cc_for_fair} for {device_id} does not exist"
                    )
            
            # Check the Datastream's cloud_container
            for sensor_ds in device.sensor_ds_list:
                for ds in sensor_ds.datastream_cfgs:
                    if ds.cloud_container and not cc.container_exists(ds.cloud_container):
                        return False, (
                                f"cloud_container {ds.cloud_container} specified in "
                                f"{ds.ds_type_id} does not exist"
                            )
                    if ds.sample_container and not cc.container_exists(ds.sample_container):
                        return False, (
                                f"sample_container {ds.sample_container} specified in "
                                f"{ds.ds_type_id} does not exist"
                            )
        return True, ""


RULE_SET: list[ValidationRule] = [
    Rule1_device_id(),
    Rule2_not_none(),
    Rule3_validate_class_refs(),
    Rule4_cloud_container_specified(),
    Rule5_cloud_container_exists()
]

def validate(inventory: dict[str, DeviceCfg]) -> tuple[bool, list[str]]:
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

