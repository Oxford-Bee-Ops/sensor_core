import datetime as dt
from threading import Thread
from time import sleep

import pytest
import yaml
from example import my_fleet_config
from example.my_config_object_defs import ExampleSensorCfg
from sensor_core import api, edge_orchestrator
from sensor_core import configuration as root_cfg
from sensor_core.cloud_connector import CloudConnector
from sensor_core.edge_orchestrator import EdgeOrchestrator
from sensor_core.sensor_core import SensorCore
from sensor_core.utils import file_naming

logger = root_cfg.setup_logger("sensor_core")
root_cfg.TEST_MODE = True


class Test_Orchestrator:
    @pytest.mark.quick
    def test_SensorCore_status(self) -> None:
        sc = SensorCore()
        sc.configure(my_fleet_config.INVENTORY)
        message = sc.status()
        logger.info(message)
        assert message is not None

    @pytest.mark.quick
    def test_Orchestrator(self) -> None:
        # Standard flow
        # We reset cfg.my_device_id to override the computers mac_address
        # This is a test device defined in BeeOps.cfg to have a DummySensor.
        root_cfg.update_my_device_id("d01111111111")

        sc = SensorCore()
        sc.configure(my_fleet_config.INVENTORY)

        orchestrator = EdgeOrchestrator.get_instance()
        orchestrator.load_sensors()
        orchestrator.start_all()
        sleep(2)
        orchestrator.observability_run()
        orchestrator.stop_all()
        # Check that we have data in the journals
        # SCORE & SCORP & DUMML & DUMMF should contain data.
        # DUMMD should be empty
        # The files will have been pushed to the cloud, so we need to get the modified data on each journal.
        stores = {
            "SCORE": orchestrator._score_ds.ds_config.cloud_container,
            "SCORP": orchestrator._scorp_ds.ds_config.cloud_container,
            "DUMML": root_cfg.my_device.cc_for_journals,
            "DUMMF": root_cfg.my_device.cc_for_journals,
            "DUMMD": root_cfg.my_device.cc_for_journals,
        }
        for ds_type_id, container_name in stores.items():
            fname = file_naming.get_cloud_journal_filename(ds_type_id, api.utc_now())
            modified_time = CloudConnector().get_blob_modified_time(str(container_name), fname.name)
            assert (api.utc_now() - modified_time).total_seconds() < 60, f"Journal not updated {fname}"

        # Check that a FAIR yaml file has been created in the last 10s
        files = CloudConnector().list_cloud_files(root_cfg.my_device.cc_for_fair, 
                                                  more_recent_than=api.utc_now() - dt.timedelta(seconds=60))
        assert files, f"No FAIR files found in {root_cfg.my_device.cc_for_fair}"

        # Check we can download the FAIR yaml and recreate the object
        tmp_fname = file_naming.get_temporary_filename("yaml")
        CloudConnector().download_from_container(root_cfg.my_device.cc_for_fair, 
                                                files[0],
                                                tmp_fname)
        with open(tmp_fname, "r") as f:
            fair_dict = yaml.safe_load(f)
        fair_device_id = fair_dict[api.RECORD_ID.DEVICE_ID.value]
        assert len(fair_device_id) == 12

        # Stop without start
        orchestrator.load_sensors()
        sleep(1)
        orchestrator.stop_all()

        # Direct use of edge_orchestrator to include main() keep-alive
        logger.info("sensor_test: Direct use of EdgeOrchestor to include keep-alive")
        factory_thread = Thread(target=edge_orchestrator.main)
        factory_thread.start()
        sleep(1)
        assert orchestrator._orchestrator_is_running

        # Sensor fails; factory_thread should restart everything after 1s
        logger.info("sensor_test: # Sensor fails; factory_thread should restart everything after 1s")
        sensor = orchestrator._get_sensor(ExampleSensorCfg.sensor_type, 1)
        assert sensor is not None
        orchestrator.sensor_failed(sensor)
        assert not orchestrator._orchestrator_is_running
        start_clock = api.utc_now()
        while not orchestrator._orchestrator_is_running:
            sleep(1)
            assert (api.utc_now() - start_clock).total_seconds() < 10, (
                "Orchestrator did not restart quickly enough")
        orchestrator.stop_all()

        # Stop the factory_thread
        logger.info("sensor_test: # Stop the edge_orchestrator main loop")
        edge_orchestrator.request_stop()
        if factory_thread.is_alive():
            factory_thread.join()

        # Repeat runs of observability logging
        logger.info("sensor_test: # Repeat runs of observability logging")
        orchestrator.load_sensors()
        orchestrator.start_all()
        sleep(2)
        orchestrator.observability_run()
        sleep(2)
        orchestrator.observability_run()
        #orchestrator.upload_to_cloud()
        orchestrator.stop_all()

        # There may be files left waiting to be processed, so we can't assert there aren't.
        # filelist: list[Path] = list(root_cfg.EDGE_PROCESSING_DIR.glob("*"))
        # filelist = [x for x in filelist if not x.suffix.endswith("csv")]
        # assert len(filelist) == 0
