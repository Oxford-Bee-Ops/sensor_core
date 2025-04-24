from threading import Thread
from time import sleep
import logging

import pytest
from example.my_fleet_config import INVENTORY
from example.my_sensor_example import EXAMPLE_FILE_DS_TYPE_ID
from sensor_core import api, edge_orchestrator
from sensor_core import configuration as root_cfg
from sensor_core.edge_orchestrator import EdgeOrchestrator
from sensor_core.sensor_core import SensorCore
from sensor_core.utils import sc_test_emulator

logger = root_cfg.setup_logger("sensor_core", level=logging.DEBUG)


class Test_Orchestrator:
    @pytest.mark.quick
    def test_SensorCore_status(self) -> None:
        sc = SensorCore()
        sc.configure(INVENTORY)
        message = sc.status()
        logger.info(message)
        assert message is not None

    @pytest.mark.quick
    def test_Orchestrator(self) -> None:
        with sc_test_emulator.ScEmulator.get_instance() as th:
        
            # Standard flow
            # We reset cfg.my_device_id to override the computers mac_address
            # This is a test device defined in BeeOps.cfg to have a DummySensor.
            root_cfg.update_my_device_id("d01111111111")

            sc = SensorCore()
            sc.configure(INVENTORY)

            orchestrator = EdgeOrchestrator.get_instance()
            orchestrator.load_config()
            orchestrator.start_all()
            sleep(12)
            orchestrator.stop_all()
            sleep(2)
            # Check that we have data in the journals
            # SCORE & SCORP & DUMML & DUMMF should contain data.
            # DUMMD should be empty
            # The files will have been pushed to the cloud, so we need to get 
            # the modified data on each journal.
            th.assert_records("sensor-core-journals",
                            {"V3_DUMML*": 1, "V3_DUMMD*": 1})
            th.assert_records("sensor-core-upload",
                            {"V3_DUMMF*": 1})
            th.assert_records("sensor-core-system-records",
                            {"V3_SCORE*": 1, "V3_SCORP*": 1})
            th.assert_records("sensor-core-fair",
                            {"V3_DUMM*": 3})

            # Stop without start
            orchestrator.load_config()
            sleep(1)
            orchestrator.stop_all()

            # Repeat runs of observability logging
            logger.info("sensor_test: # Repeat runs of observability logging")
            orchestrator.load_config()
            orchestrator.start_all()
            orchestrator.stop_all()

            # There may be files left waiting to be processed, so we can't assert there aren't.
            # filelist: list[Path] = list(root_cfg.EDGE_PROCESSING_DIR.glob("*"))
            # filelist = [x for x in filelist if not x.suffix.endswith("csv")]
            # assert len(filelist) == 0

    def test_orchestrator_main(self) -> None:
        
        with sc_test_emulator.ScEmulator.get_instance():

            orchestrator = EdgeOrchestrator.get_instance()

            # Direct use of edge_orchestrator to include main() keep-alive
            logger.info("sensor_test: Direct use of EdgeOrchestor to include keep-alive")
            factory_thread = Thread(target=edge_orchestrator.main)
            factory_thread.start()
            start_clock = api.utc_now()
            while not orchestrator._orchestrator_is_running:
                sleep(1)
                assert (api.utc_now() - start_clock).total_seconds() < 10, (
                    "Orchestrator did not restart quickly enough")
            assert orchestrator._orchestrator_is_running

            # Sensor fails; factory_thread should restart everything after 1s
            logger.info("sensor_test: # Sensor fails; factory_thread should restart everything after 1s")
            sensor = orchestrator._get_sensor(EXAMPLE_FILE_DS_TYPE_ID, 1)
            assert sensor is not None
            orchestrator.sensor_failed(sensor)
            assert not orchestrator._orchestrator_is_running
            start_clock = api.utc_now()
            while not orchestrator._orchestrator_is_running:
                sleep(1)
                assert (api.utc_now() - start_clock).total_seconds() < 10, (
                    "Orchestrator did not restart quickly enough")
            orchestrator.stop_all()

            # Wait for the main thread to exit
            logger.info("sensor_test: # Stop the edge_orchestrator main loop")
            if factory_thread.is_alive():
                factory_thread.join()
