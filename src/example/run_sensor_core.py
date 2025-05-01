from time import sleep

from sensor_core import SensorCore
from sensor_core import configuration as root_cfg

from example import my_fleet_config

logger = root_cfg.setup_logger("sensor_core")

def run_sensor_core():
    """Run SensorCore as defined in the system.cfg and fleet_config.py files."""

    # !!! For this test example, we over-ride the computer's mac_address 
    # so that we always find a known device in the config.
    # You should delete this update_my_device_id line once you have a working config.
    SensorCore.update_my_device_id("d01111111111")

    try:
        # Configure the SensorCore with the fleet configuration
        # This will load the configuration and check for errors
        logger.info("SensorCore starting...")
        sc = SensorCore()

        logger.info("Configuring SensorCore.")
        sc.configure(my_fleet_config.INVENTORY)

        # If you want the sensor to restart automatically after reboot then uncomment the line below.
        # You will need to have installed a virtual environment and specified its location in system.cfg.
        # Default location is $HOME/venv.  This won't work on Windows.
        #
        #sc.make_my_script_persistent(__file__)

        # Start the SensorCore and begin data collection
        logger.info("Starting SensorCore...")
        sc.start()
        while True:
            logger.info(sc.status())
            sleep(180)

    except KeyboardInterrupt:
        logger.error("Keyboard interrupt => stopping SensorCore... this may take up to 180s.")
        sc.stop()
    except Exception as e:
        logger.error(f"{root_cfg.RAISE_WARN()}Error: {e}")
        sc.stop()


if __name__ == "__main__":
    run_sensor_core()