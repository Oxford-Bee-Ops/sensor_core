# Class to optionally manage:
# - Wifi
# - LED indicator status
# - auto-update the user software
# - auto-update the OS to keep current with security fixes
import os
import time
from typing import Any

from gpiozero import LED

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.utils import utils

logger = utils.setup_logger("sensor_core")


class DeviceManager:
    """Manages LED & Wifi status if configured to do so in my_device (DeviceCfg):
     - manage_wifi: bool = True
     - manage_leds: bool = True
    """
    # Device states
    S_BOOTING = "Booting"
    S_WIFI_UP = "Wifi Up"
    S_INTERNET_UP = "Internet Up"
    S_WIFI_FAILED = "Wifi failed"
    S_AP_DOWN = "AP Down"
    S_AP_UP = "AP Up"
    S_AP_IN_USE = "AP In Use"

    # LED GPIO pins
    GPIO_RED = 26
    GPIO_GREEN = 16

    def __init__(self) -> None:
        if root_cfg.system_cfg is None:
            logger.error("DeviceManager: system_cfg is None; exiting")
            return
        ###############################
        # Wifi management
        ###############################
        if root_cfg.my_device.wifi_clients:
            self.inject_wifi_clients()
        self.ping_failure_count_all = 0
        self.ping_success_count_all = 0
        self.ping_failure_count_run = 0
        self.ping_success_count_run = 0
        self.last_ping_was_ok = False
        self.log_counter = 0
        self.log_frequency = 60 * 10
        self.client_wlan = "wlan0"
        self.use_cloned_mac = False

        # Start wifi management thread
        if root_cfg.running_on_rpi and root_cfg.my_device.attempt_wifi_recovery:
            self.delete_and_reconfigure_client_wifi()
            self.wifi_timer = utils.RepeatTimer(interval=2.0, 
                                                function=self.wifi_timer_callback)
            self.wifi_timer.start()
            logger.info("Wifi timer started")

        ###############################
        # LED status management
        ###############################
        self.currentState = self.S_BOOTING
        self.currentAPState = self.S_AP_DOWN
        self.lastStateChangeTime = api.utc_now()
        self.led_flash_counter = 0
        self.red_led = False
        self.green_led = False
        if root_cfg.system_cfg.install_type == api.INSTALL_TYPE_SENSOR:
            self.red_led_obj: LED = None
            self.green_led_obj: LED = None
            # Start the LED management thread
            if root_cfg.running_on_rpi and root_cfg.my_device.manage_leds:
                self.set_led_objects()
                self.led_timer = utils.RepeatTimer(interval=1, 
                                                   function=self.led_timer_callback)
                self.led_timer.start()

        return

    #############################################################################################################
    # LED management functions
    #
    # LED stat FSM table
    #
    # State:            |Booting	 |Wifi up	    |Internet up	|Wifi failed
    # LED  :            |Red*        |Green*	    |Green	        |Red
    # Input[WifiUp]	    |Wifi up	 |-	            |-              |Wifi up
    # Input[GoodPing]   |-	         |Internet up	|-	            |-
    # Input[PingFail] 	|-	         |-     	    |Wifi up	    |-
    # Input[WifiDown]	|-	         |Wifi failed	|Wifi failed
    # *=blinking
    #
    #############################################################################################################
    # Set the LED objects
    # This can fail, so we put it here to make it easy to call and handle the exceptions
    def set_led_objects(self) -> bool:
        try:
            if self.red_led_obj is None:
                self.red_led_obj = LED(DeviceManager.GPIO_RED)
                self.red_led_obj.on()
            if self.green_led_obj is None:
                self.green_led_obj = LED(DeviceManager.GPIO_GREEN)
                self.green_led_obj.off()
            return True
        except Exception as e:
            logger.warning("Failed to set LED objects: " + str(e))
            return False

    # This function gets called every second.
    # Set the LEDs to ON or OFF as appropriate given the current device state.
    def led_timer_callback(self, args: Any) -> None:
        # We run in a try block because we don't an "LED busy" issue to permanently kill the LED loop
        try:
            logger.debug("LED timer callback")
            if self.red_led_obj is None or self.green_led_obj is None:
                if not self.set_led_objects():
                    # Failed to set the LED objects, so exit the callback
                    return
            if self.currentState == self.S_BOOTING:
                # Green should be off; red should be blinking
                self.green_led = False
                self.green_led_obj.off()
                if self.red_led is True:
                    self.red_led = False
                    self.red_led_obj.off()
                else:
                    self.red_led = True
                    self.red_led_obj.on()
            elif self.currentState == self.S_WIFI_UP:
                # Green should be blinking; red should be off
                self.red_led = False
                self.red_led_obj.off()
                if self.green_led is True:
                    self.green_led = False
                    self.green_led_obj.off()
                else:
                    self.green_led = True
                    self.green_led_obj.on()
            elif self.currentState == self.S_INTERNET_UP:
                # Green should be on; red should be off
                self.green_led = True
                self.green_led_obj.on()
                self.red_led = False
                self.red_led_obj.off()
                # If the AP is up, then blink the red LED for 100ms
                self.led_flash_counter += 1
                if (self.currentAPState == self.S_AP_UP) and (self.led_flash_counter % 5 == 0):
                    self.red_led_obj.on()
                    time.sleep(0.05)
                    self.red_led_obj.off()
            elif self.currentState == self.S_WIFI_FAILED:
                # Green should be off; red should be on
                self.green_led = False
                self.green_led_obj.off()
                self.red_led = True
                self.red_led_obj.on()
        except Exception as e:
            logger.error("LED timer callback threw an exception: " + str(e), exc_info=True)

    #############################################################################################################
    # Wifi management functions
    ##############################################################################################################
    def inject_wifi_clients(self) -> None:
        # Inject the wifi clients via nmcli
        # This is done so that the device has out-of-the-box awareness of the wifi clients
        # We use the nmcli command to get the list of wifi clients
        # We copy the config because we may modify it and want to be able to revert
        self.wifi_clients = root_cfg.my_device.wifi_clients.copy()
        if self.wifi_clients is None:
            logger.info("No wifi clients in the device configuration")
            return
        
        # Inject the wifi clients
        for client in self.wifi_clients:
            if (client.ssid is None or 
                client.priority is None or 
                client.pw is None or 
                client.ssid == "" or 
                client.priority == "" or 
                client.pw == ""):
                logger.warning(f"Skipping invalid wifi client: {client}")
                continue

            # Use nmcli to configure the client wifi connection if it doesn't already exist
            existing_connections = (
                utils.run_cmd("sudo nmcli -t -f NAME connection show").split("\n")
            )

            # Configure the wifi client
            if client.ssid not in existing_connections:
                logger.info(f"Adding client wifi connection {client.ssid} on {self.client_wlan}")
                utils.run_cmd(
                    f"sudo nmcli connection add con-name {client.ssid} "
                    f"ifname {self.client_wlan} type wifi wifi.mode infrastructure wifi.ssid {client.ssid} "
                    f"wifi-sec.key-mgmt wpa-psk wifi-sec.psk {client.pw} "
                    f"connection.autoconnect-priority {client.priority} "
                    f"ipv4.dns '8.8.8.8 8.8.8.4'"
                )        

    def set_wifi_status(self, wifi_up: bool) -> None:
        if wifi_up:
            # We only check wifi status if ping has failed
            if self.currentState != self.S_WIFI_UP:
                self.currentState = self.S_WIFI_UP
                self.set_last_state_change_time()
        else:
            # Wifi failed
            if self.currentState != self.S_WIFI_FAILED:
                self.currentState = self.S_WIFI_FAILED
                self.set_last_state_change_time()

    def set_ap_status(self, device_status: str) -> None:
        self.currentAPState = device_status

    def set_ping_status(self, ping_successful: bool) -> None:
        if ping_successful:
            # We have good connectivity to the internet
            if self.currentState != self.S_INTERNET_UP:
                self.currentState = self.S_INTERNET_UP
                self.set_last_state_change_time()
        else:
            # Ping failed, but wifi might be up
            self.set_ap_status(DeviceManager.S_AP_DOWN)
            if self.currentState == self.S_INTERNET_UP:
                self.currentState = self.S_WIFI_UP
                self.set_last_state_change_time()

    def set_last_state_change_time(self) -> None:
        self.lastStateChangeTime = api.utc_now()

    def get_time_since_last_state_change(self) -> float:
        currentTime = api.utc_now()
        return (currentTime - self.lastStateChangeTime).total_seconds()

    # Configure the client wifi connection
    def delete_and_reconfigure_client_wifi(self) -> None:
        """ We only delete and reconfigure if we suspect something is wrong with the existing config
        This may be the case if we change the client_wlan or the mac_address. """

        logger.info("Deleting all wifi connections")
        list_of_connections_str = utils.run_cmd("sudo nmcli -t -f NAME connection show")
        # remove the lo connection (local loopback) from the list and convert to a list
        list_of_connections = list_of_connections_str.replace("lo", "").split("\n")
        for connection in list_of_connections:
            if connection != "":
                utils.run_cmd(
                    'sudo nmcli connection delete "' + connection + '"',
                    ignore_errors=True,
                )
                logger.warning("Deleted wifi connection " + connection)

        # Loop through the wifi_clients list and configure the client wifi connections
        # NetworkManager will automatically connect to the wifi with the highest priority and strongest signal
        self.inject_wifi_clients()


    # Create a function for logging useful info
    def log_wifi_info(self) -> None:
        try:
            logger.info(utils.run_cmd("sudo iwgetid -r", ignore_errors=True))
            logger.info(utils.run_cmd("sudo ifconfig " + self.client_wlan, ignore_errors=True))
            logger.info(
                utils.run_cmd(
                    "sudo iwconfig " + self.client_wlan,
                    grep_strs=["Link", "Quality"],
                    ignore_errors=True,
                )
            )
            logger.info(
                utils.run_cmd(
                    "sudo nmcli device wifi list ifname " + self.client_wlan,
                    grep_strs=["Infra"],
                    ignore_errors=True,
                )
            )
            logger.info(utils.run_cmd("sudo arp -n", ignore_errors=True))
        except Exception as e:
            # grep did not match any lines
            logger.error("log_wifi_info threw an exception: " + str(e))

    # Function to manage the AP wifi connection
    # We only enable the AP wifi connection if the client wifi connection is UP
    def wifi_timer_callback(self) -> None:
        # Test that internet connectivity is UP and working by pinging google DNS servers
        # -c 1 means ping once, -W 1 means timeout after 1 second
        ping_rc = os.system("ping -c 1 -W 1 8.8.8.8 1>/dev/null")
        if ping_rc != 0:
            # Track ping stats for logging purposes
            self.ping_failure_count_all += 1
            if self.last_ping_was_ok:
                self.ping_success_count_run = 0
                self.ping_failure_count_run = 0
            self.ping_failure_count_run += 1
            self.last_ping_was_ok = False
            logger.info(
                "Ping failure count run: %s, all (good/bad): %s/%s",
                str(self.ping_failure_count_run),
                str(self.ping_success_count_all),
                str(self.ping_failure_count_all),
            )

            # Set ping status so that the LEDs reflect this change
            self.set_ping_status(False)

            # Only check Wifi status if ping fails
            ESSID = utils.run_cmd("sudo iwgetid -r", ignore_errors=True)
            if len(ESSID) > 3:
                self.set_wifi_status(True)
                logger.info("Wifi is up: " + ESSID)
            else:
                self.set_wifi_status(False)
                logger.info("Not connected to a wireless access point")

            #############################################
            # Recovery actions
            #
            # We use modulo 600 to retry recovery actions every 10 minutes
            # If ping is failing, this can be ~2s per iteration, so 20 minutes
            #
            # Since recovery involves reconfiguring wifi clients, we only do this if there are clients
            # specified in the configuration.
            #############################################
            retry_timer = 600
            if root_cfg.my_device.attempt_wifi_recovery and (len(root_cfg.my_device.wifi_clients) > 0):

                # If the failure count gets to 4 hours then reboot the device
                # Ping cycle is 2s, so 60*60*2 = 4 hours
                if self.ping_failure_count_run == (60 * 60 * 2):
                    logger.error(utils.RAISE_WARN() + "Rebooting device due to lack of internet for >4 hours")
                    utils.run_cmd("sudo reboot")

                # If we're connected to wifi but the internet is down, then try a different SSID
                elif ((self.currentState == self.S_WIFI_UP) and 
                        (self.ping_failure_count_run % retry_timer == 30)):
                    logger.info("Disconnecting from current SSID %s to try another one", ESSID)
                    # We keep the client_wlan up, but we delete the current connection - it will get re-added
                    # when we reconfigure the client wifi connections below if we continue to fail pings
                    assert ESSID != "", "ESSID is empty despite getting it above with iwgetid -r"
                    utils.run_cmd("sudo nmcli conn delete " + ESSID, ignore_errors=True)
                    # The next highest priority connection should take over at this point
                    time.sleep(2)
                    # Re-add the connection but at lower priority
                    new_priority = -1
                    for client in self.wifi_clients:
                        if client.ssid == ESSID:
                            client.priority -= 10
                            new_priority = client.priority
                            break
                    # Now re-add the connection with the new lower priority
                    # The existing connections won't be impacted because the code checks whether they already 
                    # exist
                    # If we've decremented the priority to -100 (lowest valid value); we reset all info
                    if new_priority > -100:
                        logger.warning(
                            "Lowered priority of wifi connection " + ESSID + " to " + str(new_priority)
                        )
                        self.delete_and_reconfigure_client_wifi()
                    else:
                        logger.warning(
                            "Lowered priority of wifi connection " + ESSID + " to -100; reconfiguring all"
                        )
                        self.wifi_clients = root_cfg.my_device.wifi_clients
                        self.delete_and_reconfigure_client_wifi()

                # If we've failed to get internet for a while, try reconfiguring the client wifi connections
                elif self.ping_failure_count_run % retry_timer == 60:
                    logger.info("Reconfiguring client wifi connections")
                    self.delete_and_reconfigure_client_wifi()

                # Try restarting the client wifi interface
                elif self.ping_failure_count_run % retry_timer == (60 * 2):
                    logger.info("Restarting client wifi interface")
                    utils.run_cmd("sudo nmcli dev disconnect " + self.client_wlan, ignore_errors=True)
                    utils.run_cmd("sudo nmcli dev connect " + self.client_wlan, ignore_errors=True)

                #############################################
                # End of recovery actions
                #############################################

        # Ping was successful
        else:
            # Track ping stats for logging purposes
            self.ping_success_count_all += 1
            if not self.last_ping_was_ok:
                self.ping_failure_count_run = 0
                self.ping_success_count_run = 0
            self.ping_success_count_run += 1
            if self.ping_success_count_run % 30 == 0:
                logger.info(
                    "Ping successful count run: %s, all (good/bad): %s/%s",
                    str(self.ping_success_count_run),
                    str(self.ping_success_count_all),
                    str(self.ping_failure_count_all),
                )

            # Set ping status so that the LEDs reflect this change
            self.set_ping_status(True)

            self.last_ping_was_ok = True

        # Log useful info and status periodically
        self.log_counter += 1
        if self.log_counter % self.log_frequency == 0:
            self.log_wifi_info()



# Main loop called from crontab on boot up
if __name__ == "__main__":
    # Check if we're running on a Raspberry Pi and if the device_manager is already running
    if not root_cfg.running_on_rpi:
        print("Not running on a Raspberry Pi; exiting")
        exit(0)
    if utils.is_already_running("device_manager"):
        print("Device manager is already running; exiting")
        exit(0)

    device_manager = DeviceManager()
    while True:
        # Sleep for 1 seconds
        time.sleep(1)
        pass
