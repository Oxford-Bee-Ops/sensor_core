# sensor-core

SensorCore makes it as easy to use Raspberry Pi SBCs for scientific data collection
in long-running experiments. SensorCore is a pre-baked set of functionality and design choices that hide much of the complexity of managing devices, sensors, and data flows.


SENSOR INTEGRATION
- Plug-n-play for a range of common sensors
- Easily extended with new sensors and custom data processing


DATA MANAGEMENT
- Pushes recordings & data directly to cloud storage
- Processes recordings and data on the device or via an ETL for data aggregation and summarisation
- Captures metadata in accordance with FAIR principles


DEVICE MANAGEMENT
- Simplifies management of a "fleet" of RPIs sensors running autonomously
- Provides recipes & functionality for spinning up a secure, internet-accessible dashboard
- Manages upgrade of the RPI OS, the SensorCore software and any custom software
- Manages security via a firewall
- Manages Wifi and other network connections
- Controls red/green health status LEDs on the device
- Ensures recording is persistent and reliable over reboots / power cycles / etc


Key design decisions:
- Python: The system is written in Python to make it easy to extend and modify.
- Push-to-cloud: The system pushes data to cloud storage rather than persistently storing it on device.
- Memory-as-disk: The system uses memory-as-disk to reduce wear on the SD card (a key single point of failure).
- Strict file naming: The system enforces strict file naming conventions to ensure that data is easily identifiable and manageable, and related to FAIR records.
- Configuration is stored in Git.


## Installation

To install the code for development and integration purposes, run the command:

`pip install sensor_core`

`pip install git+https://github.com/Oxford-Bee-Ops/sensor-core`


To install on a Raspberry Pi SBC for live deployment, follow the instructions in Usage > User Flow below.


## Usage
### PRE-REQUISITES
You will need: 
- a GitHub account to store your *fleet* configuration and any custom code you choose to write
- an Azure account for storage of your sensor output
- some Raspberry Pi devices and sensors!


### USER FLOW - BASIC SENSING USING SUPPORTED SENSORS
- Physically build your Raspberry Pi and attach your chosen sensors.
- Install an SD card with the Raspberry Pi OS and power up your RPI.
    - In the RaspberryPi Imager, make sure you enable SSH access and include default Wifi config for easy access.
- Copy the keys.env, system.cfg and fleet_config.py files from the SensorCore repo /example folder.
- Customize the keys.env:
    - Set `cloud_storage_key` to enable access to your Azure Storage accounts (see notes in keys.env)
    - Set `my_git_ssh_private_key` to enable access to your Git repo (see further notes in keys.env)
- Customize the system.cfg:
    - You can leave most values as defaults, but you should expect to set:
        - `my_git_repo_url` to your GitHub repo URL
        - `inventory_class` to point to your python fleet config file (eg "my_configs.fleet_config.Inventory")
    - See the system.cfg file in /example for more details.
- Customize the fleet_config.py to contain config for your device(s) and check your changes into Git!
    - You will need the mac address of the device's wlan0 interface as the identifier of the device
    - To get the mac address run `cat /sys/class/net/wlan0/address`
    - See the example fleet_config.py for more details.
    - If you don't check your changes into Git, they won't appear on the device!
- Log in to your RPi:
    - create a **.sensor_core** folder in your user home directory (ie `/home/<user>/.sensor_core`)
    - copy your **keys.env** and **system.cfg** to the .sensor_core folder
    - copy **rpi_installer.sh** from /scripts to the .sensor_core folder
    - run `dos2unix rpi_installer.sh` to ensure the file is in the right format
    - run `chmod +x rpi_installer.sh` to make it executable
    - run `./rpi_installer.sh` to install SensorCore on your RPi
- If your system.cfg has `auto_start_on_install="Yes"`, SensorCore will now be running!
- You can check by running the command line interface (CLI):
    - run `bcli`


### USER FLOW - EXTENDING & CUSTOMIZING
- Supporting new sensors
- Custom processing of recordings or data
- Contributing updates to SensorCore


### USER FLOW - ETL
- Setting up an ETL pipeline to process the data

## RPi device management functions
FC=Fleet config; SC=system.cfg; KE=keys.env

| Function  | Config control | Notes |
| ------------- | ------------- | ------------- |
| Automatic code updates | FC:`auto_update_code` | Uses crontab + `uv pip install` + your Git project's pyproject.toml to refresh your code and its dependencies (including SensorCore) on a configurable frequency
| Automatic OS updates | FC:`auto_update_os` |  Uses crontab + `sudo apt update && sudo apt upgrade -y` to update the OS on a configurable frequency.  This is a good best practice for staying up to date with security fixes.
| Firewall | SC:`enable_firewall` | Installs and configures UFW (Uncomplicated Firewall)
| Wifi AP awareness | FC:`wifi_clients` | Enable devices to auto-connect to the network by pre-configuring access point details.
| Wifi connections | FC:`attempt_wifi_recovery` | If internet connectivity is lost, try to auto-recover by switching wifi APs and other actions. Requires wifi_clients to be set in the FC.
| Status LEDs | FC:`manage_leds` | Controls a red & green LED used to reflect system status
| SD card wear | SC:`journald_Storage` | Make logging volatile so that it is written to memory rather than the SD card; logs will be lost over reboot as a result.

## System setup

| Function  | Config control | Notes |
| ------------- | ------------- | ------------- |
| Cloud storage access key | KE:`cloud_storage_key` | The Shared Access Signature that provides access to your Azure cloud storage
| Auto-start SensorCore | SC:`auto_start_on_install` | Starts SensorCore automatically after installation and / or reboot; unless manual mode invoked via CLI.
| Install a virtual environment | SC:`venv_dir` | Uses uv to install a venv unless one already exists at this location
| Code install location | SC:`my_code_dir` | Specifies the location where code should be installed from Git
| Git repo | SC:`my_git_repo_url` | URL of your Git repo containing your configuration and any custom code
| Git branch | SC:`my_git_branch` | Name of the Git branch to use if not main
| SSH key for Git | SC:`my_git_ssh_private_key_file` | The location of the private SSH key file that provides access to your code repo; not required if your repo is public
| Fleet config | SC:`inventory_class` | The class reference to a python file that defines the configuration for each of your RPi devices (your "fleet")

