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

To install the code, run:

`pip install git+https://github.com/Oxford-Bee-Ops/sensor_core`


To install on a Raspberry Pi SBC for live deployment, follow the instructions in Usage > User Flow below.


## Usage
### PRE-REQUISITES
You will need: 
- a GitHub account to store your *fleet* configuration and any custom code you choose to write
- an Azure account for storage of your sensor output
- a Raspberry Pi SBC and some sensors!
- basic experience with Python coding


### USER FLOW - BASIC SENSING USING SUPPORTED SENSORS
The following is a basic step-by-step guide which can be substantially automated when you come to deploy lots of devices!

- Physically build your Raspberry Pi and attach your chosen sensors.
- Install an SD card with the Raspberry Pi OS and power up your RPI.
    - In the RaspberryPi Imager, make sure you enable SSH access and include default Wifi config for easy access.
- Copy the files in the SensorCore repo `/example` folder to your own computer / dev environment / git project.
- Edit **keys.env**:
    - Set `cloud_storage_key` to enable access to your Azure Storage accounts (see notes in keys.env)
    - Do not check keys.env into Git - you should keep this keys file somewhere secure.
- Edit **fleet_config.py** to contain the configuration for your device(s)
    - You will need the mac address of the device's wlan0 interface as the identifier of the device
    - To get the mac address run `cat /sys/class/net/wlan0/address`
    - See the example fleet_config.py for more details.
- Edit the **system.cfg**:
    - You can leave all values as defaults to start with but you will likely want to customise later.
    - In particular, you will need to set `my_git_repo_url` to your GitHub repo URL if you want SensorCore to auto-update the device code.
    - See the system.cfg file in /example for more details.
- Log in to your RPi:
    - create a **.sensor_core** folder in your user home directory (ie `/home/<user>/.sensor_core`)
    - copy your **keys.env** and **system.cfg** to the .sensor_core folder
    - install `uv` to manage your venv and code, by running:
        - `curl -LsSf https://astral.sh/uv/install.sh | sh`
    - create and activate your virtual environment in $HOME/venv:
        - `uv venv $HOME/venv`
        - `source $HOME/venv/bin/activate`
    - install pre-requisites:
        - `sudo apt-get install libsystemd-dev libffi-dev`
        - libsystemd-dev is required by systemd-python to interact with journald
        - libffi-dev is required by azure-storage-blob via cryptography 
        - `sudo apt-get install -y cmake build-essential gfortran libopenblas-dev liblapack-dev`
        - required for building numpy
    - install sensor-core:
        - `uv pip install git+https://github.com/Oxford-Bee-Ops/sensor_core`
    - install your now-customized example code in **$HOME/code/<my_git_project_name>/**
    - run SensorCore:
        - `cd $HOME/code/<my_git_project_name>`
        - `python run_sensor_core.py`
- If your system.cfg has `auto_start_on_install="Yes"`, SensorCore will now be running!
- You can check by running the command line interface (CLI):
    - run `bcli`


### USER FLOW - EXTENDING & CUSTOMIZING
- Supporting new sensors
    - To support new sensors, create a new python file in the same form as my_sensor_example.py that extends **sensor_core.Sensor**.
    - You will need to define a configuration object for your sensor that subclasses **sensor_core.SensorCfg**.
    - You will need to update your fleet_config to use this new SensorCfg.
- Custom processing of recordings or data
    - To implement custom data processing, create a new pythong file in the same form as my_processor_example.py that extends **sensor_core.DataProcessor**.
    - You will need to define a configuration object for your DataProcessor that subclasses **sensor_core.DataProcessorCfg**.
    - You will need to update your fleet_config to use this new DataProcessorCfg.
- Contributing updates to SensorCore
    - In the first instance, please email admin@bee-ops.com.


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
| SD card wear | SC:`enable_volatile_logs` | Make logging volatile so that it is written to memory rather than the SD card to reduce wear; logs will be lost over reboot as a result but import logs are streamed to cloud storage in real time anyway.

## System setup

| Function  | Config control | Notes |
| ------------- | ------------- | ------------- |
| Cloud storage access key | KE:`cloud_storage_key` | The Shared Access Signature that provides access to your Azure cloud storage
| Auto-start SensorCore | SC:`auto_start` | Starts SensorCore automatically after reboot; unless manual mode invoked via CLI.
| Install a virtual environment | SC:`venv_dir` | Uses uv to install a venv unless one already exists at this location
| Code install location | SC:`my_code_dir` | Specifies the location where code should be installed from Git
| Git repo | SC:`my_git_repo_url` | URL of your Git repo containing your configuration and any custom code
| Git branch | SC:`my_git_branch` | Name of the Git branch to use if not main

