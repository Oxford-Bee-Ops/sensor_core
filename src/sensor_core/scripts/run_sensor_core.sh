#!/bin/bash

# Function to read system.cfg file and export the key-value pairs found
export_system_cfg() {
    if [ ! -f "$HOME/.sensor_core/system.cfg" ]; then
        echo "Error: system.cfg file is missing in $HOME/.sensor_core"
        exit 1
    fi
    dos2unix -q "$HOME/.sensor_core/system.cfg" || { echo "Failed to convert system.cfg to Unix format"; exit 1; }
    while IFS='=' read -r key value; do
        if [[ $key != \#* && $key != "" ]]; then
            if [[ $key =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
                # Strip surrounding quotes from the value
                value=$(echo "$value" | sed -e 's/^"//' -e 's/"$//')
                export "$key"="$value"
            else
                echo "Warning: Skipping invalid key '$key' in system.cfg"
            fi
        fi
    done < "$HOME/.sensor_core/system.cfg"

    # Check if my_start_script is set
    if [ ! -n "$my_start_script" ]; then
        echo "Error: my_start_script is not set in system.cfg"
        exit 1
    fi
}

# Function to create a ramdisk mount
create_ramdisk_mount() {
    sc_mount="/sensor_core"
    if ! mountpoint -q "$sc_mount"; then
        echo "Creating ramdisk mount at $sc_mount"
        sudo mkdir -p "$sc_mount"

        if ! sudo mount -t tmpfs -o size=1200M tmpfs "$sc_mount"; then
            echo "Error: Failed to create ramdisk mount at $sc_mount"
            exit 1
        fi
        
        # Ensure the mount persists across reboots
        if ! grep -q "$sc_mount" /etc/fstab; then
            echo "tmpfs $sc_mount tmpfs size=1200M 0 0" | sudo tee -a /etc/fstab > /dev/null
        fi
    else
        echo "Ramdisk mount already exists at $sc_mount"
    fi
    # Change ownership of the mount to the local user
    echo "Changing ownership of $sc_mount to $USER"
    sudo chown "$USER:$USER" "$sc_mount"
}

# Function to activate the virtual environment
activate_venv() {
    # Ensure venv_dir is exported from system.cfg
    if [ -z "$venv_dir" ]; then
        echo "Error: venv_dir is not set in system.cfg"
        exit 1
    fi
    source "$HOME/$venv_dir/bin/activate" "$venv_dir"
    if [ $? -ne 0 ]; then
        echo "Error: Failed to activate virtual environment $HOME/$venv_dir"
        exit 1
    fi
    echo "Activated virtual environment $venv_dir"
}


###################################################################################################
# Run SensorCore
#
# We run it in the users code directory so that their code is in the python path.
# If an instance is already running, the edge_orchestrator.py will exit cleanly.
###################################################################################################
export_system_cfg
create_ramdisk_mount
activate_venv
echo "Calling $my_start_script in $HOME/$venv_dir"
python -m $my_start_script 2>&1 | /usr/bin/logger -t SENSOR_CORE

