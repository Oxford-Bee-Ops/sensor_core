#!/bin/bash

# Function to get the Git project name from the URL
git_project_name() {
    # Get the Git project name from the URL
    local url="$1"
    local project_name=$(basename "$url" .git)
    echo "$project_name"
}


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

    # Append the git project name to the my_code_dir variable
    if [ -n "$my_git_repo_url" ]; then
        my_code_dir="$my_code_dir/$(git_project_name "$my_git_repo_url")"
    fi
    # Append the git project name to the sensor_core_code_dir variable
    if [ -n "$dua_git_url" ]; then
        sensor_core_code_dir="$sensor_core_code_dir/$(git_project_name "$dua_git_url")"
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
    # Check if the virtual environment directory exists
    if [ -d "$HOME/$venv_dir" ]; then
        # Activate the virtual environment
        source "$HOME/$venv_dir/bin/activate"
    else
        echo "Error: Virtual environment not found at $HOME/$venv_dir"
        exit 1
    fi
}

# Function to ensure the SensorCore code directory is in the Python path
add_to_python_path() {
    # Ensure $my_code_dir is defined
    if [ -z "$my_code_dir" ]; then
        echo "Error: my_code_dir is not defined in the configuration file"
        exit 1
    fi

    sensor_core_code_dir="$HOME/$my_code_dir"
    if [[ ":$PYTHONPATH:" != *":$sensor_core_code_dir:"* ]]; then
        export PYTHONPATH="$sensor_core_code_dir:$PYTHONPATH"
        echo "Added $sensor_core_code_dir to PYTHONPATH"
    else
        echo "$sensor_core_code_dir is already in PYTHONPATH"
    fi
}

###################################################################################################
# Run SensorCore
#
# We run it in the users code directory so that their code is in the python path.
# If an instance is already running, the edge_orchestrator.py will exit cleanly.
###################################################################################################
echo "Starting SensorCore"
export_system_cfg
create_ramdisk_mount
activate_venv
#add_to_python_path
cd "$HOME/$my_code_dir"
nohup python -m sensor_core.edge_orchestrator 2>&1 | /usr/bin/logger -t SENSOR_CORE &

