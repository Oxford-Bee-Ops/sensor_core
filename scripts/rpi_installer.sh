#!/bin/bash

# RPI installer
#
# This script installs SensorCore code according to the ~user/.sensor_core/system.cfg file
# It is safe to re-run this script multiple times.
#
# Pre-requisites:
# - system.cfg file must exist in the $HOME/.sensor_core directory
# - keys.env file must exist in the $HOME/.sensor_core directory
# - SSH enabled on the RPi
# - SSH keys for the SensorCore repository must exist in the $HOME/.sensor_core directory
#   if using the private repository
# - SSH keys for the users code repository must exist in the $HOME/.sensor_core directory
#   if using a private repository
#
# This script will:
# - install the UV package installer
# - create a venv in $HOME/.sensor_core/venv if one doesn't already exist
# - install the SensorCore code & dependencies in the venv
# - make persistent changes to the RPi for long-running operations:
#   - make the log storage volatile
#   - set predictable network interface names
#   - enable the I2C interface
# - start SensorCore if auto_start_on_install is set in the system.cfg file
#
# Starting SensorCore (either via this script, via code or via the CLI) will:
# - cause SensorCore to persist across reboots via crontab
#   - invoking your custom code as defined in the fleet config.
# - start DeviceManager, which will optionally manage (depending on the system.cfg file):
#   - Wifi
#   - LEDs for sensor status
#   - auto-updates of the OS
#   - auto-updates of the SensorCore code
#   - auto-updates of the users code.
# 		- including installing the users code if a git repo is specified in the system.cfg file

# Function to check pre-requisites
check_prerequisites() {
    echo "Checking pre-requisites..."
    if [ ! -d "$HOME/.sensor_core" ]; then
        echo "Error: $HOME/.sensor_core directory is missing"
        exit 1
    fi
    if [ ! -f "$HOME/.sensor_core/system.cfg" ]; then
        echo "Error: system.cfg file is missing in $HOME/.sensor_core"
        exit 1
    fi
    if [ ! -f "$HOME/.sensor_core/keys.env" ]; then
        echo "Error: keys.env file is missing in $HOME/.sensor_core"
        exit 1
    fi
    if ! command -v sudo >/dev/null 2>&1; then
        echo "Error: sudo is not installed or not available"
        exit 1
    fi
    if ! command -v git >/dev/null 2>&1; then
        echo "Error: git is not installed or not available"
        exit 1
    fi
    # Check ssh is enabled
    if ! systemctl is-active --quiet ssh; then
        echo "Error: SSH is not enabled. Please enable SSH."
        exit 1
    fi
    echo "All pre-requisites are met."
}

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

# Install SSH keys from the ./sensor_core directory to the ~/.ssh directory
install_ssh_keys() {
    echo "Installing SSH keys..."
    if [ ! -d "$HOME/.ssh" ]; then
        mkdir -p "$HOME/.ssh" || { echo "Failed to create ~/.ssh directory"; exit 1; }
    fi

    # Copy the private key file to the ~/.ssh directory
    if [ -f "$HOME/.sensor_core/$dua_git_ssh_private_key_file" ]; then
        cp "$HOME/.sensor_core/$dua_git_ssh_private_key_file" "$HOME/.ssh/" || { echo "Failed to copy $dua_git_ssh_private_key_file to ~/.ssh"; exit 1; }
        chmod 600 "$HOME/.ssh/$dua_git_ssh_private_key_file" || { echo "Failed to set permissions for $dua_git_ssh_private_key_file"; exit 1; }
    else
        echo "Error: Private key file $dua_git_ssh_private_key_file does not exist in $HOME/.sensor_core"
        exit 1
    fi

    # Copy the users private key file to the ~/.ssh directory
    if [ -f "$HOME/.sensor_core/$my_git_ssh_private_key_file" ]; then
        cp "$HOME/.sensor_core/$my_git_ssh_private_key_file" "$HOME/.ssh/" || { echo "Failed to copy $my_git_ssh_private_key_file to ~/.ssh"; exit 1; }
        chmod 600 "$HOME/.ssh/$my_git_ssh_private_key_file" || { echo "Failed to set permissions for $my_git_ssh_private_key_file"; exit 1; }
    else
        echo "Error: Private key file $my_git_ssh_private_key_file does not exist in $HOME/.sensor_core"
        exit 1
    fi

    # Set up known_hosts for GitHub if it doesn't already exist
    if ! ssh-keygen -F github.com > /dev/null; then
        ssh-keyscan github.com >> "$HOME/.ssh/known_hosts"
    fi

    echo "SSH keys installed successfully."
}

# Function to update journald configuration using the values exported from system.cfg
# It restarts the systemd-journald service to apply the changes
update_journald_config() {
    echo "Updating journald configuration..."
    local journald_config="/etc/systemd/journald.conf"
    local temp_file="/tmp/journald.conf"

    if [ -z "$journald_Storage" ] || [ -z "$journald_SystemMaxUse" ]; then
        echo "Error: journald_Storage or journald_SystemMaxUse is not set in system.cfg"
        exit 1
    fi

    # Validate journald_Storage
    if [[ ! "$journald_Storage" =~ ^(volatile|persistent|auto|none)$ ]]; then
        echo "Error: Invalid value for journald_Storage ($journald_Storage). Allowed values are 'volatile', 'persistent', 'auto', or 'none'."
        exit 1
    fi

    # Validate journald_SystemMaxUse
    if [[ ! "$journald_SystemMaxUse" =~ ^[0-9]+[KMG]?$ ]]; then
        echo "Error: Invalid value for journald_SystemMaxUse ($journald_SystemMaxUse). It must be a number optionally followed by 'K', 'M', or 'G'."
        exit 1
    fi

    if [ ! -f "$journald_config" ]; then
        echo "Error: $journald_config does not exist. Cannot update journald configuration."
        exit 1
    fi

    # Backup the original configuration
    sudo cp "$journald_config" "${journald_config}.bak" || { echo "Failed to backup $journald_config"; exit 1; }

    # Modify or append the configuration
    sudo awk -v storage="$journald_Storage" -v max_use="$journald_SystemMaxUse" '
    BEGIN { storage_updated = 0; max_use_updated = 0 }
    /^Storage=/ { $0 = "Storage=" storage; storage_updated = 1 }
    /^SystemMaxUse=/ { $0 = "SystemMaxUse=" max_use; max_use_updated = 1 }
    END {
        if (!storage_updated) print "Storage=" storage
        if (!max_use_updated) print "SystemMaxUse=" max_use
    } 1' "$journald_config" > "$temp_file" || { echo "Failed to modify $journald_config"; exit 1; }

    # Replace the original file with the updated one
    sudo mv "$temp_file" "$journald_config" || { echo "Failed to update $journald_config"; exit 1; }

    # Restart systemd-journald to apply changes
    sudo systemctl restart systemd-journald || { echo "Failed to restart systemd-journald"; exit 1; }

    echo "journald configuration updated successfully."
}

# Function to set RPi config
update_rpi_config() {
	###############################################
	# Set RPi to use predictable network interface names
	# wlan0 remains, but the wifi dongle gets renamed from wlan1 to wlx<mac address>
	# This is critical if we're to use mac address as the device_id
	###############################################
	echo "Setting predictable network interface names"
	sudo raspi-config nonint do_net_names 0

	###############################################
	# Enable i2c interface
	###############################################
	echo "Enabling I2C interface"
	sudo raspi-config nonint do_i2c 0
}

# Function to install UV package installer
install_uv() {
    source $HOME/.local/bin/env
    if command -v uv >/dev/null 2>&1; then
        echo "UV is already installed."
        return
    fi

    echo "Installing UV package installer..."
    curl -LsSf https://astral.sh/uv/install.sh | sh || { echo "Failed to install UV"; exit 1; }
    source $HOME/.local/bin/env

    # Verify that UV is installed
    if ! command -v uv >/dev/null 2>&1; then
        echo "Error: UV installation failed. 'uv' command not found."
        exit 1
    fi

    echo "UV installed successfully."
}

# Function to create a virtual environment if it doesn't already exist
# The venv location is specified in the system.cfg (venv_dir)
create_venv() {
    if [ -z "$venv_dir" ]; then
        echo "Error: venv_dir is not set in system.cfg"
        exit 1
    fi
    if [ ! -d "$HOME/$venv_dir" ]; then
        echo "Creating $HOME/$venv_dir"
        # Use uv to create the virtual environment
        uv venv --system-site-packages "$HOME/$venv_dir" || { echo "Failed to create virtual environment"; exit 1; }
    else
        echo "Virtual environment already exists."
    fi
}

# Function to install SensorCore
install_sensor_core() {
    echo "Installing SensorCore..."

    if [ -z "$dua_git_url" ] || [ -z "$dua_git_branch" ] || [ -z "$sensor_core_code_dir" ]; then
        echo "Error: dua_git_url, dua_git_branch, or sensor_core_code_dir is not set in system.cfg"
        exit 1
    fi

    ############################################
    # Manage SSH prep
    ############################################
    # Verify that the private key file exists
    if [ ! -f "$HOME/.ssh/$dua_git_ssh_private_key_file" ]; then
        echo "Error: Private key file ~/.ssh/$dua_git_ssh_private_key_file does not exist."
        exit 1
    fi

    # Ensure the private key has correct permissions
    chmod 600 "$HOME/.ssh/$dua_git_ssh_private_key_file"

    # Set the GIT_SSH_COMMAND
    export GIT_SSH_COMMAND="ssh -i $HOME/.ssh/$dua_git_ssh_private_key_file -o IdentitiesOnly=yes"

    # Persist the GIT_SSH_COMMAND in .bashrc if not already present
    if ! grep -qs "export GIT_SSH_COMMAND=" "$HOME/.bashrc"; then
        echo "export GIT_SSH_COMMAND='ssh -i \$HOME/.ssh/$dua_git_ssh_private_key_file -o IdentitiesOnly=yes'" >> "$HOME/.bashrc"
    fi

    # Ensure known_hosts exists and add GitHub key if necessary
    mkdir -p "$HOME/.ssh"
    touch "$HOME/.ssh/known_hosts"
    chmod 600 "$HOME/.ssh/known_hosts"
    if ! ssh-keygen -F github.com > /dev/null; then
        echo "Adding GitHub key to known_hosts"
        ssh-keyscan github.com >> "$HOME/.ssh/known_hosts"
    fi

    ############################################
    # Do the Git clone and pip install
    ############################################
    # Validate sensor_core_code_dir
    if [[ "$sensor_core_code_dir" =~ [^a-zA-Z0-9/_-] ]]; then
        echo "Error: sensor_core_code_dir contains invalid characters. Only alphanumeric, '/', '_', and '-' are allowed."
        exit 1
    fi

    # Create the repo directory if it doesn't already exist
    if [ ! -d "$HOME/$sensor_core_code_dir" ]; then
        echo "Creating directory $HOME/$sensor_core_code_dir..."
        mkdir -p "$HOME/$sensor_core_code_dir" || { echo "Failed to create directory $HOME/$sensor_core_code_dir"; exit 1; }
    fi

    # Clone the repository if it doesn't already exist
    if [ ! -d "$HOME/$sensor_core_code_dir/.git" ]; then
        echo "Cloning SensorCore repository into $HOME/$sensor_core_code_dir..."
        git clone --branch "$dua_git_branch" --depth 1 "git@$dua_git_url" "$HOME/$sensor_core_code_dir" || { echo "Failed to clone SensorCore repository"; exit 1; }
    else
        echo "SensorCore repository already exists. Pulling latest changes and resetting so we use the latest release..."
        git -C "$HOME/$sensor_core_code_dir" fetch --depth 1 origin "$dua_git_branch" || { echo "Failed to fetch updates for SensorCore repository"; exit 1; }
        git -C "$HOME/$sensor_core_code_dir" reset --hard origin/"$dua_git_branch" || { echo "Failed to reset to the latest commit for SensorCore repository"; exit 1; }
    fi

    # Activate the venv
    if [ -f "$HOME/$venv_dir/bin/activate" ]; then
        source "$HOME/$venv_dir/bin/activate" || { echo "Failed to activate virtual environment"; exit 1; }
    else
        echo "Error: Virtual environment not found at $HOME/$venv_dir/bin/activate"
        exit 1
    fi

    # Reinstall the latest version of SensorCore in the virtual environment
    echo "Reinstalling SensorCore in editable mode..."
    cd "$HOME/$sensor_core_code_dir" || { echo "Failed to navigate to $HOME/$sensor_core_code_dir"; exit 1; }
    uv pip install -e . || { echo "Failed to reinstall SensorCore in editable mode"; exit 1; }

    echo "SensorCore installed successfully."
}

# Function to install user's code
install_user_code() {
    echo "Installing user's code..."

    if [ -z "$my_git_repo_url" ] || [ -z "$my_git_branch" ] || [ -z "$my_code_dir" ]; then
        echo "Error: my_git_repo_url, my_git_branch, or my_code_dir is not set in system.cfg"
        exit 1
    fi

    ############################################
    # Manage SSH prep
    ############################################
    # Verify that the private key file exists
    if [ ! -f "$HOME/.ssh/$my_git_ssh_private_key_file" ]; then
        echo "Error: Private key file ~/.ssh/$my_git_ssh_private_key_file does not exist."
        exit 1
    fi

    # Ensure the private key has correct permissions
    chmod 600 "$HOME/.ssh/$my_git_ssh_private_key_file"

    # Set the GIT_SSH_COMMAND
    export GIT_SSH_COMMAND="ssh -i $HOME/.ssh/$my_git_ssh_private_key_file -o IdentitiesOnly=yes"

    # Persist the GIT_SSH_COMMAND in .bashrc if not already present
    if ! grep -qs "export GIT_SSH_COMMAND=" "$HOME/.bashrc"; then
        echo "export GIT_SSH_COMMAND='ssh -i \$HOME/.ssh/$my_git_ssh_private_key_file -o IdentitiesOnly=yes'" >> "$HOME/.bashrc"
    fi

    # Ensure known_hosts exists and add GitHub key if necessary
    mkdir -p "$HOME/.ssh"
    touch "$HOME/.ssh/known_hosts"
    chmod 600 "$HOME/.ssh/known_hosts"
    if ! ssh-keygen -F github.com > /dev/null; then
        echo "Adding GitHub key to known_hosts"
        ssh-keyscan github.com >> "$HOME/.ssh/known_hosts"
    fi

    ##############################################
    # Do the Git clone and pip install
    ###############################################
    # Validate my_code_dir
    if [[ "$my_code_dir" =~ [^a-zA-Z0-9/_-] ]]; then
        echo "Error: my_code_dir contains invalid characters. Only alphanumeric, '/', '_', and '-' are allowed."
        exit 1
    fi

    # Create the repo directory if it doesn't already exist
    if [ ! -d "$HOME/$my_code_dir" ]; then
        echo "Creating directory $HOME/$my_code_dir..."
        mkdir -p "$HOME/$my_code_dir" || { echo "Failed to create directory $HOME/$my_code_dir"; exit 1; }
    fi

    # Check for the .git file to see if the repository already exists
    if [ ! -d "$HOME/$my_code_dir/.git" ]; then
        echo "Cloning user's code repository..."
        git clone --branch "$my_git_branch" --depth 1 "git@$my_git_repo_url" "$HOME/$my_code_dir" || { echo "Failed to clone user's code repository"; exit 1; }
    else
        echo "User's code repository already exists. Resetting and pulling latest changes..."
        git -C "$HOME/$my_code_dir" fetch origin --depth 1 "$my_git_branch" || { echo "Failed to fetch updates for user's code repository"; exit 1; }
    fi

    # Activate the venv
    if [ -f "$HOME/$venv_dir/bin/activate" ]; then
        source "$HOME/$venv_dir/bin/activate" || { echo "Failed to activate virtual environment"; exit 1; }
    else
        echo "Error: Virtual environment not found at $HOME/$venv_dir/bin/activate"
        exit 1
    fi

    # Reinstall the latest version of SensorCore in the virtual environment
    echo "Reinstalling user code in editable mode..."
    cd "$HOME/$my_code_dir" || { echo "Failed to navigate to $HOME/$my_code_dir"; exit 1; }
    uv pip install -e . || { echo "Failed to reinstall user code in editable mode"; exit 1; }

    echo "User's code installed successfully."
}

# Install the Uncomplicated Firewall and set appropriate rules.
install_ufw() {
    # If enable_firewall="Yes"
    if [ "$enable_firewall" != "Yes" ]; then
        echo "Firewall installation skipped as enable_firewall is not set to 'Yes'."
        return
    fi
    sudo apt-get install -y ufw

    # Clear any current rules
    sudo ufw --force reset

    # Allow IGMP broadcast traffic
    sudo ufw allow proto igmp from any to 224.0.0.1
    sudo ufw allow proto igmp from any to 224.0.0.251
    # Allow SSH on 22 and FTP on 21
    #sudo ufw allow 21
    sudo ufw allow 22
    # Allow DNS on 53
    #sudo ufw allow 53
    # Allow DHCP on 67 / 68
    #sudo ufw allow 67/udp
    #sudo ufw allow 68/udp
    # Allow NTP on 123
    #sudo ufw allow 123
    # Allow HTTPS on 443
    sudo ufw allow 443
    # Re-enable the firewall
    sudo ufw --force enable
}

# Function to start SensorCore if auto_start_on_install is set in the system.cfg file
auto_start_on_install_if_required() {
    if [ -z "$auto_start_on_install" ]; then
        echo "Error: auto_start_on_install is not set in system.cfg"
    elif [ "$auto_start_on_install" == "Yes" ]; then
        echo "Starting SensorCore..."
        # Start SensorCore by calling the run_sensor_core.sh
        # The location of the script is defined in the system.cfg with key of 'sensor_core_code_dir'
        if [ ! -f "$HOME/$sensor_core_code_dir/scripts/run_sensor_core.sh" ]; then
            echo "Error: run_sensor_core.sh file is missing in $HOME/$sensor_core_code_dir/scripts"
            exit 1
        fi
        # Start SensorCore in the background and redirect output to stdout
        "$HOME/$sensor_core_code_dir/scripts/run_sensor_core.sh" 2>&1
        if [ $? -ne 0 ]; then
            echo "Error: Failed to start SensorCore"
            exit 1
        fi
        echo "SensorCore started successfully."
    else
        echo "auto_start_on_install is not set to 'Yes'. Not starting SensorCore."
    fi
}

###################################################################################################
#
# Main script execution to configure a RPi device suitable for long-running SensorCore operations
# 
###################################################################################################
echo "Starting RPi installer..."
check_prerequisites
export_system_cfg
install_ssh_keys
update_rpi_config
update_journald_config
install_uv
create_venv
install_user_code
install_ufw
auto_start_on_install_if_required

# Add a flag file in the .sensor_core directory to indicate that the installer has run
touch "$HOME/.sensor_core/rpi_installer_ran"
echo "RPi installer completed successfully."

