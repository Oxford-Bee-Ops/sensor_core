import os
from setuptools import setup
from setuptools.command.install import install

class PostInstallCommand(install):
    """Post-installation for setting executable permissions on scripts."""
    def run(self):
        install.run(self)
        scripts_dir = os.path.join(self.install_lib, "sensor_core", "scripts")
        for script in os.listdir(scripts_dir):
            script_path = os.path.join(scripts_dir, script)
            if os.path.isfile(script_path):
                os.chmod(script_path, 0o755)  # Set executable permissions

setup(
    cmdclass={
        'install': PostInstallCommand,
    },
)
