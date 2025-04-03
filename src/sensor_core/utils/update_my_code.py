import os
import sys
from pathlib import Path

from git import Repo

from sensor_core import configuration as root_cfg
from sensor_core.utils import utils

logger = utils.setup_logger("sensor_core")


def update_my_code():
    if root_cfg.system_cfg is None:
        logger.error(f"{utils.RAISE_WARN()}No system configuration found")
        sys.exit(1)

    # Check for the .git file to see if the repository already exists
    repo_name = root_cfg.system_cfg.my_git_repo_url.split("/")[-1]
    assert repo_name.endswith(".git"), "Repository name must end with .git"
    repo_path: Path = Path.home() / root_cfg.system_cfg.my_code_dir / repo_name

    # Pull the latest from Git
    if not repo_path.exists():
        # We want the equivalent of:
        # git clone --branch my_git_branch --depth 1 my_git_repo_url my_code_dir
        Repo.clone_from(
            root_cfg.system_cfg.my_git_repo_url,
            repo_path,
            branch=root_cfg.system_cfg.my_git_branch,
            depth=1,
        )
        logger.info(f"Cloned user's repo {root_cfg.system_cfg.my_git_repo_url} to {repo_path}")
    else:
        Repo(repo_path).remote().fetch(depth=1)
        logger.info(f"Fetched latest changes from {root_cfg.system_cfg.my_git_repo_url} to {repo_path}")


    # Build the dependencies and install all in the virtual environment using uv
    code_dir_path = Path.home() / root_cfg.system_cfg.my_code_dir / repo_path.stem
    if code_dir_path.exists():
        os.chdir(code_dir_path)
        utils.run_cmd("uv pip install -e .")
    else:
        print(f"Failed to find the code directory at {code_dir_path}")
        sys.exit(1)
