####################################################################################################
# Utils that have no dependencies on other modules in the project
####################################################################################################
import subprocess
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, Union

from pydantic_settings import BaseSettings


def create_root_working_dir(path: Path) -> None:
    """ Create the root working directory if it doesn't exist. 
    This requires root privileges on Linux.
   
    Args:
        path (Path): The path to the directory to create.

    Raises:
        subprocess.CalledProcessError: If the directory creation fails.
     """
    if not path.exists():
        try:
            subprocess.run(["sudo", "mkdir", "-p", str(path)], check=True)
            print(f"Directory {path} created successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to create directory {path}: {e}")
            raise e

############################################################
# Dataclass display utility
############################################################
def display_dataclass(obj: Any, indent: int=0) -> str:
    """
    Recursively display the contents of a dataclass hierarchy.

    Args:
        obj (Any): The dataclass object to display.
        indent (int): The current indentation level for nested objects.

    Returns:
        str: A formatted string representation of the dataclass hierarchy.
    """
    fbracket = ["[", "(", "{"]
    bbracket = ["]", ")", "}"]

    def fb(i: int) -> str:
        return f"{'  ' * i}{fbracket[i % 3]}"

    def bb(i: int) -> str:
        return f"{bbracket[i % 3]}"

    def id(i: int) -> str:
        return f"{'  ' * i}"

    def nlb(i: int) -> str:
        return f"{'  ' * i}{bb(i)}"

    if not is_dataclass(obj):
        return f"{fb(indent)}{obj!r}{bb(indent)}"

    result = ""

    for field in fields(obj):
        value = getattr(obj, field.name)
        if value is None:
            # Skip empty fields
            continue
        elif is_dataclass(value):
            # Recursively display nested dataclass
            result += f"{fb(indent)}{field.name}::\n{display_dataclass(value, indent + 1)}{nlb(indent)}\n"
        elif isinstance(value, list) and all(isinstance(item, (str, float, int)) for item in value):
            # Treat lists of simple types as a single block
            result += f"{fb(indent)}{field.name}={value}{bb(indent)}\n"
        elif isinstance(value, list):
            # Handle lists, including lists of dataclasses
            result += f"{fb(indent)}{field.name}::\n"
            for i, item in enumerate(value):
                result += f"{id(indent + 1)}[{i}]\n"
                result += f"{display_dataclass(item, indent + 2)}"
            result += f"{nlb(indent)}\n"
        else:
            # Display simple fields
            result += f"{fb(indent)}{field.name}={value!r}{bb(indent)}\n"
    return result


def save_settings_to_env(settings: BaseSettings, file_path: Union[str, Path]) -> None:
    """
    Save a Pydantic BaseSettings object to a .env file.

    Parameters:
        settings (BaseSettings): The Pydantic BaseSettings object to save.
        file_path (Union[str, Path]): The path to the .env file.
    """
    file_path = Path(file_path)  # Ensure file_path is a Path object
    with file_path.open("w", encoding="utf-8") as env_file:
        for key, value in settings.model_dump().items():
            if value is None:
                continue

            # Convert the value to a string and escape special characters if needed
            env_file.write(f"{key}={value}\n")
