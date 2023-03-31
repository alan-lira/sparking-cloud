from os import listdir
from pathlib import Path
from typing import Any


def find_full_file_name_by_prefix(root_folder: Path,
                                  file_prefix: Any) -> str:
    full_file_name = ""
    files_list_with_prefix = [filename for filename in listdir(root_folder) if filename.startswith(file_prefix)]
    if files_list_with_prefix:
        full_file_name = str(files_list_with_prefix[0])
    return full_file_name


def check_if_file_exists(file: Path) -> bool:
    return file.is_file()


def remove_file(file: Path) -> None:
    file_exists = check_if_file_exists(file)
    if file_exists:
        file.unlink()
