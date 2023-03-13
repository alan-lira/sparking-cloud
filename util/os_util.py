from os import listdir
from pathlib import Path
from typing import Any


def find_full_file_name_by_prefix(root_folder: Path,
                                  file_prefix: Any) -> str:
    files_list_with_prefix = [filename for filename in listdir(root_folder) if filename.startswith(file_prefix)]
    return str(files_list_with_prefix[0])
