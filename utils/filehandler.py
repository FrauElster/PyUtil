# pylint: disable=C0111
# pylint: disable=W1203
import csv
import dataclasses
import datetime
import glob
import json
import logging
import os
import pickle
import shutil
import zipfile
from decimal import Decimal
from typing import List, Any, Dict, Union, Optional

LOGGER = logging.getLogger(__name__)


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        if isinstance(o, datetime.datetime):
            return o.strftime("%H:%M:%S %d.%m.%y")
        if isinstance(o, datetime.time):
            return o.strftime("%H:%M:%S")
        if isinstance(o, Decimal):
            return float(o)
        if type(o) in [list, dict, bool, int, float, str]:
            return o
        if hasattr(o, "to_json") and callable(o.to_json):
            return o.to_json()
        else:
            return json.JSONEncoder.default(self, o)


def serialize(raw: Any) -> Union[Dict, List]:
    return json.loads(jsonize(raw))


def jsonize(raw: Any) -> str:
    return json.dumps(raw, cls=EnhancedJSONEncoder)


def to_rel_file_path(abs_path: str):
    package_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.relpath(abs_path, os.path.join(package_directory, '..', '..'))


def to_abs_file_path(file_name: str) -> str:
    """ :returns an existing absolute file path based on the project root directory + file_name"""
    package_directory = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(package_directory, '..', '..', file_name)
    path = os.path.normpath(path)
    return path


def create_dir(dir_name: str, is_abs: bool = False) -> str:
    """
    creates a directory if it is not already exiting
    :param is_abs: determines if the given path is absolute or relative to project root
    :param dir_name: directory name
    :return: the absolute path to the directory
    """
    dir_path: str = dir_name if is_abs else to_abs_file_path(dir_name)
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)
        LOGGER.info(f'created {dir_name} directory')

    return dir_path


def delete_file(filename: str, is_abs: bool = False) -> bool:
    filename = filename if is_abs else to_abs_file_path(filename)
    if os.path.isfile(filename):
        os.remove(filename)
        LOGGER.info(f'Removed file {filename}')
        return True
    else:
        if not os.path.exists(filename):
            LOGGER.warning(f'"{filename}" does not exist')
        else:
            LOGGER.warning(f'"{filename}" is not a file')
        return False


def delete_dir(dir_name: str, is_abs: bool = False) -> bool:
    """
    deletes a directory if it exists
    :param is_abs: determines if the given path is absolute or relative to project root
    :param dir_name: directory name
    :return: whether it exists or not
    """
    dir_path: str = dir_name if is_abs else to_abs_file_path(dir_name)
    if os.path.isdir(dir_path):
        shutil.rmtree(dir_path, ignore_errors=True)
        LOGGER.info(f'Removed directory "{dir_name}"')
        return True
    if not os.path.exists(dir_path):
        LOGGER.warning(f'"{dir_name}" does not exists')
    else:
        LOGGER.warning(f'"{dir_name}" is not a directory')
    return False


def rename_file(old_filename: str, new_filename: str, is_abs: bool = False) -> bool:
    old_filepath: str = old_filename if is_abs else to_abs_file_path(old_filename)
    new_filepath: str = new_filename if is_abs else to_abs_file_path(new_filename)
    if not check_if_file_exists(old_filepath):
        LOGGER.info(f"Could not rename {old_filepath} to {new_filepath}")
        return False
    os.rename(old_filepath, new_filepath)
    return True


def _greedy_file_resolution(filename: str) -> Optional[str]:
    if check_if_file_exists(filename, is_abs=True):
        return filename
    if check_if_file_exists(to_abs_file_path(filename), is_abs=True):
        return to_abs_file_path(filename)


def check_if_file_exists(filename: str, is_abs: bool = False) -> bool:
    """
    Checks if a file exist and if it is a file
    :param is_abs: determines if the given path is absolute or relative to project root
    :param filename: the file to check
    :return: whether it exists and is a file
    """
    filepath: str = filename if is_abs else to_abs_file_path(filename)
    if not os.path.exists(filepath):
        LOGGER.debug(f'{filepath} does not exist')
        return False
    if not os.path.isfile(filepath):
        LOGGER.debug(f'{filepath} is not a file')
        return False
    return True


def _greedy_dir_resolution(dirname: str) -> Optional[str]:
    if check_if_dir_exists(dirname, is_abs=True):
        return dirname
    if check_if_dir_exists(to_abs_file_path(dirname), is_abs=True):
        return to_abs_file_path(dirname)


def check_if_dir_exists(dirname: str, is_abs: bool = False) -> bool:
    """
    Checks if a directory exist and if it is a directory
    :param dirname: the directory to check
    :param is_abs: determines if the given path is absolute or relative to project root
    :return: whether it exists and is a file
    """
    dir_path: str = dirname if is_abs else to_abs_file_path(dirname)
    if not os.path.exists(dir_path):
        LOGGER.debug(f'{dir_path} does not exist')
        return False
    if not os.path.isdir(dir_path):
        LOGGER.debug(f'{dir_path} is not a directory')
        return False
    return True


def save_file(file_name: str, data: Any, is_abs: bool = False, force_pickle: bool = False) -> None:
    """ writes a file, if a file with file_name already exists its content gets overwritten """
    file_path: str = file_name if is_abs else to_abs_file_path(file_name)
    if not os.path.isfile(file_path):
        LOGGER.debug(f'{file_path} created')
    if force_pickle or get_file_ending(file_path) in ["pickl"]:
        with open(file_path, 'wb') as file:
            pickle.dump(obj=data, file=file)
            return
    with open(file_path, 'w') as file:
        if get_file_ending(file_path) == 'json':
            json.dump(data, file, cls=EnhancedJSONEncoder)
        elif get_file_ending(file_path) == 'csv' and isinstance(data, list) and data and isinstance(data[0], list):
            writer = csv.writer(file)
            writer.writerows(data)
        else:
            file.write(str(data))
    LOGGER.debug(f'saved {file_name}')


def append_to_file(file_name: str, data: Any, is_abs: bool = False, force_pickle: bool = False) -> bool:
    ok: bool = True
    if not check_if_file_exists(file_name, is_abs=is_abs):
        save_file(file_name=file_name, data=data, is_abs=is_abs, force_pickle=force_pickle)
        return ok

    content = load_file(filename=file_name, is_abs=is_abs, force_pickle=force_pickle)
    if isinstance(content, list):
        if isinstance(data, list):
            content.extend(data)
        else:
            content.append(data)
    if isinstance(content, str):
        content += f'\n{data}'
    if isinstance(content, dict) and isinstance(data, dict):
        try:
            content.update(data)
        except Exception as e:
            LOGGER.warning(f'{e.__class__.__name__} occurred while trying to append to file {file_name}: {e}')
            ok = False
    save_file(file_name=file_name, data=content, force_pickle=force_pickle)
    return ok


def get_files_in_dir(dirname: str, endings: List[str] = None, recursive: bool = False, is_abs: bool = False) \
        -> Optional[List[str]]:
    """
    :param dirname: the directory name or path specific to project root
    :param is_abs: determines if the given path is absolute or relative to project root
    :param endings: A optional list of str with str. Only file that end with one of the endings will be returned
                    endings e.g.: "png", "txt", "css" so NOT ".css"
    :param recursive: if files in subfolders should be returned
    :return: a list with filepaths
    """
    if not check_if_dir_exists(dirname):
        LOGGER.info(f"Directory {dirname} doesnt exist in {to_abs_file_path('')}")
        return

    if not endings:
        endings = ["*"]
    dir_path: str = dirname if is_abs else to_abs_file_path(dirname)
    files: List[str] = []
    for ending in endings:
        files.extend(glob.glob(dir_path + f'{"/**" if recursive else ""}/*.{ending}', recursive=recursive))
    return files


def load_file(filename: str, is_abs: bool = False, force_pickle: bool = False) -> any:
    """
    loads contents of a file.
    If the file ends with `json` or `pickl` it tries to load it with these formats
    :param force_pickle: if True, the file gets saved with pickl
    :param filename: the path to the file to load
    :param is_abs: determines if the given path is absolute or relative to project root
    :return:
    """
    file_path: str = filename if is_abs else to_abs_file_path(filename)
    if not check_if_file_exists(file_path):
        return None
    if force_pickle or get_file_ending(file_path) in ["pickl"]:
        with open(file_path, 'rb') as file:
            try:
                return pickle.load(file)
            except EOFError:
                return None
    with open(file_path, 'r') as stream:
        if get_file_ending(file_path) == 'json':
            try:
                return json.load(stream)
            except json.JSONDecodeError as exc:
                LOGGER.error(f'JSON parsing error: {exc}')
                return None

        else:
            return stream.read()


def get_file_base(filepath: str) -> str:
    """
    :param filepath: the absolute filepath
    :return: the filename / filebase
    """
    return os.path.basename(filepath)


def get_file_ending(filepath: str) -> str:
    """
    :param filepath: the path to a file
    :return: the ending of a file
    """
    return filepath.split(".")[-1]


def get_file_without_ending(filepath: str) -> str:
    """
    "file.txt" -> "file"
    :param filepath:
    :return:
    """
    if not get_file_ending(filepath):
        return filepath
    splitted = filepath.split(".")
    splitted.pop(-1)
    return ".".join(splitted)


def create_zip(archive_name: str, files: Union[str, List[str]], is_abs: bool = False, force: bool = False) -> bool:
    """
    Creates a zip archive with "<archive_name>.zip" and compresses all files Listed in files.
    :param force: whether an existing archive should be overwritten
    :param archive_name: the name of the archive. If it does not end with ".zip" it will be appended
    :param files: a list of file paths to include.
    :param is_abs: if the [archive_name] is an absolute path
    :return: bool of success
    """
    archive_name = archive_name if get_file_ending(archive_name) == "zip" else f"{archive_name}.zip"
    archive_name = archive_name if is_abs else to_abs_file_path(archive_name)

    if check_if_file_exists(archive_name):
        if force:
            LOGGER.debug(f"{archive_name} already exists. Overwriting...")
        else:
            LOGGER.debug(f"{archive_name} already exists. Aborting. "
                         f"You can force a overwrite by setting 'force' = True")
            return False

    zip_file = zipfile.ZipFile(archive_name, "w")
    files = [files] if isinstance(files, str) else files
    for file in files:
        filepath = _greedy_file_resolution(file)
        if not filepath:
            LOGGER.info(f"Could not include {file} to {archive_name}: File not found")
            continue
        zip_file.write(filepath)
    zip_file.close()
    LOGGER.debug(f"{archive_name} created")
    return True


def append_to_zip(archive_name: str, files: Union[str, List[str]], is_abs: bool = False,
                  create_if_not_exist: bool = True) -> bool:
    """
    Appends files to an archive
    :param archive_name: the name of the archive. If it does not end with ".zip" its appended
    :param files: a list of file paths to append
    :param is_abs: if the archive_name is absolute path or root relative
    :param create_if_not_exist: if set, the archive will be created if non exist
    :return: bool of success
    """
    archive_name = archive_name if get_file_ending(archive_name) == "zip" else f"{archive_name}.zip"
    archive_name = archive_name if is_abs else to_abs_file_path(archive_name)

    if not check_if_file_exists(archive_name):
        if create_if_not_exist:
            return create_zip(archive_name, is_abs=True, files=files)
        else:
            LOGGER.info(f"Could not append to {archive_name}: does not exist.")
            return False

    zip_file = zipfile.ZipFile(archive_name, "a")
    files = [files] if isinstance(files, str) else files
    for file in files:
        filepath = _greedy_file_resolution(file)
        if not filepath:
            LOGGER.info(f"Could not include {file} to {archive_name}: File not found")
            continue
        zip_file.write(filepath)
    zip_file.close()
    LOGGER.debug(f"{archive_name} created")
    return True
