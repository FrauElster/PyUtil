############### requirements ###############
#
# tika
# pandas
# fpdf
#
############################################
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

import pandas as pandas
from fpdf import FPDF
from pandas import DataFrame
from tika import parser

LOGGER = logging.getLogger(__name__)
PROJECT_ROOT_RELATIVE_TO_THIS_FILE: str = os.path.join("..", "..")


@dataclasses.dataclass
class Table:
    column_names: List[str]
    column_values: List[List[str]]
    name: Optional[str] = None

    def as_csv(self, filename: str) -> str:
        """
        Saves the table as csv into FILENAMES.DOWNLOAD_DIR/filename.csv
        :param filename: if set the csv is stored
        into filename.csv, if not and the table has a name, its stored into name.csv else its just a random string
        :return: the filename it is stored to within the FILENAMES.DOWNLOAD_DIR
        """
        filename = filename if get_file_ending(filename) == "csv" else f"{filename}.csv"
        data = [self.column_names]
        data.extend(self.column_values)
        save_file(file_name=filename, data=data)
        return filename


class CsvEncoder:
    @staticmethod
    def encode(o: Any) -> Table:
        if isinstance(o, Table):
            return o

        if isinstance(o, DataFrame):
            return Table(column_names=o.columns.values, column_values=list(map(lambda col: list(col), o.values)))

        column_names: List[str] = []
        column_values: List[List[Union[str, float, int, bool]]] = []

        data = serialize(o)
        if isinstance(data, dict):
            column_names = list(data.keys())
            column_values.append(list(data.values()))
        elif isinstance(data, list):
            if isinstance(data[0], dict):
                column_names = list(data[0].keys())
            elif isinstance(data[0], list):
                column_names = data.pop(0)

            for column_value in data:
                if isinstance(column_value, dict):
                    column_values.append(list(column_value.values()))
                elif isinstance(column_value, list):
                    column_values.append(column_value)
                else:
                    msg: str = f"Could not encode row {column_value} to csv"
                    LOGGER.warning(msg)
                    raise ValueError(msg)

        if not column_values or not column_names:
            msg: str = f"Could not encode {data} to csv: no values"
            LOGGER.warning(msg)
            raise ValueError(msg)

        return Table(column_names, column_values)


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        if isinstance(o, datetime.datetime):
            return o.strftime("%H:%M:%S %Y.%m.%d")
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


def to_rel_file_path(abs_path: str) -> str:
    package_directory = os.path.dirname(os.path.abspath(__file__))
    return os.path.relpath(abs_path, os.path.join(package_directory, PROJECT_ROOT_RELATIVE_TO_THIS_FILE))


def to_abs_file_path(file_name: str) -> str:
    """ :returns an existing absolute file path based on the project root directory + file_name"""
    package_directory = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(package_directory, PROJECT_ROOT_RELATIVE_TO_THIS_FILE, file_name)
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


def save_file(file_name: str, data: Any, is_abs: bool = False, raw: bool = False) -> None:
    """
    Writes data to a file. If a file already exists, it gets overwritten

    If [file_name] ends with ".json" it will serialize the data and store it in json format.
    If [file_name] ends with ".pickl" it will pickl the data
    If [file_name] ends with ".csv" and is a Table or a dataclass / list of dataclasses it will write a csv file.

    :param file_name: the name of the file
    :param data: the data to write
    :param is_abs: if [file_name] is an absolute path
    :param raw: if the file ending is ignored and the data just gets saved without any special treatment
    :return:
    """
    file_path: str = file_name if is_abs else to_abs_file_path(file_name)
    if not os.path.isfile(file_path):
        LOGGER.debug(f'{file_path} created')

    if raw:
        with open(file_path, 'w') as file:
            file.write(data)
            return

    if get_file_ending(file_path) in ["pickl"]:
        with open(file_path, 'wb') as file:
            if get_file_ending(file_path) == "pickl":
                pickle.dump(obj=data, file=file)
            else:
                file.write(data)
            return

    with open(file_path, 'w') as file:
        if get_file_ending(file_path) == 'json':
            try:
                json.dump(data, file, cls=EnhancedJSONEncoder)
                return
            except TypeError as e:
                LOGGER.warning(f"Could not encode {file_name}:\n{e.__class__.__name__}: {e}")
        elif get_file_ending(file_path) == 'csv':
            try:
                table: Table = CsvEncoder.encode(data)
                _data = [table.column_names]
                _data.extend(table.column_values)
                writer = csv.writer(file)
                writer.writerows(_data)
                return
            except ValueError:
                pass
        elif get_file_ending('pdf'):
            if isinstance(data, str):
                pdf = FPDF('P', 'mm', 'A4')
                pdf.set_font('Arial')
                pdf.add_page()
                pdf.set_font_size(21)
                pdf.multi_cell(150, 10, txt=data)
                pdf.output(file_path)
                return

        file.write(str(data))
    LOGGER.debug(f'saved {file_name}')


def append_to_file(file_name: str, data: Any, is_abs: bool = False, raw: bool = False) -> bool:
    ok: bool = True
    if not check_if_file_exists(file_name, is_abs=is_abs):
        save_file(file_name=file_name, data=data, is_abs=is_abs, raw=raw)
        return ok

    content = load_file(filename=file_name, is_abs=is_abs, raw=raw)
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
    save_file(file_name=file_name, data=content, raw=raw)
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


def load_file(filename: str, is_abs: bool = False, raw: bool = False) -> any:
    """
    loads contents of a file.

        - If the file ends with `json` it loads it up as Dict[str, Any]
        - If the file ends with `pickl` it loads it up as as pyobject
        - If the file ends with `csv` it loads it up as pandas.DataFrame
        - If the file ends with `pdf` it loads it up as str

    :param raw: if True, the file will just be opened and the content returned, without any file
    ending specific processing steps
    :param filename: the path to the file to load
    :param is_abs: determines if the given path is absolute or relative to project root
    :return:
    """
    file_path: str = filename if is_abs else to_abs_file_path(filename)
    if not check_if_file_exists(file_path):
        return None

    if raw:
        with open(file_path, 'r') as stream:
            return stream.read()

    # binary needed
    if get_file_ending(file_path) in ["pickl"]:
        with open(file_path, 'rb') as file:
            if get_file_ending(file_path) == "pickl":
                try:
                    return pickle.load(file)
                except EOFError:
                    return None

            return file.read()

    # none binary
    with open(file_path, 'r') as stream:
        if get_file_ending(file_path) == 'json':
            try:
                return json.load(stream)
            except json.JSONDecodeError as exc:
                LOGGER.error(f'JSON parsing error: {exc}')
                return None
        elif get_file_ending(file_path) == "csv":
            return pandas.read_csv(stream)
        elif get_file_ending(file_path) == "pdf":
            raw = parser.from_file(file_path)
            if raw['content']:
                return raw['content']
            else:
                LOGGER.debug(f'"{file_path}" was not decodable, consider using ocr (https://github.com/FrauElster/pdf_reader) gives an example')
                return None

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


def extract_zip(archive_name: str, dir_name: str = None, is_abs: bool = False) -> Optional[List[str]]:
    """
    Extracts all files of [archive_name] into [dir_name] and returns a list of all file_paths
    :param archive_name: the name of the zip file
    :param dir_name: the name of the dir to extract into
    :param is_abs: if the specified paths are absolute
    :return: None on failure else a list of all extracted file paths
    """
    archive_name = archive_name if get_file_ending(archive_name) == "zip" else f"{archive_name}.zip"
    archive_name = archive_name if is_abs else to_abs_file_path(archive_name)
    if not check_if_file_exists(archive_name):
        LOGGER.info(f"Could not extract {archive_name}: file not found")
        return

    if dir_name:
        dir_name = dir_name if is_abs else to_abs_file_path(dir_name)
    else:
        dir_name = get_file_without_ending(archive_name)

    zip_file = zipfile.ZipFile(archive_name)
    zip_file.extractall(path=dir_name)
    zip_file.close()
    return get_files_in_dir(dir_name, is_abs=True, recursive=True)


def extract_file_from_zip(archive_name: str, file_name: str = None, is_abs: bool = False) -> Optional[Any]:
    """
    Extracts [file_name] from a [archive_name] and returns its content
    :param archive_name: the name of the zip file
    :param file_name: the name of the file to extract
    :param is_abs: if the specified path is absolute
    :return: None on failure. Else the content of the extracted file
    """
    archive_name = archive_name if get_file_ending(archive_name) == "zip" else f"{archive_name}.zip"
    archive_name = archive_name if is_abs else to_abs_file_path(archive_name)
    if not check_if_file_exists(archive_name):
        LOGGER.info(f"Could not extract {archive_name}: file not found")
        return

    path_splitted: List[str] = archive_name.split(os.path.sep)
    path_splitted.pop(-1)
    path = os.path.sep.join(path_splitted)
    zip_file = zipfile.ZipFile(archive_name)
    zip_file.extract(file_name, path=path)

    return load_file(os.path.join(path, file_name), is_abs=True)
