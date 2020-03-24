import logging
import os
from datetime import datetime

from .filehandler import check_if_dir_exists, to_abs_file_path, check_if_file_exists, rename_file, get_file_base, \
    get_file_ending, append_to_zip, get_file_without_ending, delete_file


def _archive_log(log_file: str, archive_name: str):
    if not check_if_file_exists(log_file):
        print("hello")
        return

    new_log_file: str = f"{get_file_without_ending(log_file)}_{datetime.now().strftime('%Y.%m.%d_%H:%M:%S')}." \
                        f"{get_file_ending(log_file)}"
    rename_file(old_filename=log_file, new_filename=new_log_file)

    append_to_zip(archive_name, files=new_log_file)
    delete_file(new_log_file)


def setup_logger(logger_name: str, log_file: str = None, log_dir: str = None, log_file_debug: str = None,
                 archive_name: str = None, level: str = None) -> None:
    """
    Setup logger
    :param level: the loglevel. Has to be one of [`DEBUG`, `INFO`, `WARNING`, `ERROR`].
    If not provided it default to `INFO`
    :param logger_name: the name of the logger. This should be globally accessible variable.
    Get the logger by `logging.getLogger(<logger_name>)` anywhere in the code
    :param log_file: the filename of the log. defaults to [logger_name].[level].log
    :param log_dir: the log directory. If not provided project_root is assumed
    :param log_file_debug: the file for debug logging. If not provided, none will be created
    :param archive_name: Creates an archive of old logs. These will be signed with the date, the next log takes place.
    Or with other words the date till they held information. If not provided none will be created
    :return: None
    """
    logger: logging.Logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    if not level:
        level = logging.INFO
    elif level.lower() == "debug":
        level = logging.DEBUG
    elif level.lower() == "warning":
        level = logging.WARNING
    elif level.lower() == "error":
        level = logging.ERROR
    else:
        logger.info(f"Did not recognize loglevel {level}. Defaulting to `INFO`")
        level = logging.INFO

    formatter = logging.Formatter('%(levelname)s \t|%(asctime)s \t| %(name)s \t|  %(message)s')

    if log_dir and not check_if_dir_exists(log_dir):
        os.mkdir(to_abs_file_path(log_dir))

    log_file = log_file if log_file else f"{logger_name}.{logging._levelToName[level].lower()}.log"
    log_file = os.path.join(log_dir, log_file) if log_dir else log_file
    if archive_name:
        archive_name = os.path.join(log_dir, archive_name) if log_dir else archive_name
        _archive_log(log_file, archive_name)
    file_handler: logging.FileHandler = logging.FileHandler(to_abs_file_path(log_file), mode='w')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if log_file_debug:
        log_file_debug = log_file_debug if log_file_debug else f"{logger_name}.debug.log"
        log_file_debug = os.path.join(log_dir, log_file_debug) if log_dir else log_file_debug
        if archive_name:
            _archive_log(log_file_debug, archive_name)
        debug_file_handler: logging.FileHandler = logging.FileHandler(to_abs_file_path(log_file_debug), mode='w')
        debug_file_handler.setLevel(logging.DEBUG)
        debug_file_handler.setFormatter(formatter)
        logger.addHandler(debug_file_handler)

    console_handler: logging.StreamHandler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    logger.addHandler(console_handler)

    logger.info('Filehandler and Consolehandler were born, let\'s start logging')
