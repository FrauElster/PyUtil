import logging
import os

from .filehandler import check_if_dir_exists, to_abs_file_path

LOGGER: logging.Logger = logging.getLogger(__name__)


def setup_logger(log_file: str, log_dir: str = None, log_file_debug: str = None) -> None:
    """
    setup for the various handler for logging
    :return:
    """
    log_file = os.path.join(log_dir, log_file) if log_dir else log_file

    LOGGER.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(levelname)s \t|%(asctime)s \t| %(name)s \t|  %(message)s')

    if log_dir and not check_if_dir_exists(log_dir):
        os.mkdir(to_abs_file_path(log_dir))

    file_handler: logging.FileHandler = logging.FileHandler(to_abs_file_path(log_file), mode='w')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)

    if log_file_debug:
        debug_file_handler: logging.FileHandler = logging.FileHandler(to_abs_file_path(log_file), mode='w')
        debug_file_handler.setLevel(logging.DEBUG)
        debug_file_handler.setFormatter(formatter)
        LOGGER.addHandler(debug_file_handler)

    console_handler: logging.StreamHandler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    LOGGER.addHandler(console_handler)

    LOGGER.info('Filehandler and Consolehandler were born, let\'s start logging')
