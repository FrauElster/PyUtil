import logging
import os
import sys

from .RequirementsTester import RequirementsTester
from .filehandler import to_abs_file_path, check_if_file_exists

LOGGER: logging.Logger = logging.getLogger(__name__)

PROJECT_DIR = to_abs_file_path("")


def activate_venv():
    activate_this = os.path.join(PROJECT_DIR, 'venv/bin', 'activate_this.py')
    if not check_if_file_exists(activate_this):
        LOGGER.debug(f"{activate_this} not found. Could not activate virtual envirenment.\n"
                       f"You should run the following commands:\n\t"
                       f"'chmod +x {PROJECT_DIR}/scripts/setup.sh'\n\t"
                       f"'{PROJECT_DIR}/scripts/setup.sh'")
        RequirementsTester().test_requirements()
    else:
        with open(activate_this) as f:
            exec(f.read(), {'__file__': activate_this})

    LOGGER.debug(f'venv is {"" if hasattr(sys, "real_prefix") else "not "}activated')

