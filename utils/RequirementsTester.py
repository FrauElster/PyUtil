import distutils.text_file
import unittest
from pathlib import Path

import pkg_resources

_REQUIREMENTS_PATH = Path(__file__).parent.with_name("requirements.txt")


class RequirementsTester(unittest.TestCase):
    """Test availability of required packages."""

    def test_requirements(self):
        """Test that each required package is available."""
        # Ref: https://stackoverflow.com/a/45474387/
        requirements = distutils.text_file.TextFile(filename=str(_REQUIREMENTS_PATH)).readlines()
        for requirement in requirements:
            with self.subTest(requirement=requirement):
                pkg_resources.require(requirement)
