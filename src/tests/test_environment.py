import os
import sys
from unittest import TestCase

from src.config import *


class TestEnvironment(TestCase):
    def test_environment_vars(self):
        """Env var 'PROJECT_ROOT' is set."""
        self.assertIsNotNone(os.environ.get("PROJECT_ROOT", None))

    def test_foolbox_loaded(self):
        """Module 'foolbox' can be loaded."""
        try:
            import foolbox
        except ModuleNotFoundError:
            pass
        self.assertTrue("foolbox" in sys.modules)
