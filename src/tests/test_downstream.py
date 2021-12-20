import os
from unittest import TestCase

from src.setup import *
from src.datafeed_.kraken.downstream import *


class TestKraken(TestCase):
    def test_prepared_files(self):
        """Prepared data needed to replicate findings exists where it must."""
        fs = {"funding": "funding-r-kraken.ftr",
              "spot": "spot-close-kraken.ftr",
              "perpetual": "perp-bidask-kraken.ftr"}

        pth = os.path.join(os.environ.get("PROJECT_ROOT"), "data/prepared")

        self.assertTrue(
            all([f_ in os.listdir(os.path.join(pth, p_, "kraken"))
                 for p_, f_ in fs.items()])
        )
