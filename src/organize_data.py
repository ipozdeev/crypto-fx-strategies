import logging
from setup import *
from datafeed_.kraken.upstream import *


logging.basicConfig(format='%(asctime)s: %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO,
                    handlers=[logging.StreamHandler()])


if __name__ == '__main__':
    save_spot_from_ohlcv()
    save_perpetual_from_csv()
    save_funding_rates()
