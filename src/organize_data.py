from setup import *
from datafeed_.kraken.upstream import *


if __name__ == '__main__':
    save_spot_from_ohlcv()
    save_perpetual_from_csv()
    save_funding_rates()
