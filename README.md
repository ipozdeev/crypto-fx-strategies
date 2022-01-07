# fx strategies in cryptocurrency space

(how) do strategies like carry and momentum work in the crypto universe?

go to [walkthrough](./walkthrough.ipynb) for results.

![crypto carry performance](output/figures/carry-pnl.png "crypto carry performance")

## requirements
there are some tests which will be passed once the rest of this section has been 
dealt with:
```commandline
python -m unittest discover
```

**first**, environment variable `PROJECT_ROOT` must point to the project folder; 
you can set it in the .env file, and python will rely on `python-dotenv` to set it.

**second**, the necessary virtual environment can be created from `requirements.txt`: 
```commandline
python3 -m venv pyenv; source pyenv/bin/activate; pip install -r requirements.txt
```
to create a virtual environment in `$PROJECT_ROOT/pyenv` and install all packages;
please don't forget to activate it every time!

**third**, package [foolbox](https://github.com/ipozdeev/foolbox) must be downloaded to where 
python can find it.

**fourth**, a certain data folder layout must be adhered to; you can create it with
```commandline
make data_dir_layout
```

**fifth**, you have to download 
[spot](https://support.kraken.com/hc/en-us/articles/360047124832-Downloadable-historical-OHLCVT-Open-High-Low-Close-Volume-Trades-data) 
and [perpetual futures](https://support.kraken.com/hc/en-us/articles/360022835871-Historical-Data) 
prices from Kraken and place them into `data/raw/spot/kraken/` and `data/raw/perpetual/kraken/` respectively; 
after this the following should work when run in the command line:
```commandline
python src/organize_data.py
```

this will create several .ftr (feather) data files in `data/prepared/spot(perpetual)/kraken/` 
that are used by functions from `src.datafeed_.kraken.downstream`
 