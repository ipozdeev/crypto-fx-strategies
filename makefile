.PHONY data_dir_layout:
	mkdir -p data/raw/spot/kraken data/raw/perpetual/kraken data/prepared/spot/kraken data/prepared/perpetual/kraken data/prepared/funding/kraken


.PHONY: clean_macosx save_data prepare_data

clean_macosx:
	find data -type f -name "*.zip" -exec zip -d {} "__MACOSX/*" \;

save_data:
	python src/organize_data.py

prepare_data: clean_macosx save_data