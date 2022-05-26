build:
	rm -rf ./dist && mkdir ./dist
	cp ./main.py ./dist
	zip -r dist/new_transformer.zip new_transformer