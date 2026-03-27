SHELL:=$(shell which bash)

clean-build-folder:
	rm -fr  ./dist/ ./build/ ./*.egg-info

build-pip-package: clean-build-folder
	DIR=/tmp/test
	rm -rf $DIR
	mkdir -p $DIR
	mv ./redis_streams_test $DIR/  # not the best, but setup exclude doesn't work
	find ./ -name "__pycache __" -exec rm -rf {} \;
	python3 setup.py sdist bdist_wheel
	mv $DIR/redis_streams_test ./
	tar -tvf ./dist/redis-streams-*.tar.gz

upload-test-pip-package:
	python3 -m twine upload --repository testpypi dist/* --verbose

upload-pip-package:
	python3 -m twine upload --repository pypi dist/* --verbose

lint:
	flake8 redis_streams/
	black redis_streams/
	mypy --install-types --non-interactive redis_streams/
	mypy --explicit-package-bases --namespace-packages redis_streams/
	vulture --min-confidence 100 redis_streams/
