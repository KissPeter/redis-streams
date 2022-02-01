SHELL:=$(shell which bash)

clean-build-folder:
	rm -fr  ../dist/ ../build/ ../*.egg-info

build-pip-package: clean-build-folder
	DIR=/tmp/test
	rm -rf $DIR
	mkdir -p $DIR
	mv ./redis_streams_test $DIR/  # not the best, but setup exclude doesn't work
	find ./ -name "__pycache __" -exec rm -rf {} \;
	&& python3 setup.py sdist bdist_wheel
	tar -tvf ./dist/redis-streams-*.tar.gz
	mv $DIR/redis_streams_test ./

upload-test-pip-package:
	cd .. && python3 -m twine upload --repository testpypi dist/* --verbose

upload-pip-package:
	cd .. && python3 -m twine upload --repository pypi dist/* --verbose
