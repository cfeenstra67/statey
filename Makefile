
install-deps:
	@pip install -r requirements.txt -r requirements-tests.txt


test:
	@py.test -vv tests

fmt:
	@black statey tests --line-length=88
# 	@pylint statey

check:
	@black statey tests --line-length=88 --fail

part:=patch

bumpversion:
	bump2version --verbose $(part)

clean:
	@pip uninstall -y statey
	@rm -rf build/ dist/ *.egg-info .eggs

build: clean
	@python setup.py sdist bdist_wheel

pypi-check:
	twine check dist/*

pypi-upload-test:
	twine upload --repository-url https://test.pypi.org/legacy/ dist/*

pypi-upload:
	twine upload dist/*
