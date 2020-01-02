
install-deps:
	@pip install -r requirements.txt -r requirements-tests.txt


test:
	@py.test -vv tests
