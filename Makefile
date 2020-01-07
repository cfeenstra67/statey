
install-deps:
	@pip install -r requirements.txt -r requirements-tests.txt


test:
	@py.test -vv tests

check:
	@black statey tests --line-length=88
	@pylint statey


part:=patch

bumpversion:
	bump2version --verbose $(part)
