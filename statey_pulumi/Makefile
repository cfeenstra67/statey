

build-go:
	@mkdir -p build/go
	@cd go &&\
	go build -o ../build/go/libstalumigo.so -buildmode=c-shared connector/main.go
	@install_name_tool -id @rpath/libstalumigo.so build/go/libstalumigo.so


build-python:
	python setup.py build_ext

install-python:
	python setup.py install

clean:
	@pip uninstall -y stalumi
	@rm -rf build/ dist/ _stalumi.c *.egg-info .eggs

build: clean build-go build-python

install: clean build-go install-python
