#!/usr/bin/env python

from setuptools import setup, find_packages


with open('requirements.txt') as f:
	# Load one dependency per line, ignoring comments
	requirements = [
		line.strip()
		for line in f.read().strip().split('\n')
		if not line.strip().startswith('#')
	]


with open('requirements-tests.txt') as f:
	# Load one dependency per line, ignoring comments
	requirements_tests = [
		line.strip()
		for line in f.read().strip().split('\n')
		if not line.strip().startswith('#')
	]

with open('requirements-dev.txt') as f:
	# Load one dependency per line, ignoring comments
	requirements_dev = [
		line.strip()
		for line in f.read().strip().split('\n')
		if not line.strip().startswith('#')
	]

with open(os.path.join(CURRENT_DIR, 'README.rst')) as f:
    long_description = f.read().strip()

extra_reqs = {
	'fmt': ['fmt==0.3.1'],
	'pickle': [
		'dill==0.3.2',
		'cloudpickle==1.4.1'
	],
	'cli': [
		'click==7.1.2',
		'asciidag @ git+https://github.com/cfeenstra67/asciidag.git'
	],
	'pulumi': [
		'pylumi==1.2.0',
		'jsonschema==3.2.0'
	],
}

extra_reqs['all'] = sum(extra_reqs.values(), [])
extra_reqs['base'] = extra_reqs['fmt'] + extra_reqs['cli']
extra_reqs['tests'] = requirements_tests
extra_reqs['dev'] = requirements_dev

setup(
	name='statey',
	version='0.0.2',
	description='Graph-based provisioning framework.',
	long_description=long_description,
	long_description_content_type='text/x-rst',
	classifiers=[
        'Development Status :: 1 - Planning',
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
	],
	license='MIT',
	author='Cam Feenstra',
	author_email='cameron.l.feenstra@gmail.com',
	packages=find_packages(exclude=('tests', 'tests.*')),
	install_requires=requirements,
	extras_require=extra_reqs,
	entry_points={
		'console_scripts': [
			'statey=statey.cli.__main__:main'
		]
	},
	url='https://github.com/cfeenstra67/statey',
    package_data={
        '': [
            'requirements.txt',
            'requirements-dev.txt',
            'requirements-tests.txt',
            'README.rst',
        ],
    },
    include_package_data=True
)
