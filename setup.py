#!/usr/bin/env python

from setuptools import setup, find_packages


with open('requirements.txt') as f:
	# Load one dependency per line, ignoring comments
	requirements = [
		line.strip()
		for line in f.read().strip().split('\n')
		if not line.strip().startswith('#')
	]

extra_reqs = {
	'fmt': ['fmt==0.3.1'],
	'pickle': [
		'dill==0.3.2',
		'cloudpickle==1.4.1'
	],
	'cli': [
		'click==7.1.2',
		'git+https://github.com/cfeenstra67/asciidag.git'
	],
	'pulumi': [
		'pylumi==1.2.0',
		'jsonschema==3.2.0'
	]
}

extra_reqs['all'] = sum(extra_reqs.values(), [])

setup(
	name='statey',
	version='0.0.2',
	description='Graph-based provisioning framework.',
	author='Cam Feenstra',
	author_email='cameron.l.feenstra@gmail.com',
	packages=find_packages(exclude=('tests', 'tests.*')),
	install_requires=requirements,
	extras_require=extra_reqs
)
