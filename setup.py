#!/usr/bin/env python

from setuptools import setup, find_packages


with open('requirements.txt') as f:
	# Load one dependency per line, ignoring comments
	requirements = [
		line.strip()
		for line in f.read().strip().split('\n')
		if not line.strip().startswith('#')
	]


setup(
	name='statey',
	version='0.0.1',
	description='Graph-based provisioning framework.',
	author='Cam Feenstra',
	author_email='cameron.l.feenstra@gmail.com',
	packages=find_packages(exclude=('tests', 'tests.*')),
	install_requires=requirements
)
