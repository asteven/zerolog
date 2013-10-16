from setuptools import setup, find_packages

name = 'zerolog'
version = '0.1.0'

setup(
    name=name,
    version=version,
    author='Steven Armstrong',
    author_email='steven-%s@armstrong.cc' % name,
    url='http://github.com/asteven/%s/' % name,
    description='Distributed log stream collecting and logger configuration system for python',
    packages=find_packages(),
)
