from setuptools import setup, find_packages
from os import path
import time

if path.exists("VERSION.txt"):
    # this file can be written by CI tools (e.g. Travis)
    with open("VERSION.txt") as version_file:
        version = version_file.read().strip().strip("v")
else:
    version = str(time.time())

setup(
    name='open-bus-stride-etl',
    version=version,
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'open-bus-stride-etl = open_bus_stride_etl.cli:main',
        ]
    },
)
