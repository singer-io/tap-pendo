#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-pendo",
    version="0.0.1",
    description="Singer.io tap for extracting data",
    author="wclark@agari.com",
    url="https://github.com/jwalterclark/tap-pendo",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_pendo"],
    install_requires=[
        'singer-python==5.2.1',
        "requests",
        'backoff==1.3.2'
    ],
    entry_points="""
    [console_scripts]
    tap-pendo=tap_pendo:main
    """,
    packages=["tap_pendo"],
    package_data={
        "schemas": ["tap_pendo/schemas/*.json"]
    },
    include_package_data=True,
)
