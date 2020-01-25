#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-pendo",
    version="0.0.1",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_pendo"],
    install_requires=[
        'singer-python',
        "requests",
        'backoff==1.8.0'
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
