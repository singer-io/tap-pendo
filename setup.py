#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-pendo",
    version="0.0.15",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="https://github.com/singer-io/tap-pendo",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_pendo"],
    install_requires=[
        'singer-python==5.2.1',
        "requests",
        'pyhumps==1.3.1',
        'backoff==1.3.2',
        'ijson==3.1.4',
    ],
    extras_require={
        'dev': [
            'ipdb==0.11',
            'pylint==2.5.3',
        ]
    },
    entry_points="""
    [console_scripts]
    tap-pendo=tap_pendo:main
    """,
    packages=["tap_pendo"],
    package_data={
        "schemas": ["tap_pendo/schemas/*.json"],
        "schemas/shared": ["tap_pendo/schemas/shared/*.json"]
    },
    include_package_data=True,
)
