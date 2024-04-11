#!/usr/bin/env python
# -*- coding: utf-8 -*-
import glob
import pathlib

from setuptools import find_packages
from setuptools import setup

here = pathlib.Path(__file__).parent.resolve()


setup(
    name="midia_fetcher",
    version="0.0.1",
    description="Framework for fetching of MIDIA data",
    #    long_description=(here / 'README.md').read_text(encoding='utf-8'),
    #    long_description_content_type='text/markdown',
    url="https://github.com/midiaIDorg/midia_fetcher",
    author="Michał Piotr Startek, Mateusz Krzysztof Łącki",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    keywords="DIA TimsTOF",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires="toml".split(),
    scripts=glob.glob("tools/midia_fetch.py"),
    package_dir={"midia_tester": "midia_tester"},
    package_data={"midia_tester": ["configs/default.toml"]},
)
