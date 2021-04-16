#!/usr/bin/env python
# coding: utf-8

from setuptools import find_packages
from setuptools import setup

setup(
    name='ml-deployment',
    version='0.1',
    author='Aditya',
    install_requires=["numpy", "pandas", "google-cloud-storage", "scikit-learn", "joblib"],
    packages=find_packages(exclude=['data']),
    description='Dataflow sklearn Batch',
    url=''
)