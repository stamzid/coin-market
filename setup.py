# -*- coding: utf-8 -*-

# Learn more: https://github.com/stamzid/coin-market/setup.py

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='coinmarket',
    version='1.0.0',
    description='Sample ETL pipeline for capturing OHLCV Metrics from coinmarketcap',
    long_description=readme,
    author='Syed Tamzid',
    author_email='tamzid2004@gmail.com',
    url='https://github.com/stamzid/coin-market',
    license=license,
    packages=find_packages(exclude='tests')
)