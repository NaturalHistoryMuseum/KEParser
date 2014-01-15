from setuptools import setup
import sys, os

version='0.1.0'

setup(
    name='KEParser',
    version=version,
    author='Ben Scott',
    author_email='ben@benscott.co.uk',
    packages=['keparser'],
    license='LICENSE.txt',
    description='Parse KEEMU key=value export files.',
    long_description=open('README.txt').read(),
    install_requires=[
        "pyyaml == 3.10",
    ],
)