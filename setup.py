#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='pubsub_pipeline',
    version='0.0.1',
    description='Easy implementation of python enrichment pipelines '
                'based on google cloud pubsub',
    author='Sune Debel',
    author_email='sad@hypefactors.com',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    license='LICENSE.txt',
    install_requires=['google-cloud-pubsub==0.39.1']
)
