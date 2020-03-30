import os
from setuptools import setup, find_packages

setup(name='DANE',
    version='0.1',
    description='Utils for working with the Distributed Annotation and Enrichment system.',
    url='https://github.com/CLARIAH/DANE',
    author='Nanne van Noord',
    author_email='n.j.e.vannoord@uva.nl',

    packages=find_packages(exclude=('test',)),

    install_requires=[
      'yacs',
      'pika',
    ])
