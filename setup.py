import os
from setuptools import setup, find_packages
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='DANE',
    version='0.2.12',
    author='Nanne van Noord',
    author_email='n.j.e.vannoord@uva.nl',
    url='https://github.com/CLARIAH/DANE',
    description='Utils for working with the Distributed Annotation and Enrichment system.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='Apache License 2.0',

    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Multimedia :: Video",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],

    packages=find_packages(exclude=('test',)),

    install_requires=[
      'yacs',
      'pika',
      'elasticsearch'
    ])
