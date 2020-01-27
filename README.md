# DANE
The Distributed Annotation 'n' Enrichment (DANE) system handles compute job assignment and file storage for the automatic annotation of content.

This repository contains contains the building blocks for with DANE, such as creating custom analysis workers or submitting new jobs. Code is still in development, and somewhat untested.

## Installation
Install as a import-able pip package. `-e` flag indicates that it is an editable installation, i.e. if you pull a new version or modify the code this is directly usable.

    git clone https://github.com/CLARIAH/DANE.git
    cd DANE/
    pip install -e .

### Usage
Examples for how to use the components can be found in the `examples/` directory.
