# DANE-utils
The Distributed Annotation 'n' Enrichment (DANE) system handles compute job assignment and file storage for the automatic annotation of content available in the [MediaSuite](https://mediasuite.clariah.nl/).

This repository contains utils which improve the ease of working with DANE, for things such as creating custom analysis workers or submitting new jobs. Code is still in development, and largely untested.

## Installation
Install as a import-able pip package. `-e` flag indicates that it is an editable installation, i.e. if you pull a new version or modify the code this is directly usable.

    git clone https://github.com/CLARIAH/DANE-utils.git
    cd DANE-utils/
    pip install -e .

### Usage
Examples for how to use these utils can be found in the `examples/` directory.
