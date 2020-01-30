Installation instructions
======================================

In the current stage of development we recommend to install DANE as a local pip package. Eventually we
expect it to be available through PyPi.

.. code:: bash

    git clone https://github.com/CLARIAH/DANE.git
    cd DANE/
    pip install -e .

The -e flag indicates that it is an editable installation, i.e. if you git pull a new version or modify the code 
this is directly reflected in the installed module.
