# DANE
The Distributed Annotation 'n' Enrichment (DANE) system handles compute task assignment and file storage for the automatic annotation of content.

This repository contains contains the building blocks for with DANE, such as creating custom analysis workers or submitting new task.

## Installation

This package can be installed through pip:

    pip install dane

### Configuration

DANE components are configured through the dane.config module, which is described here: https://dane.readthedocs.io/en/latest/intro.html#configuration 
It is however noteable that, because all DANE components are expected to rely on it, some of the DANE-server, ElasticSearch and RabbitMQ configuration 
are included in the default config. As such it is recommended that you create a `$HOME/.dane/config.yml` or `$DANE_HOME/config.yml` which contain machine-wide settings for how to connect to these services, which involves specifying the following settings:

```
DANE:
    API_URL: 'http://localhost:5500/DANE/'
    MANAGE_URL: 'http://localhost:5500/manage/'
RABBITMQ:
    HOST: 'localhost'
    PORT: 5672
    EXCHANGE: 'DANE-exchange'
    RESPONSE_QUEUE: 'DANE-response-queue'
    USER: 'guest'
    PASSWORD: 'guest'
ELASTICSEARCH:
    HOST: ['localhost']
    PORT: 9200
    USER: 'elastic'
    PASSWORD: 'changeme'
    SCHEME: 'http'
    INDEX: 'your_dane_index'
```

The values given here are the default values.

### Usage

Examples of how to use DANE can be found in the `examples/` directory.

## Local Development

We moved from `setup.py` & `requirements.txt` to a single `pyproject.toml`. For local builds and publishing we use [poetry](https://python-poetry.org/).

For local installation:

```bash
poetry install
poetry shell
```

After installation the following unit test should succeed:

```bash
python -m test.test_dane
```

To build a wheel + source package (will end up in `dist` directory):

```bash
poetry build
```

The wheel can be conveniently tested in e.g. your own DANE worker by installing it e.g. using `pip`:

```bash
pip install path_to_dane_wheel_file
```

or with poetry

```bash
poetry add path_to_dane_wheel_file
```

### Breaking changes after 0.3.1 

Since version 0.3.1 DANE must be imported in lowercase letters:

```python
import dane
```

Before version 0.3.1 you should import using uppercase letters:

```python
import DANE
```