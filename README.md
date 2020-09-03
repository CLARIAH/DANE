# DANE
The Distributed Annotation 'n' Enrichment (DANE) system handles compute task assignment and file storage for the automatic annotation of content.

This repository contains contains the building blocks for with DANE, such as creating custom analysis workers or submitting new task.

## Installation

This package can be installed through pip:

    pip install DANE

### Configuration

DANE components are configured through the DANE.config module, which is described here: https://dane.readthedocs.io/en/latest/intro.html#configuration 
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
    HOST: 'localhost'
    PORT: 9200
    USER: 'elastic'
    PASSWORD: 'changeme'
    SCHEME: 'http'
```

The values given here are the default values.

### Usage

Examples of how to use DANE can be found in the `examples/` directory.
