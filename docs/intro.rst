Introduction
======================================

The Distributed Annotation 'n' Enrichment (DANE) system handles compute job assignment and file storage for the automatic annotation of content in a distributed manner.

The use-case for which DANE was designed centres around the issue that the compute resources, and the collection of source media are not on the same device.
Due to limited resources or policy choices it might not be possible or desirable to bulk transfer all source media to the compute resources, alternatively the
source collection might be continuously growing or require on-demand processing.

In all cases, the computation depends on several stages which can all bottleneck (i.e., introduce delays) to the overall process. By subdividing jobs into
individual tasks, which can be scheduled separately DANE is capable of more optimally using the available resources. For example, in the case of intensive video analysis
of a large archive, it is not feasible to move the entire video archive in a single pass to the compute server. By designing specific tasks for data transfer between servers,
analysis, and post-hoc clean-up, DANE can be used to schedule these tasks such that they can be performed in parallel.

Usage
**********************

In essence the DANE ecosystem consists of three parts, 1) The back-end (`DANE-server <https://github.com/CLARIAH/DANE-server/>`_), 2) The compute workers, 3) A client that submits the jobs. 
The format of the communication between these components follows the :doc:`job specification <DANE/jobs>` format which details the source material to process, 
the tasks to be performed, as well as information about the task results. 

.. image:: https://docs.google.com/drawings/d/e/2PACX-1vRCjKm3O5cqbF5LRlUyC6icAbQ3xmedKvArlY_8h31PJqAu3iZe6Q5qcVbs3rujVoGpzesD00Ck9-Hw/pub?w=953&amp;h=438

Once a job is submitted to DANE-server its tasks will be assigned to workers and executed in the order indicated in the job. As such, a worker relies on a DANE-server instance for 
its task assignment. To use DANE, one thus needs all three parts, namely an instance of DANE-server, some compute workers, and some client or process to submit jobs. 
Examples of workers and clients can be found :doc:`here <examples>`, whereas DANE-server is documented in its repository.

.. _config:

Configuration
**********************

The configuration of DANE components is done through the `DANE.config <https://github.com/CLARIAH/DANE/blob/master/DANE/config.py>`_ module, 
which builds on top of `YACS <https://github.com/rbgirshick/yacs>`_. The DANE.config specifies some default options, with default values, but
it is mainly meant to be extended with component specific options. YACS makes it possible to specify configurations in a yaml format, and in code,
here is a yaml example with some of the default config options:

.. code-block:: yaml

    DANE:
      HOST: '0.0.0.0'
      PORT: 5500
      API_URL: 'http://localhost:5500/DANE/'
    RABBITMQ:
      HOST: 'localhost'
      PORT: 5672
      EXCHANGE: 'DANE-exchange'

Here, we have specified that the host that the DANE server listens on is `0.0.0.0` with port `5500`, additionally, the url at which the API is reachable is
given by the `API_URL` field. Similarly, we specify a number of options for the RabbitMQ queueing system.

To deviate from the default options there are two options, 1) the system-wide DANE config file, and 2) the component specific config file. To best illustrate how
these are used we will first demonstrate how to get access to the config. The DANE.config module has an cfg object, which is a YACS config node, which we can get access to
by importing it as follows:

.. code-block:: python

    from DANE.config import cfg

We now have access to the config, and then we can pass it for example to a worker (as shown in the :doc:`examples`):

.. code-block:: python

    fsw = filesize_worker(cfg)

or we can retrieve specific values from the config.

.. code-block:: python

    print('The DANE API is available at', cfg.DANE.API_URL)

During the loading of the module, the default configuration will be constructed, subsequently DANE.config will try to load the system-wide config file, then the
component specific config file, and finally the instance specific config. By loading these in this order, 
the most specific options will be used (i.e., system-wide overrides defaults, and component specific
overrides system-wide and defaults both). DANE.config will look for the system-wide config at :code:`$HOME/.dane/config.yml` (or :code:`$DANE_HOME/config.yml` if available).

For the component specific config DANE.config looks in the directory of the importing component, for a `base_config.yml`, and it also looks for an optional instance 
specific config `config.yml`. It expects a directory structure akin to:

.. code-block:: 

    filesize_worker/
        filesize_worker.py
        base_config.yml
        config.yml

A nice feature of YACS is that it is not necessary to specify all configuration options, we only need to specific the ones we would like to change or add. For the 
filesize_worker, the base_config.yml might thus look like this:

.. code-block:: yaml

    FILESIZE_WORKER:
        UNIT: 'KB'
        PRECISION: 2

Defining new (non-functional) options for the worker, namely the units in which the filesize should be expressed, and the number of decimals we want shown in the output. 
It also gives a default value for this option. Subsequently, we can define an instance specific config.yml (which shouldn't be committed to GIT), which contains the following options.

.. code-block:: yaml

    DANE:
      API_URL: 'http://somehost.ext:5500/DANE/'
    FILESIZE_WORKER:
        UNIT: 'MB'

This indicates that the API can be found at a different URL than the default one, and that we want the file size expressed in MB, for all other config options we
rely on the previously defined defaults.

.. _states:

Task states
**********************

Once a DANE worker has completed a task, or task progression has been interrupted due to an error, it should return a JSON object consisting of a `state` and a `message`.
The message is expected to be an informative, and brief, indication of what went wrong, this message is not intended for automatic processing. 

The state returned by a worker is used for automatic processing in DANE, based on this state it is determined whether a job is completed, in progress, requires retrying, or 
requires manual intervention. The state is one of the numerical `HTTP Status codes <https://developer.mozilla.org/en-US/docs/Web/HTTP/Status>`_ with the aim of trying to adhere
to the semantics of what the status code represents. For example, the state 200 indicates that the task has been successfully handled, whereas 102 indicates it is still in progress.
Below we provide an overview of all used state codes and how they are handled by DANE.

State overview
^^^^^^^^^^^^^^^^^^

* `102`: Task has been sent to a queue, it might be being worked on or held in queue.
* `200`: Task completed successfully.
* `201`: Task is registered, but has not been acted upon.
* `400`: Malformed request, typically the job description.
* `403`: Access denied to underlying source material.
* `404`: Underlying source material not found.
* `422`: If a task cannot be routed to a queue, this state is returned.
* `500`: Error occurred during processing, details should be given in message.
* `502`: Worker received invalid or partial input.
* `503`: Worker received an error response from a remote service it depends on. 

Tasks with state 502 or 503, can be retried automatically. Whereas states 400, 403, 404, 422, and 500 require manual intervention. Once a manual intervention has taken place
the job can be resumed.
