Examples
======================================

All code examples can be found `here <https://github.com/CLARIAH/DANE/tree/master/examples>`_.

Examples
**********************

To explore the options of DANE we've create a jupyter notebook for you to experiment with creating
a DANE documents and tasks yourself. 

https://github.com/CLARIAH/DANE/blob/master/examples/dane_example.ipynb

Instead of performing any interaction with a DANE server, the examples work with a 
`DummyHandler <https://github.com/CLARIAH/DANE/blob/master/examples/dummyhandler.py>`_
which implements all the required handler functionality. The DummyHandler simply
stores all information in variables, as such there is no persistence of the data,
but it does allow for experimenting with DANE.

An example worker
**********************

DANE workers have been designed such that very little boilerplate code is necessary.
Nonetheless, some boilerplate is required to ensure we can rely on the logic defined
in the :class:`DANE.base_classes.base_worker`.

In this example we will break down the code on how to construct a `worker <https://github.com/CLARIAH/DANE/blob/master/examples/filesize_worker.py>`_
which returns the file size of a source file.

We'll start by defining a new class, which we have appropriately named 
`filesize_worker`. We ensure that it inherits from :class:`DANE.base_classes.base_worker`,
and then we're ready to start adding logic.

.. code-block:: python

    class filesize_worker(DANE.base_classes.base_worker):
        __queue_name = 'filesize_queue'
        __binding_key = '#.FILESIZE'

        def __init__(self, config):
            super().__init__(queue=self.__queue_name, 
                    binding_key=self.__binding_key, config=config)

First, we define two class constants with the name of the queue the worker should use,
and the binding key. We want all workers of the same type to share the same queue name, 
so if we start multiple workers they can divide the work. 

The binding key follows the pattern :code:`<document type>.<task key>`, where the document type 
can be `*` for any type of source material, or optionally we can build a worker which only
processes a specific type of document. The a task object uses a key to specify that
we mean this type of worker.

In theory, multiple different workers can have the same key, while having a 
different queue name. This could be use for example to do logging. However, this can be
risky in that if the queue for the intended task is not initialised, the task might never
be assigned to the correct queue.

Up next is the __init__ function. In order to properly set up the worker we need to call the
init of the base_worker class, provide the queue name, binding key, and the config
parameters to connect to the RabbitMQ instance. If the worker requires any set up, 
the init can be extended to include this as well.

Besides any setting up logic which might be in the init, the majority of worker specific
logic is contained in the callback. This function is called whenever a new task is read 
from the queue.

The base_worker contains all the code for interacting with the queue, so in the
callback we can focus on actually doing the work.

.. code-block:: python

    def callback(self, task, doc):
        if exists(doc.target['url']):

            fs = getsize(doc.target['url'])

            return json.dumps({'state': 200,
                'message': 'Success')
        else: 
            return json.dumps({'state': 404,
                'message': 'No file found at source_url'})

The callback receives a task and a document. For the file size worker we are only interested in the
source material. We assume that the source material is a local file, so we can
rely on functionality from :class:`os.path`.

The first step is to check if the source material actually exists. In general,
any input verification and validity checking is relegated to the workers
themselves. If the file exists, we retrieve its size and return a JSON serialised
dict containing the success state (200), a message detailing that we have succeeded
If we want to store the retrieved file size, or make it available to later tasks we can
store it in a :class:`DANE.Result`.

For the else clause, we can simply return a 404 state, and a descriptive message to indicate
that the source material was not found. In all cases a task **must** return a **state**
and a **message**. For more on states see :ref:`states`. 

Lastly, we need some code to start the worker.

.. code-block:: python

    if __name__ == '__main__':

        fsw = filesize_worker(cfg)
        print(' # Initialising worker. Ctrl+C to exit')

        try: 
            fsw.run()
        except KeyboardInterrupt:
            fsw.stop()

To start a worker, we first initialise it with a config file. By default
a worker only needs access to the ElasticSearch and RabbitMQ details provided 
by the DANE.config, such that it can store and read data, as well as
set up a queue and listen to work to perform. However, this can be extended
with worker specific configuration options. More details on how to work with
the configuration can be found in the :ref:`Usage <config>` guide.

After having initialised the worker we can simply call the :func:`DANE.base_classes.base_worker.run()`
method to start listening for work. As this starts a blocking process, we have
added a way in which we can (slightly) more elegantly interrupt it. Namely,
once Ctrl+C is pressed, this will trigger the KeyBoardInterrupt exception,
which we catch with the try-except block, and then we call the stop method.

To test this worker it is necessary to have access to a `RabbitMQ <https://www.rabbitmq.com/>`_
instance. However, to simulate task requests we have constructed 
`a generator <https://github.com/CLARIAH/DANE/blob/master/examples/filesize_request_generator.py>`_ 
which can be run without having to set up the other components of a DANE server.
