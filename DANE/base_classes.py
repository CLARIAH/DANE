# Copyright 2020-present, Netherlands Institute for Sound and Vision (Nanne van Noord)
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##############################################################################

import DANE
import DANE.utils
from DANE.handlers import ESHandler
from abc import ABC, abstractmethod
import pika
import json
import threading
import functools
import traceback
import os.path

class base_worker(ABC):
    """Abstract base class for a worker. 

    This class contains most of the logic of dealing with DANE-server,
    classes (workers) inheriting from this class only need to specific the
    callback method.
    
    :param queue: Name of the queue for this worker
    :type queue: str
    :param binding_key: A string following the format as explained
        here: https://www.rabbitmq.com/tutorials/tutorial-five-python.html
        or a list of such strings
    :type binding_key: str or list
    :param config: Config settings of the worker
    :type config: dict
    :param depends_on: List of task_keys that need to have been performed on
        the document before this task can be run
    :type depends_on: list, optional
    :param auto_connect: Connect to AMQ on init, set to false to debug worker
        as a standalone class.
    :type auto_connect: bool, optional
    :param no_api: Disable ESHandler, mainly for debugging.
    :type no_api: bool, optional
    """

    VALID_TYPES = ["Dataset", "Image", "Video", "Sound", "Text", "*", "#"]
    def __init__(self, queue, binding_key, config, depends_on=[], 
            auto_connect=True, no_api=False):

        self.queue = queue

        if not isinstance(binding_key, list):
            binding_key = [binding_key]

        for bk in binding_key:
            type_filter = bk.split('.')[0]
            if type_filter not in self.VALID_TYPES:
                raise ValueError("Invalid type filter `{}`. Valid types are: {}".format(
                    type_filter, 
                    ", ".join(self.VALID_TYPES)))

        self.binding_key = binding_key

        self.config = config
        self.depends_on = depends_on
        self._connected = False

        if DANE.utils.cwd_is_git():
        # if the cwd is a git repo we can prefill the generator dict
            self.generator = { 'id': DANE.utils.get_git_revision(),
                    "type": "Software",
                    "name": self.queue,
                    "homepage": DANE.utils.get_git_remote()}
        else:
            self.generator = None

        if auto_connect:
            self.connect()

        if not no_api:
            self.handler = ESHandler(config)
        else:
            self.handler = None

    def connect(self):
        """Connect the worker to the AMQ. Called by init if autoconnecting.
        """
        self.host = self.config.RABBITMQ.HOST
        self.port = self.config.RABBITMQ.PORT
        self.exchange = self.config.RABBITMQ.EXCHANGE

        user = self.config.RABBITMQ.USER
        password = self.config.RABBITMQ.PASSWORD

        credentials = pika.PlainCredentials(user, password)
        self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        credentials=credentials,
                        host=self.host, port=self.port))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=self.exchange, 
                exchange_type='topic')

        self.channel.queue_declare(queue=self.queue, 
                arguments={'x-max-priority': 10},
                durable=True)

        if not isinstance(self.binding_key, list):
            self.binding_key = [self.binding_key]

        for bk in self.binding_key:
            self.channel.queue_bind(exchange=self.exchange, 
                queue=self.queue,
                routing_key=bk)

        self.channel.basic_qos(prefetch_count=1)
        self._connected = True
        self._is_interrupted = False

    def run(self):
        """Start listening for tasks to be executed.
        """
        if self._connected:
            for method, props, body in self.channel.consume(self.queue, inactivity_timeout=1):
                if self._is_interrupted or not self._connected:
                    break
                if not method:
                    continue
                self._callback(self.channel, method, props, body)
        else:
            raise DANE.errors.ResourceConnectionError('Not connected to AMQ')

    def stop(self):
        """Stop listening for tasks to be executed.
        """
        if self._connected:
            self._is_interrupted = True
        else:
            raise DANE.errors.ResourceConnectionError('Not connected to AMQ')

    def _callback(self, ch, method, props, body):
        try:
            body = json.loads(body)
            if 'task' not in body.keys() or 'document' not in body.keys():
                raise KeyError(('Incompleted task specification, '
                    'require both `task` and `document` information'))

            task = DANE.Task(**body['task'])
            doc = DANE.Document(**body['document'], api=self.handler)

            done = True # assume assigned are done, unless find otherwise
            if len(self.depends_on) > 0:
                if self.handler is None:
                    raise SystemError("No handler available to check worker dependencies")

                assigned = doc.getAssignedTasks()
                assigned_keys = [a['key'] for a in assigned]

                dependencies = []
                for dep in self.depends_on:
                    if dep not in assigned_keys:
                        # this task is not yet assigned to the document
                        # create and assign it
                        dependencies.append(dep)
                        done = False
                    else:
                        # only need to check if this is done if all preceding deps are done
                        if done:
                            # task is assigned to the document, but is it done?
                            if any([a['state'] != 200 for a in assigned if a['key'] == dep]):
                                # a task of type dep is assigned that isnt done
                                # wait for it
                                done = False
            if not done:
                # some dependency isnt done yet, wait for it
                response = { 'state': 412, 
                        'message': 'Unfinished dependencies',
                        'dependencies': dependencies }

                self._ack_and_reply(json.dumps(response), ch, method, props)
            else: 
                self.thread = threading.Thread(target=self._run, 
                        args=(task, doc, ch, method, props))
                self.thread.setDaemon(True)
                self.thread.start()

        except TypeError as e:
            response = { 'state': 400, 
                    'message': 'Invalid format, unable to proceed'}

            self._ack_and_reply(json.dumps(response), ch, method, props)
        except Exception as e:
            traceback.print_exc() # TODO add a flag to disable this
            response = { 'state': 500, 
                    'message': 'Unhandled error: ' + str(e)}

            self._ack_and_reply(json.dumps(response), ch, method, props)

    def _run(self, task, doc, ch, method, props):
        try:
            response = self.callback(task, doc)
        except DANE.errors.RefuseJobException:
            # worker doesnt want the job yet, nack it
            nack = functools.partial(ch.basic_nack, 
                    delivery_tag=method.delivery_tag)
            self.connection.add_callback_threadsafe(nack)
            return
        except Exception as e:
            traceback.print_exc() # TODO add a flag to disable this

            response = { 'state': 500, 
                    'message': 'Unhandled worker error: ' + str(e)}

        if not isinstance(response, str):
            response = json.dumps(response)

        reply_cb = functools.partial(self._ack_and_reply, response,
                ch, method, props)
        self.connection.add_callback_threadsafe(reply_cb)

    def _ack_and_reply(self, response, ch, method, props):
        ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(
                         correlation_id = props.correlation_id,
                         delivery_mode=2),
                     body=str(response))

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def getDirs(self, document):
        """This function returns the TEMP and OUT directories for this job
        creating them if they do not yet exist
        output should be stored in response['SHARED']
        
        :param job: The job
        :type job: :class:`DANE.Job`
        :return: Dict with keys `TEMP_FOLDER` and `OUT_FOLDER`
        :rtype: dict
        """
        # expect that TEMP and OUT folder exist 
        TEMP_SOURCE = self.config.PATHS.TEMP_FOLDER
        OUT_SOURCE = self.config.PATHS.OUT_FOLDER

        if not os.path.exists(TEMP_SOURCE):
            os.mkdir(TEMP_SOURCE)
        if not os.path.exists(OUT_SOURCE):
            os.mkdir(OUT_SOURCE)

        # Get a more specific path name, by chunking id into (at most)
        # three chunks of 2 characters
        chunks = os.path.join(*[document._id[i:2+i] for i in range(0, 
            min(len(document._id),6), 2)])
        TEMP_DIR = os.path.join(TEMP_SOURCE, chunks, document._id)
        OUT_DIR = os.path.join(OUT_SOURCE, chunks, document._id)

        if not os.path.exists(TEMP_DIR):
            os.makedirs(TEMP_DIR)
        if not os.path.exists(OUT_DIR):
            os.makedirs(OUT_DIR)

        return {
            'TEMP_FOLDER': TEMP_DIR,
            'OUT_FOLDER': OUT_DIR
        }

    @abstractmethod
    def callback(self, task, document):
        """Function containing the core functionality that is specific to
        a worker. 

        :param task: Task to be executed
        :type task: :class:`DANE.Task`
        :param document: Document the task is applied to
        :type document: :class:`DANE.Document`
        :return: Task response with the `message`, `state`, and
            optional additional response information
        :rtype: dict
        """
        return
