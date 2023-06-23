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

from dane import Task, Document, ProcState
from dane.utils import cwd_is_git, get_git_revision, get_git_remote
from dane.errors import RefuseJobException, ResourceConnectionError
from dane.handlers import ESHandler
from abc import ABC, abstractmethod
import pika
import json
from typing import Tuple, Optional
import threading
import functools
import os.path
import logging

logger = logging.getLogger("DANE")  # TODO change to __name__ everywhere later on


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

    def __init__(
        self, queue, binding_key, config, depends_on=[], auto_connect=True, no_api=False
    ):
        logger.info("Initialising base worker")
        self.queue = queue

        if not isinstance(binding_key, list):
            binding_key = [binding_key]

        for bk in binding_key:
            type_filter = bk.split(".")[0]
            if type_filter not in self.VALID_TYPES:
                logger.error(f"Invalid type filter: {type_filter}")
                raise ValueError(
                    "Invalid type filter `{}`. Valid types are: {}".format(
                        type_filter, ", ".join(self.VALID_TYPES)
                    )
                )

        self.binding_key = binding_key

        self.config = config
        self.depends_on = depends_on
        self._connected = False

        if cwd_is_git():
            # if the cwd is a git repo we can prefill the generator dict
            self.generator = {
                "id": get_git_revision(),
                "type": "Software",
                "name": self.queue,
                "homepage": get_git_remote(),
            }
        else:
            self.generator = None

        if auto_connect:
            self.connect()

        if not no_api:
            self.handler = ESHandler(config)
        else:
            self.handler = None

    def connect(self):
        """Connect the worker to the AMQ. Called by init if autoconnecting."""
        logger.info("Connecting to message queue...")
        self.host = self.config.RABBITMQ.HOST
        self.port = self.config.RABBITMQ.PORT
        self.exchange = self.config.RABBITMQ.EXCHANGE

        user = self.config.RABBITMQ.USER
        password = self.config.RABBITMQ.PASSWORD

        credentials = pika.PlainCredentials(user, password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                credentials=credentials, host=self.host, port=self.port
            )
        )
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=self.exchange, exchange_type="topic")

        self.channel.queue_declare(
            queue=self.queue, arguments={"x-max-priority": 10}, durable=True
        )

        if not isinstance(self.binding_key, list):
            self.binding_key = [self.binding_key]

        for bk in self.binding_key:
            self.channel.queue_bind(
                exchange=self.exchange, queue=self.queue, routing_key=bk
            )

        self.channel.basic_qos(prefetch_count=1)
        self._connected = True
        self._is_interrupted = False

    def run(self):
        """Start listening for tasks to be executed."""
        logger.info("Waiting for the queue to bring in some tasks...")
        if self._connected:
            for method, props, body in self.channel.consume(
                self.queue, inactivity_timeout=1
            ):
                if self._is_interrupted or not self._connected:
                    break
                if not method:
                    continue

                # first inspect if the task has dependencies, otherwise start processing
                self._inspect_then_run_task(self.channel, method, props, body)
        else:
            raise ResourceConnectionError("Not connected to AMQ")

    def stop(self):
        """Stop listening for tasks to be executed."""
        logger.info("No longer waiting for the queue, stopping...")
        if self._connected:
            self._is_interrupted = True
        else:
            raise ResourceConnectionError("Not connected to AMQ")

    def _validate_received_data(self, data: str) -> Optional[dict]:
        logger.info("Validating received queue data")
        try:
            valid_data = json.loads(data)
            if all(x in valid_data.keys() for x in ["task", "document"]):
                return valid_data
        except json.JSONDecodeError:
            logger.error(f"Non-JSON data passed in the queue: {data}")
        return None

    def _check_handler_or_die(self):
        logger.info("Checking handler availability")
        if self.handler is None:
            raise SystemError("No handler available to check worker dependencies")

    def _check_task_dependencies(self, doc: Document) -> Tuple[bool, list]:
        logger.info(
            "Checking tasks dependencies (if they are met or we still have to wait)"
        )
        done = True  # assume assigned are done, unless find otherwise
        dependencies = []
        if len(self.depends_on) > 0:
            assigned_tasks = doc.getAssignedTasks()
            task_keys = [t["key"] for t in assigned_tasks]

            for dep in self.depends_on:
                # if the task is not yet assigned to the document, create and assign it
                if dep not in task_keys:
                    dependencies.append(dep)
                    done = False
                elif done:  # otherwise only check if all preceding deps are done
                    if any(  # task is assigned to the document, but is it done?
                        [
                            t["state"] != ProcState.SUCCESS.value
                            for t in assigned_tasks
                            if t["key"] == dep
                        ]
                    ):  # a task of type dep is assigned that isnt done, wait for it
                        done = False
        return done, dependencies

    # inspects the received queue data and if there are any task dependencies
    # that need to be done before this worker can properly do it's processing
    def _inspect_then_run_task(self, ch, method, props, body):
        logger.info("Inspecting task obtained from queue data")
        try:
            # first validate the received queue data
            body = self._validate_received_data(body)
            if not body:
                raise KeyError(
                    (
                        "Incompleted task specification, "
                        "require both `task` and `document` information"
                    )
                )

            # check if the handler is available (raises SystemError)
            self._check_handler_or_die()

            # now create Task and Document objects from the queue data
            task = Task(**body["task"])
            doc = Document(**body["document"], api=self.handler)

            # try to find task dependencies before continuing
            dependencies_met, dependencies = self._check_task_dependencies(doc)

            if not dependencies_met:  # some dependency isn't done yet, wait for it
                logger.info(f"Dependencies not met, putting {task.key} task on hold")
                response = {
                    "state": ProcState.UNFINISHED_DEPENDENCY.value,
                    "message": "Unfinished dependencies",
                    "dependencies": dependencies,
                }
                self._ack_and_reply(response, ch, method, props)
            else:  # start the worker "callback" in a different thread
                logger.info(
                    f"Dependencies met, starting the work on the {task.key} task"
                )
                self.thread = threading.Thread(
                    target=self._start_processing_task,
                    args=(task, doc, ch, method, props),
                )
                self.thread.setDaemon(True)
                self.thread.start()

        except TypeError:
            logger.exception("Invalid format, unable to proceed")
            response = {
                "state": ProcState.BAD_REQUEST.value,
                "message": "Invalid format, unable to proceed",
            }
            self._ack_and_reply(response, ch, method, props)
        except Exception as e:
            logger.exception("Unhandled error")
            response = {
                "state": ProcState.ERROR.value,
                "message": "Unhandled error: " + str(e),
            }
            self._ack_and_reply(response, ch, method, props)

    # Triggers the worker's callback function
    # TODO send back a PROCESSING state BEFORE running the callback (figure out how)
    def _start_processing_task(self, task, doc, ch, method, props):
        logger.info(f"Started processing task {task._id} for doc {doc._id}")
        try:
            # now let the worker do it's own work
            response = self.callback(task, doc)

            # after the work, report back the resulting state
            self._ack_with_status_msg(response, ch, method, props)
        except RefuseJobException:
            logger.exception("Job refused")
            # worker doesnt want the task yet, nack it
            self._nack_refuse_task(ch, method)
            return
        except Exception as e:
            logger.exception("Unhandler error")
            self._ack_with_status_msg(
                {
                    "state": ProcState.ERROR.value,
                    "message": f"Unhandled worker error: {str(e)}",
                },
                ch,
                method,
                props,
            )

    def _nack_refuse_task(self, ch, method):
        logger.info("Send NACK to queue: refuse task")
        nack = functools.partial(ch.basic_nack, delivery_tag=method.delivery_tag)
        self.connection.add_callback_threadsafe(nack)

    def _ack_with_status_msg(self, response: dict, ch, method, props):
        logger.info("Send ACK + msg back to queue (async)")
        reply_cb = functools.partial(
            self._ack_and_reply,
            response,
            ch,
            method,
            props,
        )
        self.connection.add_callback_threadsafe(reply_cb)

    def _ack_and_reply(self, response: dict, ch, method, props):
        logger.info("Send ACK + msg back to queue")
        ch.basic_publish(
            exchange="",
            routing_key=props.reply_to,
            properties=pika.BasicProperties(
                correlation_id=props.correlation_id, delivery_mode=2
            ),
            body=json.dumps(response),  # convert to string
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def getDirs(self, document, create_input_dir=True, create_output_dir=True):
        """This function returns the TEMP and OUT directories for this job
        creating them if they do not yet exist
        output should be stored in response['SHARED']

        :param job: The job
        :type job: :class:`Job`
        :return: Dict with keys `TEMP_FOLDER` and `OUT_FOLDER`
        :rtype: dict
        """
        logger.info(
            f"Generating TEMP_FOLDER (auto create={create_input_dir}) and OUT_FOLDER (auto create={create_output_dir})"
        )
        # expect that TEMP and OUT folder exist
        TEMP_SOURCE = self.config.PATHS.TEMP_FOLDER
        OUT_SOURCE = self.config.PATHS.OUT_FOLDER

        # Get a more specific path name, by chunking id into (at most)
        # three chunks of 2 characters
        chunks = os.path.join(
            *[document._id[i : 2 + i] for i in range(0, min(len(document._id), 6), 2)]
        )
        TEMP_DIR = os.path.join(TEMP_SOURCE, chunks, document._id)
        OUT_DIR = os.path.join(OUT_SOURCE, chunks, document._id)

        if create_input_dir:
            logger.info(f"Creating input dir: {TEMP_DIR}")
            if not os.path.exists(TEMP_SOURCE):
                os.mkdir(TEMP_SOURCE)
            if not os.path.exists(TEMP_DIR):
                os.makedirs(TEMP_DIR)

        if create_output_dir:
            logger.info(f"Creating output dir: {OUT_DIR}")
            if not os.path.exists(OUT_SOURCE):
                os.mkdir(OUT_SOURCE)
            if not os.path.exists(OUT_DIR):
                os.makedirs(OUT_DIR)

        return {"TEMP_FOLDER": TEMP_DIR, "OUT_FOLDER": OUT_DIR}

    @abstractmethod
    def callback(self, task, document):
        """Function containing the core functionality that is specific to
        a worker.

        :param task: Task to be executed
        :type task: :class:`Task`
        :param document: Document the task is applied to
        :type document: :class:`Document`
        :return: Task response with the `message`, `state`, and
            optional additional response information
        :rtype: dict
        """
        return
