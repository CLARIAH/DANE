import DANE
from abc import ABC, abstractmethod
import pika
import json
import threading
import functools

class base_worker(ABC):
    """Abstract base class for a worker. 

    This class contains most of the logic of dealing with DANE-core,
    classes (workers) inheriting from this class only need to specific the
    callback method.
    
    :param queue: Name of the queue for this worker
    :type queue: str
    :param binding_key: A str following the format as explained
        here: https://www.rabbitmq.com/tutorials/tutorial-five-python.html
        or a list of such strings
    :type binding_key: str or list
    :param config: Config settings of the worker
    :type config: dict
    """
    def __init__(self, queue, binding_key, config):
        self.queue = queue
        self.binding_key = binding_key

        self.config = config
        self.host = config['RABBITMQ']['host']
        self.port = config['RABBITMQ']['port']
        self.exchange = config['RABBITMQ']['exchange']

        user = config['RABBITMQ']['user']
        password = config['RABBITMQ']['password']

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
        self.channel.basic_consume(on_message_callback=self._callback, 
                                   auto_ack=False,
                                   queue=self.queue)

    def run(self):
        """Start listening for tasks to be executed.
        """
        self.channel.start_consuming()

    def stop(self):
        """Stop listening for tasks to be executed.
        """
        self.channel.stop_consuming()

    def _callback(self, ch, method, props, body):
        try:
            job = DANE.Job.from_json(body)
        except TypeError as e:
            response = { 'state': 400, 
                    'message': 'Invalid job format, unable to proceed'}

            self._ack_and_reply(json.dumps(response), ch, method, props)
        except Exception as e:
            response = { 'state': 500, 
                    'message': 'Unhandled error: ' + str(e)}

            self._ack_and_reply(json.dumps(response), ch, method, props)
        else: 
            self.thread = threading.Thread(target=self._run, 
                    args=(job, ch, method, props))
            self.thread.setDaemon(True)
            self.thread.start()

    def _run(self, job, ch, method, props):
        response = self.callback(job)

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

    @abstractmethod
    def callback(self, job_request):
        """The callback contains the core functionality that is specific to
        this worker. 

        :param job_request: Job specification for new task
        :type job_request: :class:`DANE.Job`
        :return: dict (or JSON str) with the job message, state, and
            additional response information
        :rtype: dict or str
        """
        return

class base_handler(ABC):
    """Abstract base class for a handler. 

    A handler functions as the API used in DANE to facilitate all communication
    with the database and the queueing system.
    
    :param config: Config settings for the handler
    :type config: dict
    """
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def register_job(self, job):
        """Register a job in the database

        :param job: The job
        :type job: :class:`DANE.Job`
        :return: job_id
        :rtype: int
        """
        return

    # job objects task list is modified after each task is registered
    # to include task_ids, propagate this change
    @abstractmethod
    def propagate_task_ids(self, job):
        return

    # get_dirs returns the directories for this jobs TEMP and OUT 
    # creating them if they do not yet exist
    # output should be stored in response['SHARED']
    @abstractmethod
    def get_dirs(self, job):
        return

    @abstractmethod
    def register(self, job_id, task):
        """Register a task in the database

        :param job_id: id of the job this task belongs to
        :type job_id: int
        :param task: the task to register
        :type task: :class:`DANE.Task`
        :return: task_id
        :rtype: int
        """
        return
    
    @abstractmethod
    def getTaskState(self, task_id):
        """Retrieve state for a given task_id

        :param task_id: id of the task
        :type task_id: int
        :return: task_state
        :rtype: int
        """
        return

    @abstractmethod
    def getTaskKey(self, task_id):
        """Retrieve task_key for a given task_id

        :param task_id: id of the task
        :type task_id: int
        :return: task_key
        :rtype: str
        """
        return

    @abstractmethod
    def jobFromJobId(self, job_id):
        return

    @abstractmethod
    def jobFromTaskId(self, task_id):
        return

    def isDone(self, task_id):
        return self.getTaskState(task_id) == 200

    @abstractmethod
    def run(self, task_id):
        return

    @abstractmethod
    def retry(self, task_id):
        return

    @abstractmethod
    def callback(self, task_id, response):
        return

    @abstractmethod
    def search(self, source_id, source_set=None):
        return

    @abstractmethod
    def getUnfinished(self):
        return
