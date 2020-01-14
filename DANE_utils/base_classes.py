from DANE_utils import jobspec
from abc import ABC, abstractmethod
import pika
import json
import threading
import functools

class base_worker(ABC):

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

        # binding_key can be a single str following the format as explained
        # here: https://www.rabbitmq.com/tutorials/tutorial-five-python.html
        # or it can be a list of such strings
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
        self.channel.start_consuming()

    def stop(self):
        self.channel.stop_consuming()

    def _callback(self, ch, method, props, body):
        try:
            job = jobspec.jobspec.from_json(body)
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

        reply_cb = functools.partial(self._ack_and_reply, json.dumps(response),
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
        return

class base_handler(ABC):
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def register_job(self, job):
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
        return
    
    @abstractmethod
    def getTaskState(self, task_id):
        return

    @abstractmethod
    def getTaskKey(self, task_id):
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
    def callback(self, task_id, response):
        return

    @abstractmethod
    def search(self, source_id, source_set=None):
        return
