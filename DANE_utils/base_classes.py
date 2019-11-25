from abc import ABC, abstractmethod
import pika
import json

class base_worker(ABC):

    def __init__(self, host, queue, binding_key):
        self.host = host
        self.exchange = 'DANE'
        self.queue = queue
        self.binding_key = binding_key

        self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(self.host))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=self.exchange, 
                exchange_type='topic')

        self.channel.queue_declare(queue=self.queue, 
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
        self.channel.basic_consume(on_message_callback=self.__callback, 
                                   auto_ack=False,
                                   queue=self.queue)

    def run(self):
        self.channel.start_consuming()

    def stop(self):
        self.channel.stop_consuming()

    def __callback(self, ch, method, props, body):
        response = self.callback(json.loads(body))

        ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))

        ch.basic_ack(delivery_tag=method.delivery_tag)

    @abstractmethod
    def callback(self, job_request):
        return
