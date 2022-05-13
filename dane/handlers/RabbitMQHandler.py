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

import pika
import sys
import json
import threading
from time import sleep
import functools
import logging

MAX_RETRY = 8
RETRY_INTERVAL = 2 # seconds

logger = logging.getLogger('DANE')

class RabbitMQHandler():

    def __init__(self, config):
        self.config = config	
        self.callback = None
        self.retry = 0
        self.connect()

    def connect(self):
        if not hasattr(self, 'connection') or \
            not self.connection or self.connection.is_closed:
            credentials = pika.PlainCredentials(
                    self.config.RABBITMQ.USER, 
                    self.config.RABBITMQ.PASSWORD)

            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        credentials=credentials,
                        host=self.config.RABBITMQ.HOST,
                        port=self.config.RABBITMQ.PORT))
            except (pika.exceptions.AMQPConnectionError, 
                    pika.exceptions.ConnectionClosedByBroker) as e:
                self.retry += 1
                if self.retry <= MAX_RETRY:
                    nap_time = RETRY_INTERVAL ** self.retry
                    logger.warning('RabbitMQ Connection Failed. '\
                            'RETRYING in {} seconds'.format(nap_time))
                    sleep(nap_time)
                    self.connect()
                else:
                    logger.critical(
                            'RabbitMQ connection failed, no retries left')
                    raise e from None
            else:
                self.retry = 0
                self.channel = self.connection.channel()
                self.pub_channel = self.connection.channel()

                self.pub_channel.confirm_delivery()

                self.channel.exchange_declare(
                        exchange=self.config.RABBITMQ.EXCHANGE, 
                        exchange_type='topic')

                self.channel.queue_declare(
                        queue=self.config.RABBITMQ.RESPONSE_QUEUE, 
                        durable=True)

    def run(self):
        raise NotImplementedError('Run should be implemented server-side')

    def stop(self):
        raise NotImplementedError('Stop should be implemented server-side')

    def assign_callback(self, callback):
        raise NotImplementedError('assign_callback should be implemented server-side')

    def publish(self, routing_key, task, document, retry=False):
        try:
            self.pub_channel.basic_publish(
                exchange=self.config.RABBITMQ.EXCHANGE,
                routing_key=routing_key,
                properties=pika.BasicProperties(
                    reply_to=self.config.RABBITMQ.RESPONSE_QUEUE,
                    correlation_id=str(task._id),
                    priority=int(task.priority),
                    delivery_mode=2
                ),
                mandatory=True,
                body=json.dumps({
                    # flipflop between json and object is intentional
                    # but maybe not most elegant way..
                    'task': json.loads(task.to_json()),
                    'document': json.loads(document.to_json())
                    }))
        except pika.exceptions.ChannelWrongStateError as e:
            if not retry: # retry once
                logger.exception('Publish error')
                self.connect()
                self.publish(routing_key, task, document, retry=True)
            else:
                raise e
        except Exception as e:
            raise e
