import pika
import uuid
import json
import DANE 
from DANE.config import cfg

class filesize_server():

    def __init__(self, config):
        self.config = config.RABBITMQ
        credentials = pika.PlainCredentials(self.config.USER, 
                self.config.PASSWORD)
        self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        credentials=credentials,
                        host=self.config.HOST, port=self.config.PORT))

        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='response_queue', exclusive=True)

        self.channel.basic_consume(
            queue='response_queue',
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        print('# Response:', json.loads(body))
        self.stop()
        print('## Handled response. Exiting..')

    def run(self):
        self.channel.start_consuming()

    def stop(self):
        self.channel.stop_consuming()

    def simulate_request(self):
        job = DANE.Job(source_url=__file__, 
            source_id='TEST',
            tasks=DANE.taskSequential(['FILESIZE']))

        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange=self.config.EXCHANGE,
            routing_key='plaintext.filesize',
            properties=pika.BasicProperties(
                reply_to='response_queue',
                correlation_id=self.corr_id,
            ),
            body=job.to_json())

if __name__ == '__main__':

    fss = filesize_server(cfg)

    print('## Simulating request for size of this file')
    fss.simulate_request()

    print('## Waiting for response. Ctrl+C to exit.')
    try: 
        fss.run()
    except KeyboardInterrupt:
        fss.stop()
