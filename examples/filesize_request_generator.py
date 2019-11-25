import pika
import uuid
import json

class filesize_server():

    def __init__(self):
        self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='localhost'))

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
        job_spec = {'file': __file__}

        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='DANE',
            routing_key='plaintext.filesize',
            properties=pika.BasicProperties(
                reply_to='response_queue',
                correlation_id=self.corr_id,
            ),
            body=json.dumps(job_spec))

if __name__ == '__main__':
    fss = filesize_server()

    print('## Simulating request for size of this file')
    fss.simulate_request()

    print('## Waiting for response. Ctrl+C to exit.')
    try: 
        fss.run()
    except KeyboardInterrupt:
        fss.stop()
