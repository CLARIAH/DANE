import pika
import uuid
import json
from dane import Document, Task
from dane.config import cfg


class filesize_server:
    def __init__(self, config):
        self.config = config.RABBITMQ
        credentials = pika.PlainCredentials(self.config.USER, self.config.PASSWORD)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                credentials=credentials, host=self.config.HOST, port=self.config.PORT
            )
        )

        self.channel = self.connection.channel()
        self.channel.queue_declare(queue="response_queue", exclusive=True)

        self.channel.basic_consume(
            queue="response_queue", on_message_callback=self.on_response, auto_ack=True
        )

    def on_response(self, ch, method, props, body):
        print("# Response:", json.loads(body))
        self.stop()
        print("## Handled response. Exiting..")

    def run(self):
        self.channel.start_consuming()

    def stop(self):
        self.channel.stop_consuming()

    def simulate_request(self):
        task = Task("FILESIZE")
        document = Document(
            {"id": "THIS", "url": "filesize_request_generator.py", "type": "Text"},
            {"id": "FileSizeExample", "type": "Software"},
        )

        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange=self.config.EXCHANGE,
            routing_key="#.FILESIZE",
            properties=pika.BasicProperties(
                reply_to="response_queue",
                correlation_id=self.corr_id,
            ),
            body=json.dumps(
                {
                    # flipflop between json and object is intentional
                    # but maybe not most elegant way..
                    "task": json.loads(task.to_json())["task"],
                    "document": json.loads(document.to_json()),
                }
            ),
        )


if __name__ == "__main__":

    fss = filesize_server(cfg)

    print("## Simulating request for size of this file")
    fss.simulate_request()

    print("## Waiting for response. Ctrl+C to exit.")
    try:
        fss.run()
    except KeyboardInterrupt:
        fss.stop()
