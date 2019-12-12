from DANE_utils.base_classes import base_worker
from os.path import getsize, exists
import json

class filesize_worker(base_worker):
    # we specify a queue name because every worker of this type should 
    # listen to the same queue
    __queue_name = 'filesize_queue'

    def __init__(self, config):
        # routing key follows pattern of <file type>.<worker type>
        # worker type is 'filesize' for this worker, and we listen
        # for any possible filetype, so '#'
        super().__init__(queue=self.__queue_name, 
                binding_key='#.filesize', config=config)

    def callback(self, job):
        print('Got request', job)

        if exists(job.source_url):
            fs = getsize(job.source_url)
            return json.dumps({'state': 200,
                'message': 'Success',
                'filesize': fs})
        else: 
            return json.dumps({'state': 404,
                'message': 'No file found at source_url'})

if __name__ == '__main__':
    config = {
        'RABBITMQ' : {
            'host': 'localhost',
            'exchange': 'DANE-exchange',
            'port': 5672,
            'user': 'guest',
            'password': 'guest'
        }
    }

    fsw = filesize_worker(config)
    print(' # Initialising worker. Ctrl+C to exit')

    try: 
        fsw.run()
    except KeyboardInterrupt:
        fsw.stop()
