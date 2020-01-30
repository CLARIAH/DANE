import DANE
from os.path import getsize, exists
import json

class filesize_worker(DANE.base_classes.base_worker):
    # we specify a queue name because every worker of this type should 
    # listen to the same queue
    __queue_name = 'filesize_queue'
    __binding_key = '#.FILESIZE'

    def __init__(self, config):
        # routing key follows pattern of <file type>.<task key>
        # task key is 'FILESIZE' for this worker, and we listen
        # for any possible filetype, so '#'
        super().__init__(queue=self.__queue_name, 
                binding_key=self.__binding_key, config=config)

    def callback(self, job):
        if exists(job.source_url):
            fs = getsize(job.source_url)
            return json.dumps({'state': 200,
                'message': 'Success',
                'size': fs})
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
