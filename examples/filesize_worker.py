from DANE_utils.base_classes import base_worker
from os.path import getsize
import json

class filesize_worker(base_worker):
    # we specify a queue name because every worker of this type should 
    # listen to the same queue
    __queue_name = 'filesize_queue'

    def __init__(self, host, exchange='DANE-exchange', 
            port=5672, user='guest', password='guest'):
        # routing key follows pattern of <file type>.<worker type>
        # worker type is 'filesize' for this worker, and we listen
        # for any possible filetype, so '#'
        super().__init__(host=host, queue=self.__queue_name, 
                binding_key='#.filesize', port=port, user=user, password=password)

    def callback(self, job_request):
        print('Got request', job_request)
        fs = getsize(job_request['file'])

        return json.dumps({'state': 200,
            'message': 'Success',
            'filesize': fs})

if __name__ == '__main__':
    fsw = filesize_worker('localhost')
    print(' # Initialising worker. Ctrl+C to exit')

    try: 
        fsw.run()
    except KeyboardInterrupt:
        fsw.stop()
