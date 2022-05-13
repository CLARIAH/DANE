from dane.base_classes import base_worker
from os.path import getsize, exists
import json
from dane.config import cfg

class filesize_worker(base_worker):
    # we specify a queue name because every worker of this type should 
    # listen to the same queue
    __queue_name = 'filesize_queue'
    __binding_key = '#.FILESIZE'

    def __init__(self, config):
        # routing key follows pattern of <document type>.<task key>
        # task key is 'FILESIZE' for this worker, and we listen
        # for any possible type, so '#'
        super().__init__(queue=self.__queue_name, 
                binding_key=self.__binding_key, config=config)

    def callback(self, task, doc):
        # Typically the target url will have to be downloaded first
        # here we assume its some local file

        if exists(doc.target['url']):
            fs = getsize(doc.target['url'])
            return json.dumps({'state': 200,
                'message': 'Success',
                'size': fs})
        else: 
            return json.dumps({'state': 404,
                'message': 'No file found at target url'})

if __name__ == '__main__':

    fsw = filesize_worker(cfg)
    print(' # Initialising worker. Ctrl+C to exit')

    try: 
        fsw.run()
    except KeyboardInterrupt:
        fsw.stop()
