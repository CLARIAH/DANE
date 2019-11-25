from DANE_utils.base_classes import base_worker
from os.path import getsize
import json

class filesize_worker(base_worker):

    def __init__(self, host):
        # routing key follows pattern of <file type>.<worker type>
        # worker type is 'filesize' for this worker, and we listen
        # for any possible filetype, so '#'
        # listen for filesize jobs (regardless of file type)
        super().__init__(host, 'filesize_queue', '#.filesize')

    def callback(self, job_request):
        print('Got request', job_request)
        fs = getsize(job_request['file'])

        return json.dumps({'filesize': fs})

if __name__ == '__main__':
    fsw = filesize_worker('localhost')
    print(' # Initialising worker. Ctrl+C to exit')

    try: 
        fsw.run()
    except KeyboardInterrupt:
        fsw.stop()
