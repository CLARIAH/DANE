import requests
import DANE

DANE_url = 'http://localhost:5500/DANE/job'

#task_list = DANE.taskSequential(['DEBUG'])
task_list = 'DEBUG'
task_list = { 'taskParallel' : [ 'DEBUG', 'FOO' ] }

if __name__ == '__main__':

    job = DANE.Job(source_url='some-url', 
            source_id=123,
            tasks=task_list)

    r = requests.post(DANE_url, data=job.to_json())
    if r.status_code != 201:
        raise RuntimeError(str(r.status_code) + " " + r.text)
    print(r.text)
