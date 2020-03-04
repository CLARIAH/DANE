from DANE.base_classes import base_handler
import uuid

class DummyHandler(base_handler):
    def __init__(self):
        super().__init__({}) # pass empty config
        self.job_register = {}
        self.task_register = {}

    def register_job(self, job):
        idx = str(uuid.uuid4())
        job.job_id = idx
        self.job_register[idx] = job
        return idx

    def delete_job(self, job):
        del self.job_register[job.job_id]

    def propagate_task_ids(self, job):
        self.job_register[job.job_id] = job

    def register(self, job_id, task):
        if job_id in self.job_register.keys():
            idx = str(len(self.task_register))
            # store job state as HTTP response codes
            self.task_register[idx] = (task, 202, job_id)
        else:
            raise APIRegistrationError('Unregistered job or unknown job id!'\
                    'Register job first')
        return idx

    def taskFromTaskId(self, task_id):
        return self.task_register[task_id]

    def getTaskState(self, task_id):
        return self.taskFromTaskId(task_id)[1]

    def getTaskKey(self, task_id):
        return self.taskFromTaskId(task_id)[0]

    def jobFromJobId(self, job_id):
        return self.job_register[job_id] 

    def jobFromTaskId(self, task_id):
        return self.job_register[self.task_register[task_id][2]] 

    def run(self, task_id):
        task, state, job_id = self.task_register[task_id]
        self.task_register[task_id] = (task, 200, job_id)
        print('DummyEndpoint: Executed task {} for '\
                'job: {}'.format(task.task_key, job_id))

    def retry(self, task_id):
        task, state, job_id = self.task_register[task_id]
        self.task_register[task_id] = (task, 200, job_id)
        print('DummyEndpoint: Retried task {} for '\
                'job: {}'.format(task.task_key, job_id))

    def callback(self, task_id, response):
        print('DummyEndpoint: Callback response {} for '\
                'task_id: {}'.format(response, task_id))

    def get_dirs(self, job):
        return {
            'TEMP_FOLDER': './',
            'OUT_FOLDER': './'
        }

    def updateTaskState(self, task_id, state, message, response=None):        
        _, _, job_id = self.task_register[task_id]
        self.task_register[task_id] = (state, message, job_id)

    def search(self, source_id, source_set=None):
        return

    def getUnfinished(self):
        return [i for t,s,i in self.task_register if s == 200]

