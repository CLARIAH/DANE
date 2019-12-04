from DANE_utils.base_classes import base_handler
import uuid

class DummyHandler(base_handler):
    def __init__(self):
        super().__init__({}) # pass empty config
        self.job_register = {}
        self.task_register = {}

    def register_job(self, job):
        idx = str(uuid.uuid4())
        self.job_register[idx] = job
        return idx

    def register(self, job_id, task):
        if job_id in self.job_register.keys():
            idx = str(len(self.task_register))
            # store job state as HTTP response codes
            self.task_register[idx] = (task, 202, job_id)
        else:
            raise APIRegistrationError('Unregistered job or unknown job id! Register job first')
        return idx

    def getTaskState(self, task_id):
        return self.task_register[task_id][1]

    def run(self, task_id):
        task, state, job_id = self.task_register[task_id]
        self.task_register[task_id] = (task, 200, job_id)
        print('DummyEndpoint: Executed task {} for job: {}'.format(task.task_key, job_id))
