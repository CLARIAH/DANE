import json
import sys
from abc import ABC, abstractmethod
from DANE_utils import errors as DANError

class jobspec():
    """This is a class representation of a job in DANE, it holds both data 
    and logic.

    :param source_url: URL pointing to source material for this block
    :type source_url: str
    :param source_id: Id of the source object within the source collection
    :type source_id: str
    :param source_set: Identifier specifying the source collection the
        material is from.
    :type source_set: str
    :param tasks: A specification of the tasks to be performed
    :type source_set: class:`jobspec.taskContainer`
    :type source_set: class:`jobspec.Task`
    :type source_set: str
    :param job_id: ID of the job, assigned by DANE-core
    :type job_id: int, optional
    :param metadata: Dictionary containing metadata related to the job, or
        the source material
    :type metadata: dict, optional
    :param priority: Priority of the job in the task queue, defaults to 1
    :type priority: int, optional
    :param response: Dictionary containing results from other tasks
    :type response: dict, optional
    :param api: Reference to a class:`base_classes.base_handler` which is
        used to communicate with the database, and queueing system.
    :type api: :class:`base_classes.base_handler`, optional
    """

    def __init__(self, source_url, source_id, source_set, tasks,
            job_id=None, metadata={}, priority=1, response={}, api=None):
        """Constructor method
        """
        # TODO add more input validation
        self.source_url = source_url
        self.source_id = source_id
        self.source_set = source_set
        self.api = api
        self.job_id = job_id

        if isinstance(tasks, str) or isinstance(tasks, dict):
            tasks = parse(tasks)
        elif not isinstance(tasks, taskContainer):
            raise TypeError("Tasks should be Task, taskContainer " + \
                    "subclass, or JSON serialised task_str")
        self.tasks = tasks
        self.tasks.set_api(self.api)

        self.metadata = metadata
        self.priority = priority
        self.response = response

    def __str__(self):
        return self.to_json()

    def to_json(self):
        """Returns this job serialised as JSON

        :return: JSON string of the job
        :rtype: str
        """
        astr = []
        for kw in vars(self):
            if kw == 'tasks':
                astr.append("\"tasks\" : {}".format(getattr(self, 
                    kw).to_json()))
            elif kw == 'api':
                continue
            else: 
                astr.append("\"{}\" : {}".format(kw, 
                    json.dumps(getattr(self, kw))))
        return "{{ {} }}".format(', '.join(astr))

    @staticmethod
    def from_json(json_str):
        """Constructs a :class:`jobspec.jobspec` instance from a JSON string

        :param json_str: Serialised :class:`jobspec.jobspec`
        :type json_str: str
        :return: JSON string of the job
        :rtype: :class:`jobspec.jobspec`
        """
        data = json.loads(json_str)
        return jobspec(**data)

    def set_api(self, api):
        """Set the API for the job and all subtasks

        :param api: Reference to a :class:`base_classes.base_handler` which is
            used to communicate with the database, and queueing system.
        :type api: :class:`base_classes.base_handler`, optional
        """
        self.api = api
        for t in self.tasks:
            t.set_api(api)

    def register(self):
        """Register this job with DANE-core, this will assign a job_id to the
        job, and a task_id to all tasks. Requires an API to be set.
        """
        if self.job_id is not None:
            raise DANError.APIRegistrationError('Job already registered')
        elif self.api is None:
            raise DANError.MissingEndpointError('No endpoint found to'\
                    'register job')

        if 'SHARED' not in self.response.keys():
            self.response['SHARED'] = {}

        self.response['SHARED'].update(self.api.get_dirs(job=self))
        self.job_id = self.api.register_job(job=self)

        for t in self.tasks:
            t.register(job_id=self.job_id)

        self.api.propagate_task_ids(job=self)

    def refresh(self):
        """Retrieves the latest information for any fields that might have
        changed their values since the creation of this job. Requires an API
        to be set.
        """
        if self.job_id is None:
            raise DANError.APIRegistrationError(
                    'Cannot refresh unregistered job')
        elif self.api is None:
            raise DANError.MissingEndpointError('No endpoint found to'\
                    'refresh job')

        job = self.api.jobFromJobId(self.job_id, get_state=True)
        self.tasks = job.tasks
        self.response = job.response
        self.metadata = job.metadata

    def run(self):
        """Run the tasks in this job.
        """
        return self.tasks.run()

    def retry(self):
        """Try to run the tasks in this job again. Unlike 
        :func:`jobspec.jobspec.run` this will attempt to run tasks which 
        encountered an error state.
        """
        return self.tasks.retry()

    def isDone(self):
        """Check if all tasks have completed.
        """
        return self.tasks.isDone()

def parse(task_str):
    """Tries to parse a serialised :class:`jobspec.Task` or
    :class:`jobspec.taskContainer`, returning the correct object.

    If input is a string, it tries to parse it as json if its still a string
    after this, then its a task str, so parse it as a Task.
    Otherwise we expect it to be a length 1 dict, with format:
    { CLASSNAME : { params } } or { CLASSNAME : [ sub_tasks ] }.
    In this case the class is expected to be a taskContainer subclass
    and the classname should start with 'task' (lowercase)

    :param task_str: Serialised :class:`jobspec.Task` or :class:`jobspec.taskContainer`
    :type task_str: str
    :type task_str: dict
    :return: Instance of :class:`jobspec.Task` or :class:`jobspec.taskContainer`
    :rtype: :class:`jobspec.Task` or :class:`jobspec.taskContainer`
    """
    if isinstance(task_str, str):
        try:
            task_str = json.loads(task_str)
        except json.JSONDecodeError:
            pass

    if isinstance(task_str, str):
        task = Task(task_str)
    elif isinstance(task_str, dict) and len(task_str) == 1:
        cls, params = list(task_str.items())[0]
        if cls == 'Task':
            task = Task(**params)
        elif hasattr(sys.modules[__name__], cls) \
                and cls.startswith('task'):
            cls = getattr(sys.modules[__name__], cls)
            task = cls(params)
        else: 
            raise TypeError(
                    "{} must be Task or taskContainer subclass".format(task_str))
    else:
        raise ValueError("Expected task_str to be str or serialised class dict.")
    return task

class Task():
    def __init__(self, task_key, task_id = None, api = None, task_state=None, task_msg=None):
        if task_key is None or task_key == '':
            raise ValueError("task key cannot be empty string \"\" or None")

        self.task_key = task_key.upper()
        self.task_id = task_id
        self.task_state = task_state
        self.task_msg = task_msg
        self.api = api

    def register(self, job_id):
        if self.task_id is not None:
            raise DANError.APIRegistrationError('Task already registered')
        elif self.api is None:
            raise DANError.MissingEndpointError('No endpoint found to'\
                    'register task')

        self.task_id = self.api.register(job_id=job_id, task=self)

    def run(self):
        if self.task_id is None:
            raise DANError.APIRegistrationError('Cannot run an unregistered'\
                    'task')
        elif self.api is None:
            raise DANError.MissingEndpointError('No endpoint found'\
                    'to perform task')

        return self.api.run(task_id = self.task_id)

    def retry(self):
        if self.task_id is None:
            raise DANError.APIRegistrationError('Cannot retry an unregistered'\
                    'task')
        elif self.api is None:
            raise DANError.MissingEndpointError('No endpoint found'\
                    'to perform task')

        return self.api.retry(task_id = self.task_id)

    def isDone(self):
        if self.task_state is not None:
            return self.task_state == 200

        if self.task_id is None:
            raise DANError.APIRegistrationError('Cannot check doneness of an'\
                    'unregistered task')
        elif self.api is None:
            raise DANError.MissingEndpointError('No endpoint found to check'\
                    'task doneness against')

        return self.api.isDone(task_id = self.task_id)

    def set_api(self, api):
        self.api = api

    def apply(self, fn):
        fn(self)
        return self

    def to_json(self):
        task_data = { "task_key": self.task_key.upper(),
                "task_id": self.task_id,
                "task_state": self.task_state,
                "task_msg": self.task_msg}

        frmt = { k:v for k,v in task_data.items() if v is not None}

        if len(frmt.keys()) > 1:
            return "{{ \"Task\" : {}}}".format(json.dumps(frmt))
        else:
            return "\"{}\"".format(frmt['task_key'])

    @staticmethod
    def from_json(json_str):
        return parse(json_str)

    def __str__(self):
        return self.to_json()

class taskContainer(ABC):
    def __init__(self, name = 'taskContainer', api=None):
        self._tasks = []
        self._name = name
        self.api = api

    def add_task(self, task):
        if not isinstance(task, taskContainer) and \
                not isinstance(task, Task):
            task = parse(task)
        task.set_api(self.api)

        return self._tasks.append(task)

    # TODO should this raise an exception if all subtasks are done?
    @abstractmethod
    def run(self):
        return

    @abstractmethod
    def retry(self):
        return

    @abstractmethod
    def isDone(self):
        return

    def set_api(self, api):
        self.api = api
        for t in self._tasks:
            t.set_api(api)

    def register(self, job_id):
        for t in self._tasks:
            t.register(job_id=job_id)

    def __len__(self):
        return self._tasks.__len__()

    def __getitem__(self, key):
        return self._tasks.__getitem__(key)

    def __setitem__(self, key, value):
        return self._tasks.__setitem__(key, parse(value))

    def __delitem__(self, key, value):
        return self._tasks.__setitem__(key, value)

    def __iter__(self):
        return self._tasks.__iter__()

    def __contains__(self, item):
        return self._tasks.__contains__(item)

    def apply(self, fn):
        for t in self._tasks:
            t.apply(fn)
        fn(self)
        return self

    def to_json(self):
        tstr = []
        for t in self._tasks:
            if isinstance(t, taskContainer):
                tstr.append(t.to_json())
            elif isinstance(t, Task):
                tstr.append(t.to_json())
            else: 
                tstr.append(json.dumps(t))
        return "{{ \"{}\" : [ {} ]}}".format(self._name, ', '.join(tstr))

    @staticmethod
    def from_json(json_str, api=None):
        return parse(json_str)

    def __str__(self):
        return self.to_json()

class taskSequential(taskContainer):
    def __init__(self, tasks=[], api=None):
        super().__init__('taskSequential', api=api)

        for task in tasks:
            self.add_task(task)

    def run(self):
        for task in self:
            if not task.isDone():
                task.run()
                return

    def retry(self):
        for task in self:
            if not task.isDone():
                task.retry()
                return

    def isDone(self):
        for task in self:
            if not task.isDone():
                return False
        return True

class taskParallel(taskContainer):
    def __init__(self, tasks=[], api=None):
        super().__init__('taskParallel', api=api)

        for task in tasks:
            self.add_task(task)

    def run(self):
        for task in self:
            if not task.isDone():
                task.run()

    def retry(self):
        for task in self:
            if not task.isDone():
                task.retry()

    def isDone(self):
        for task in self:
            if not task.isDone():
                return False
        return True
