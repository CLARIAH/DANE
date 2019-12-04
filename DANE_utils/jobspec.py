import json
import sys

class MissingEndpointError(Exception):
    pass

class APIRegistrationError(Exception):
    pass

class jobspec():
    def __init__(self, source_url, source_id, source_set, tasks,
            job_id=None, metadata={}, priority=1, response={}, api=None):

        # TODO add more input validation
        self.source_url = source_url
        self.source_id = source_id
        self.source_set = source_set
        self.api = api
        self.job_id = job_id

        if isinstance(tasks, str):
            tasks = taskContainer.from_json(tasks, self.api)
        elif isinstance(tasks, dict):
            tasks = taskContainer._from_dict(tasks, self.api)
        elif not isinstance(tasks, taskContainer):
            raise TypeError("Tasks should be taskContainer " + \
                    "subclass, or JSON serialised taskContainer")
        self.tasks = tasks

        self.metadata = metadata
        self.priority = priority
        self.response = response

    def __str__(self):
        return self.to_json()

    def to_json(self):
        astr = []
        for kw in vars(self):
            if kw == 'tasks':
                astr.append("\"tasks\" : {}".format(getattr(self, kw).to_json()))
            elif kw == 'api':
                continue
            else: 
                astr.append("\"{}\" : {}".format(kw, json.dumps(getattr(self, kw))))
        return "{{ {} }}".format(', '.join(astr))

    @staticmethod
    def from_json(json_str):
        data = json.loads(json_str)
        return jobspec(**data)

    def set_api(self, api):
        self.api = api
        for t in self.tasks:
            t.set_api(api)

    def register(self):
        if self.job_id is not None:
            raise APIRegistrationError('Job already registered')
        elif self.api is None:
            raise MissingEndpointError('No endpoint found to register job')

        self.job_id = self.api.register_job(job=self)

        for t in self.tasks:
            t.register(job_id=self.job_id)

        self.api.propagate_task_id(job=self)

    def run(self):
        return self.tasks.run()

    def isDone(self):
        return self.tasks.isDone()

class Task():
    def __init__(self, task_key, _id = None, api = None):
        if task_key is None or task_key == '':
            raise ValueError("task key cannot be empty string \"\" or None")
        elif ":" in task_key:
            if _id is not None:
                raise ValueError("Invalid task_key, `:` not permitted in task_key when _id is provided")
            else:
                _id, task_key = task_key.split(":")

        self.task_key = task_key
        self.task_id = _id
        self.api = api

    def register(self, job_id):
        if self.task_id is not None:
            raise APIRegistrationError('Task already registered')
        elif self.api is None:
            raise MissingEndpointError('No endpoint found to register task')

        self.task_id = self.api.register(job_id=job_id, task=self)

    def run(self):
        if self.task_id is None:
            raise APIRegistrationError('Cannot run an unregistered task')
        elif self.api is None:
            raise MissingEndpointError('No endpoint found to perform task')

        return self.api.run(task_id = self.task_id)

    def isDone(self):
        if self.task_id is None:
            raise APIRegistrationError('Cannot check doneness of an unregistered task')
        elif self.api is None:
            raise MissingEndpointError('No endpoint found to check task doneness against')

        return self.api.isDone(task_id = self.task_id)

    def to_json(self):
        if self.task_id is None:
            return "\"{}\"".format(self.task_key)
        else:
            return "\"{}:{}\"".format(self.task_id, self.task_key)

    def set_api(self, api):
        self.api = api

    @staticmethod
    def from_json(json_str):
        return Task(json_str)

    def __str__(self):
        return self.to_json()

class taskContainer():
    def __init__(self, name = 'taskContainer', api=None):
        self._tasks = []
        self._name = name
        self.api = api

    def add_task(self, task):
        if isinstance(task, str):
            task = Task(task, api=self.api)

        if self._verify_task(task):
            return self._tasks.append(task)

    # TODO should this raise an exception if all subtasks are done?
    def run(self):
        return NotImplementedError('Subclasses of taskContainer should implement run method')

    def isDone(self):
        return NotImplementedError('Subclasses of taskContainer should implement isDone method')

    def _verify_task(self, task):
        if not isinstance(task, Task) and not isinstance(task, taskContainer):
            raise TypeError(
                    "{} must be Task or taskContainer subclass".format(
                        type(task)))
        return True

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
        if self._verify_task(value):
            return self._tasks.__setitem__(key, value)

    def __delitem__(self, key, value):
        return self._tasks.__setitem__(key, value)

    def __iter__(self):
        return self._tasks.__iter__()

    def __contains__(self, item):
        return self._tasks.__contains__(item)

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
        if isinstance(json_str, str):
            data = json.loads(json_str)
        else:
            data = json_str
        return taskContainer._from_dict(data)

    @staticmethod
    def _from_dict(dct, api=None):
        if len(dct) != 1:
            raise ValueError("Expected dict to be in { <classname> : [<tasks>] } format.")
        else:
            cls, tasks = list(dct.items())[0]
            
            if not hasattr(sys.modules[__name__], cls) or not cls.startswith('task'):
                raise TypeError("Invalid container name," \
                        "expected {} to be taskContainer subclass.".format(
                            cls))
            else:
                cls = getattr(sys.modules[__name__], cls)
                container = cls(api=api)
                if not isinstance(container, taskContainer):
                    raise TypeError("Expected {} to be a taskContainer subclass.".format(
                            type(container)))

                container._from_json(tasks)
                return container

    def _from_json(self, data, api=None):
        if isinstance(data, dict):
            self.add_task(self._from_dict(data, api))
        elif isinstance(data, list):
            for task in data:
                self._from_json(task, api)
        elif isinstance(data, str):
            self.add_task(data) 
        else:
            raise TypeError("{} must be string, list, or dict.".format(
                        type(data)))

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
            task.run()

    def isDone(self):
        for task in self:
            if not task.isDone():
                return False
        return True
