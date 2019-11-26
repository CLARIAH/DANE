import json
import sys

class jobspec():
    def __init__(self, source_url, source_id, source_set, tasks,
            metadata = {}, priority=1, response={}):

        # TODO add more input validation
        self.source_url = source_url
        self.source_id = source_id
        self.source_set = source_set

        if isinstance(tasks, str):
            tasks = taskContainer.from_json(tasks)
        elif isinstance(tasks, dict):
            tasks = taskContainer._from_dict(tasks)
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
            else: 
                astr.append("\"{}\" : {}".format(kw, json.dumps(getattr(self, kw))))
        return "{{ {} }}".format(', '.join(astr))

    @staticmethod
    def from_json(json_str):
        data = json.loads(json_str)
        return jobspec(**data)

class taskContainer():
    def __init__(self, name = 'taskContainer'):
        self._tasks = []
        self._name = name

    def verify_task(self, task):
        if task is None or task == '':
            raise KeyError("task cant be empty string \"\" or None")
        elif not isinstance(task, str) and not isinstance(task, taskContainer):
            raise TypeError(
                    "{} must be string or taskContainer subclass".format(
                        type(task)))
        return True

    def add_task(self, item):
        if self.verify_task(item):
            return self._tasks.append(item)

    def __len__(self):
        return self._tasks.__len__()

    def __getitem__(self, key):
        return self._tasks.__getitem__(key)

    def __setitem__(self, key, value):
        if self.verify_task(value):
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
            else: 
                tstr.append(json.dumps(t))
        return "{{ \"{}\" : [ {} ]}}".format(self._name, ', '.join(tstr))

    @staticmethod
    def from_json(json_str):
        if isinstance(json_str, str):
            data = json.loads(json_str)
        else:
            data = json_str
        return taskContainer._from_dict(data)

    @staticmethod
    def _from_dict(dct):
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
                container = cls()
                if not isinstance(container, taskContainer):
                    raise TypeError("Expected {} to be a taskContainer subclass.".format(
                            type(container)))

                container._from_json(tasks)
                return container

    def _from_json(self, data):
        if isinstance(data, dict):
            self.add_task(self._from_dict(data))
        elif isinstance(data, list):
            for task in data:
                self._from_json(task)
        elif isinstance(data, str):
            self.add_task(data) 
        else:
            raise TypeError("{} must be string, list, or dict.".format(
                        type(data)))

    def __str__(self):
        return self.to_json()

class taskSequential(taskContainer):
    def __init__(self, *args):
        super().__init__('taskSequential')

        for task in args:
            self.add_task(task)

class taskParallel(taskContainer):
    def __init__(self, *args):
        super().__init__('taskParallel')

        for task in args:
            self.add_task(task)
