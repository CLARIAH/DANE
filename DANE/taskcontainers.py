# Copyright 2020-present, Netherlands Institute for Sound and Vision (Nanne van Noord)
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##############################################################################

import json
import sys
from abc import ABC, abstractmethod
from DANE import Task
from DANE.utils import parse

class taskContainer(ABC):
    """Abstract class for describing task container template

    :param name: Name of this type of container, used for serialising
    :type name: str
    :param api: Reference to a :class:`base_classes.base_handler` which is
        used to communicate with the database, and queueing system.
    :type api: :class:`base_classes.base_handler`, optional
    """
    def __init__(self, name = 'taskContainer', api=None):
        self._tasks = []
        self._name = name
        self.api = api

    def add_task(self, task):
        """Add a single task, or a taskContainer to this taskContainer

        If the API for this taskContainer is set, it will propagate this to
        the newly added tasks.

        :param task: New task or taskContainer to be added
        :type task: :class:`DANE.Task` or :class:`DANE.taskContainer`
        :return: self
        """
        if not isinstance(task, taskContainer) and \
                not isinstance(task, Task):
            task = parse(task)
        task.set_api(self.api)

        self._tasks.append(task)
        return self

    # TODO should this raise an exception if all subtasks are done?
    @abstractmethod
    def run(self):
        """Run tasks in this container, requires tasks to be registered
        
        The order in which the tasks are run depends on the logic in the
        subclass
        """
        return

    @abstractmethod
    def retry(self):
        """Try to run these tasks again. Unlike 
        :func:`run` this will attempt to run even after  
        an error state was encountered.

        The order in which the tasks are retried depends on the logic in the
        subclass
        """
        return

    def isDone(self):
        """Check if all tasks have completed.

        :return: taskContainer doneness
        :rtype: bool
        """
        for task in self:
            if not task.isDone():
                return False
        return True

    def state(self):
        """Get state of furthest along task.

        :return: State of task
        :rtype: int
        """
        for task in self:
            if not task.isDone():
                # encountered a task that isnt done, return its state
                return task.state()
        # All child tasks are done, so this container is done
        return 200

    def set_api(self, api):
        """Set the API for itself, and all underlying tasks

        :param api: Reference to a :class:`base_classes.base_handler` which is
            used to communicate with the database, and queueing system.
        :type api: :class:`base_classes.base_handler`, optional
        :return: self
        """
        self.api = api
        for t in self._tasks:
            t.set_api(api)
        return self

    def register(self, job_id):
        """Register underlying tasks with DANE-server, this will assign a task_id
        to all tasks. Requires an API to be set.

        taskContainers do not have a formal role inside DANE-server, and as such
        they will not be assigned an id.

        :return: self
        """
        for t in self._tasks:
            t.register(job_id=job_id)
        return self

    def apply(self, fn):
        """Applies `fn` to all underlying tasks

        :param fn: Function handle in the form `fn(task)`
        :type fn: function
        :return: self
        """
        for t in self._tasks:
            t.apply(fn)
        return self

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

    def to_json(self):
        """Returns this taskContainer serialised as JSON

        :return: JSON serialisation of the taskContainer
        :rtype: str
        """
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
    def from_json(task_str):
        """
        Calls :func:`DANE.utils.parse` on the input.

        :param task_str: Serialised :class:`DANE.taskContainer`
        :type task_str: str or dict
        :return: An initialised instane of a taskContainer subclass
        :rtype: :class:`DANE.taskContainer`
        """
        return parse(task_str)

    def __str__(self):
        return self.to_json()

class taskSequential(taskContainer):
    """Task container which executes its tasks sequentially.

    :param tasks: List of :class:`DANE.Task` or :class:`DANE.taskContainer`
    :type name: list
    :param api: Reference to a :class:`base_classes.base_handler` which is
        used to communicate with the database, and queueing system.
    :type api: :class:`base_classes.base_handler`, optional
    """
    def __init__(self, tasks=[], api=None):
        super().__init__('taskSequential', api=api)

        for task in tasks:
            self.add_task(task)

    def run(self):
        for task in self:
            if not task.isDone():
                task.run()
                break
        return self

    def retry(self):
        for task in self:
            if not task.isDone():
                task.retry()
                break
        return self

class taskParallel(taskContainer):
    """Task container which executes its tasks in parallel.

    :param tasks: List of :class:`DANE.Task` or :class:`DANE.taskContainer`
    :type name: list
    :param api: Reference to a :class:`base_classes.base_handler` which is
        used to communicate with the database, and queueing system.
    :type api: :class:`base_classes.base_handler`, optional
    """
    def __init__(self, tasks=[], api=None):
        super().__init__('taskParallel', api=api)

        for task in tasks:
            self.add_task(task)

    def run(self):
        for task in self:
            if not task.isDone():
                task.run()
        return self

    def retry(self):
        for task in self:
            if not task.isDone():
                task.retry()
        return self
