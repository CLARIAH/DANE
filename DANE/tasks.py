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
import DANE.errors
from collections.abc import Iterable

class Task():
    """Class representation of a task, contains task information and has logic
    for interacting with DANE-server through a :class:`base_classes.base_handler`

    :param key: Key of the task, should match a binding key of a worker
    :type key: str
    :param priority: Priority to give to this task in queue. Defautls to 1.
    :type priority: int
    :param _id: id assigned by DANE-server to this task
    :type _id: int, optional
    :param api: Reference to a :class:`base_classes.base_handler` which is
        used to communicate with the database, and queueing system.
    :type api: :class:`base_classes.base_handler`, optional
    :param state: Status code representing task state
    :type state: int, optional
    :param msg: Textual message accompanying the state
    :type msg: str, optional
    :param created_at: Creation date
    :param updated_at: Last modified date
    :param \**kwargs: Arbitrary keyword arguments. Will be stored in task.args 
    """
    def __init__(self, key, priority=1, _id = None, api = None, 
            state=None, msg=None, created_at=None, updated_at=None, **kwargs):
        if key is None or key == '':
            raise ValueError("task key cannot be empty string \"\" or None")

        self.key = key.upper()
        self.priority = max(0, min(int(priority), 10))
        self._id = _id
        self.state = state
        self.msg = msg
        self.api = api

        self.created_at = created_at
        self.updated_at = updated_at

        if len(kwargs) == 1 and list(kwargs.keys())[0] == 'args':
            self.args = kwargs['args']
        else:
            self.args = kwargs

    def assign(self, document_id):
        """Assign a task to a document, this will set an _id for the
        task and run it. Requires an API to be set.
        
        :param document_id: id of document to assign this task to.
        :return: self
        """
        if self._id is not None:
            raise DANE.errors.APIRegistrationError('Task already assigned')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to'\
                    ' assign task')

        self = self.api.assignTask(task=self, document_id=document_id)
        return self

    def assignMany(self, document_ids):
        """Assign this task to multiple documents and run it. 
        Requires an API to be set.
        """
        if self._id is not None:
            raise DANE.errors.APIRegistrationError('Cannot call assignMany'\
                    ' on an assigned task')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to'\
                    ' assign task')

        if not isinstance(document_ids, Iterable) \
                or isinstance(document_ids, str):
            raise TypeError('document_ids must be iterable')

        return self.api.assignTaskToMany(task=self, document_ids=document_ids)

    def delete(self):
        """Delete this task, requires it to be registered

        :return: bool
        """
        if self._id is None:
            raise KeyError("Cannot delete an unregistered task")

        return self.api.deleteTask(self)

    def run(self):
        """Run this task, requires it to be registered
        
        :return: self
        """
        if self._id is None:
            raise DANE.errors.APIRegistrationError('Cannot run an unassigned'\
                    'task')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found'\
                    'to perform task')

        self.api.run(task_id = self._id)
        return self

    def retry(self, force=False):
        """Try to run this task again. Unlike 
        :func:`run` this will attempt to run even after  
        an error state was encountered.
        
        :param force: Force task to rerun regardless of previous state
        :type force: bool, optional
        :return: self
        """
        if self._id is None:
            raise DANE.errors.APIRegistrationError('Cannot retry an '\
                    'unassigned task')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found'\
                    'to perform task')

        self.api.retry(task_id = self._id, force=force)
        return self

    def reset(self):
        """Reset the task state to `201`

        This can be used to force tasks to re-run after a preceding task
        has completed. Typically, the preceding task will be retried with
        `force=True`.
        
        :return: self
        """
        if self._id is None:
            raise DANE.errors.APIRegistrationError('Cannot retry an '\
                    'unassigned task')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found'\
                    'to perform task')

        self.api.updateTaskState(self._id, 201, 'Reset')
        return self

    def refresh(self):
        """Retrieves the latest information for task state and msg which might 
        have changed their values since the creation of this task. Requires an 
        API to be set

        :return: self
        """
        if self._id is None:
            raise DANE.errors.APIRegistrationError('Cannot refresh an '\
                    'unassigned task')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to'\
                    'refresh task')

        task = self.api.taskFromTaskId(self._id)
        self.state = task.state
        self.msg = task.msg
        return self

    def isDone(self):
        """ Check if this task has been completed. 

        A task is completed if it's `state` equals 200. This will
        consult the API if the state isn't set.

        :return: Task doneness
        :rtype: bool
        """
        if self.state is not None:
            return self.state == 200

        if self._id is None:
            raise DANE.errors.APIRegistrationError('Cannot check doneness of an'\
                    'unassigned task')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to check'\
                    'task doneness against')

        return self.api.isDone(task_id = self._id)

    def state(self):
        """ Get task state of this task. 

        :return: Task state 
        :rtype: int
        """
        if self.state is not None:
            return self.state

        if self._id is None:
            raise DANE.errors.APIRegistrationError('Cannot get state of an'\
                    'unassigned task')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to get'\
                    'task state from')

        return self.api.getTaskState(task_id = self._id)

    def set_api(self, api):
        """Set the API for this task

        :param api: Reference to a :class:`base_classes.base_handler` which is
            used to communicate with the database, and queueing system.
        :type api: :class:`base_classes.base_handler`, optional
        :return: self
        """
        self.api = api
        return self

    def apply(self, fn):
        """Applies `fn` to self

        :param fn: Function handle in the form `fn(task)`
        :type fn: function
        :return: self
        """
        fn(self)
        return self

    def to_json(self, indent=None):
        """Returns this task serialised as JSON

        :return: JSON serialisation of the task
        :rtype: str
        """
        task_data = { "key": self.key.upper(),
                "_id": self._id,
                "state": self.state,
                "msg": self.msg,
                "created_at": self.created_at,
                "updated_at": self.updated_at,
                "priority": self.priority}

        if len(self.args) > 0:
            task_data['args'] = self.args

        out = { k:v for k,v in task_data.items() if v is not None}
        return json.dumps(out, indent=indent)

    @staticmethod
    def from_json(task_str):
        """Constructs a :class:`DANE.Task` instance from a JSON string

        :param task_str: Serialised :class:`DANE.Task`
        :type task_str: str or dict
        :return: An initialised Task
        :rtype: :class:`DANE.Task`
        """

        if isinstance(task_str, str):
            task_str = json.loads(task_str)

        if isinstance(task_str, dict) and len(task_str) == 1:
            cls, params = list(task_str.items())[0]
            if cls.lower() == 'key':
                task = DANE.Task(key=params)
            elif cls.lower() == 'task':
                task = DANE.Task(**params)
            else: 
                raise TypeError(
                        "{} must be Task subclass".format(task_str))
        elif "task" in task_str.keys():
            task_str = {**task_str['task'], **task_str }
            del task_str['task']
            task = DANE.Task(**task_str)
        else:
            task = DANE.Task(**task_str)

        return task

    def __str__(self):
        return self.to_json()

    def __copy__(self):
        return Task(self.key, self.priority, self._id, 
                self.api, self.state, self.msg, **self.args)
