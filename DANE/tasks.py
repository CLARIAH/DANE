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
from DANE.utils import parse

class Task():
    """Class representation of a task, contains job information and has logic
    for interacting with DANE-server through a :class:`base_classes.base_handler`

    :param task_key: Key of the task, should match a binding key of a worker
    :type task_key: str
    :param task_id: id assigned by DANE-server to this task
    :type task_id: int, optional
    :param api: Reference to a :class:`base_classes.base_handler` which is
        used to communicate with the database, and queueing system.
    :type api: :class:`base_classes.base_handler`, optional
    :param task_state: Status code representing task state
    :type task_state: int, optional
    :param task_msg: Textual message accompanying the state
    :type task_msg: str, optional
    :param job_id: id of parent job, assigned by DANE-server to this task
    :type job_id: int, optional
    """
    def __init__(self, task_key, task_id = None, api = None, 
            task_state=None, task_msg=None, job_id = None):
        if task_key is None or task_key == '':
            raise ValueError("task key cannot be empty string \"\" or None")

        self.task_key = task_key.upper()
        self.task_id = task_id
        self.task_state = task_state
        self.task_msg = task_msg
        self.api = api
        self.job_id = job_id

    def register(self, job_id):
        """Register this task with DANE-server, this will assign a task_id to the
        task. Requires an API to be set.
        
        :return: self
        """
        if self.task_id is not None or self.job_id is not None:
            raise DANE.errors.APIRegistrationError('Task already registered')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to'\
                    'register task')

        self.task_id = self.api.register(job_id=job_id, task=self)
        self.job_id = job_id
        return self

    def run(self):
        """Run this task, requires it to be registered
        
        :return: self
        """
        if self.task_id is None:
            raise DANE.errors.APIRegistrationError('Cannot run an unregistered'\
                    'task')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found'\
                    'to perform task')

        self.api.run(task_id = self.task_id)
        return self

    def retry(self, force=False):
        """Try to run this task again. Unlike 
        :func:`run` this will attempt to run even after  
        an error state was encountered.
        
        :param force: Force task to rerun regardless of previous state
        :type force: bool, optional
        :return: self
        """
        if self.task_id is None:
            raise DANE.errors.APIRegistrationError('Cannot retry an '\
                    'unregistered task')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found'\
                    'to perform task')

        self.api.retry(task_id = self.task_id, force=force)
        return self

    def reset(self):
        """Reset the task state to `201`

        This can be used to force tasks to re-run after a preceding task
        has completed. Typically, the preceding task will be retried with
        `force=True`.
        
        :return: self
        """
        if self.task_id is None:
            raise DANE.errors.APIRegistrationError('Cannot retry an '\
                    'unregistered task')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found'\
                    'to perform task')

        self.api.updateTaskState(self.task_id, 201, 'Reset')
        return self

    def refresh(self):
        """Retrieves the latest information for task state and msg which might 
        have changed their values since the creation of this task. Requires an 
        API to be set

        :return: self
        """
        if self.task_id is None:
            raise DANE.errors.APIRegistrationError('Cannot refresh an '\
                    'unregistered task')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to'\
                    'refresh task')

        task = self.api.taskFromTaskId(self.task_id)
        self.task_state = task.task_state
        self.task_msg = task.task_msg
        return self

    def isDone(self):
        """ Check if this task has been completed. 

        A task is completed if it's `task_state` equals 200. This will
        consult the API if the task_state isn't set.

        :return: Task doneness
        :rtype: bool
        """
        if self.task_state is not None:
            return self.task_state == 200

        if self.task_id is None:
            raise DANE.errors.APIRegistrationError('Cannot check doneness of an'\
                    'unregistered task')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to check'\
                    'task doneness against')

        return self.api.isDone(task_id = self.task_id)

    def state(self):
        """ Get task state of this job. 

        :return: Task state 
        :rtype: int
        """
        if self.task_state is not None:
            return self.task_state

        if self.task_id is None:
            raise DANE.errors.APIRegistrationError('Cannot get state of an'\
                    'unregistered task')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to get'\
                    'task state from')

        return self.api.getTaskState(task_id = task_id)

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

    def to_json(self):
        """Returns this task serialised as JSON

        :return: JSON serialisation of the task
        :rtype: str
        """
        task_data = { "task_key": self.task_key.upper(),
                "task_id": self.task_id,
                "task_state": self.task_state,
                "task_msg": self.task_msg,
                "job_id": self.job_id}

        frmt = { k:v for k,v in task_data.items() if v is not None}

        if len(frmt.keys()) > 1:
            return "{{ \"Task\" : {}}}".format(json.dumps(frmt))
        else:
            return "\"{}\"".format(frmt['task_key'])

    @staticmethod
    def from_json(task_str):
        """Calls :func:`DANE.parse` on the input.

        :param task_str: Serialised :class:`DANE.Task`
        :type task_str: str or dict
        :return: An initialised Task
        :rtype: :class:`DANE.Task`
        """
        return parse(task_str)

    def __str__(self):
        return self.to_json()
