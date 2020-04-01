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
import DANE
from DANE.utils import parse


class Job():
    """This is a class representation of a job in DANE, it holds both data 
    and logic.

    :param source_url: URL pointing to source material for this block
    :type source_url: str
    :param source_id: Id of the source object within the source collection
    :type source_id: str
    :param source_set: Deprecated parameter, will be removed in the future.
    :type source_set: str
    :param tasks: A specification of the tasks to be performed
    :type tasks: :class:`DANE.taskContainer` or :class:`DANE.Task`
    :param job_id: ID of the job, assigned by DANE-server
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

    def __init__(self, source_url, source_id, tasks, source_set=None,
            job_id=None, metadata={}, priority=1, response={}, api=None):
        # TODO add more input validation
        self.source_url = str(source_url).strip()
        self.source_id = str(source_id).strip()
        self.source_set = str(source_set).strip()
        self.api = api
        self.job_id = job_id

        if len(self.source_url) < 1 or len(self.source_id) < 1:
            raise ValueError('Source url and id required')

        if isinstance(tasks, str) or isinstance(tasks, dict):
            tasks = parse(tasks)
        elif not isinstance(tasks, DANE.taskContainer):
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
        """Returns this job serialised as JSON, excluding the API reference.

        :return: JSON string of the job
        :rtype: str
        """
        astr = []
        for kw in vars(self):
            if kw == 'tasks':
                astr.append("\"tasks\" : {}".format(getattr(self, 
                    kw).to_json()))
            elif kw == 'api' or kw == 'source_set':
                continue
            else: 
                astr.append("\"{}\" : {}".format(kw, 
                    json.dumps(getattr(self, kw))))
        return "{{ {} }}".format(', '.join(astr))

    @staticmethod
    def from_json(json_str):
        """Constructs a :class:`DANE.Job` instance from a JSON string

        :param json_str: Serialised :class:`DANE.Job`
        :type json_str: str
        :return: JSON string of the job
        :rtype: :class:`DANE.Job`
        """
        data = json.loads(json_str)
        return Job(**data)

    def set_api(self, api):
        """Set the API for the job and all subtasks

        :param api: Reference to a :class:`base_classes.base_handler` which is
            used to communicate with the database, and queueing system.
        :type api: :class:`base_classes.base_handler`, optional
        :return: self
        """
        self.api = api
        self.tasks.set_api(api)
        return self

    def register(self):
        """Register this job in DANE, this will assign a job_id to the
        job, and a task_id to all tasks. Requires an API to be set.

        :return: self
        """
        if self.job_id is not None:
            raise DANE.errors.APIRegistrationError('Job already registered')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to'\
                    'register job')

        if 'SHARED' not in self.response.keys():
            self.response['SHARED'] = {}

        self.response['SHARED'].update(self.api.get_dirs(job=self))
        self.job_id = self.api.register_job(job=self)

        self.tasks.register(job_id=self.job_id)

        self.api.propagate_task_ids(job=self)
        return self

    def delete(self):
        """Delete this job and any attached tasks. Requires an API to be set.
        """
        return self.api.delete_job(job=self)

    def refresh(self):
        """Retrieves the latest information for any fields that might have
        changed their values since the creation of this job. Requires an API
        to be set

        :return: self
        """
        if self.job_id is None:
            raise DANE.errors.APIRegistrationError(
                    'Cannot refresh unregistered job')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to'\
                    'refresh job')

        job = self.api.jobFromJobId(self.job_id, get_state=True)
        self.tasks = job.tasks
        self.response = job.response
        self.metadata = job.metadata
        return self

    def apply(self, fn):
        """Applies `fn` to all :class:`DANE.Task` belonging to this job

        :param fn: Function handle in the form `fn(task)`
        :type fn: function
        :return: self
        """
        self.tasks.apply(fn)
        return self

    def run(self):
        """Run the tasks in this job.

        :return: self
        """
        self.tasks.run()
        return self

    def retry(self):
        """Try to run the tasks in this job again. Unlike 
        :func:`run` this will attempt to run tasks which 
        encountered an error state.

        :return: self
        """
        self.tasks.retry()
        return self

    def isDone(self):
        """Check if all tasks have completed.

        :return: Job doneness
        :rtype: bool
        """
        return self.tasks.isDone()

    def state(self):
        """Get state of furthest along task.

        :return: State of task
        :rtype: int 
        """
        return self.tasks.state()
