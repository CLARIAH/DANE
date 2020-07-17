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
from requests.utils import requote_uri

class Document():
    """This is a class representation of a document in DANE, it holds both data 
    and logic.

    :param url: URL pointing to source material for this block
    :type url: str
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

    VALID_TYPES = ["Dataset", "Image", "Video", "Sound", "Text"]
    VALID_AGENTS = ["Organization", "Human", "Software"]

    def __init__(self, target, creator, api=None, _id=None):
        
        if not {"id", "url", "type"} <= target.keys() and len(target['id']) > 2:
            raise KeyError("Target object must contains at least the `id`," + \
                    "url, and type properties")

        if target['type'] not in self.VALID_TYPES:
            raise ValueError("Invalid target type. Valid types are: {}".format(
                ", ".join(self.VALID_TYPES)))

        self.target = target
        self.target['url'] = requote_uri(str(self.target['url']).strip())

        if not {"id", "type"} <= creator.keys():
            raise KeyError("Creator object must contains at least the `id` " + \
                    "and type properties")

        if creator['type'] not in self.VALID_AGENTS:
            raise ValueError("Invalid creator type. Valid types are: {}".format(
                ", ".join(self.VALID_AGENTS)))

        self.creator = creator

        self.api = api
        self._id = _id

    def __str__(self):
        return self.to_json()

    def to_json(self, indent=None):
        """Returns this job serialised as JSON, excluding the API reference.

        :return: JSON string of the job
        :rtype: str
        """
        out = {}
        for kw in vars(self):
            if kw == 'api':
                continue
            elif kw == '_id' and self._id is None:
                continue
            else: 
                out[kw] = getattr(self, kw)

        return json.dumps(out, indent=indent)

    @staticmethod
    def from_json(json_str):
        """Constructs a :class:`DANE.Document` instance from a JSON string

        :param json_str: Serialised :class:`DANE.Document`
        :type json_str: str
        :return: JSON string of the job
        :rtype: :class:`DANE.Document`
        """
        data = json.loads(json_str)
        return Document(**data)

    def set_api(self, api):
        """Set the API for the job and all subtasks

        :param api: Reference to a :class:`base_classes.base_handler` which is
            used to communicate with the database, and queueing system.
        :type api: :class:`base_classes.base_handler`, optional
        :return: self
        """
        self.api = api
        return self

    def register(self):
        """Register this document in DANE, this will assign a _id to the
        document. Requires an API to be set.

        :return: self
        """
        if self._id is not None:
            raise DANE.errors.APIRegistrationError('Document already registered')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to'\
                    'register document')

        self._id = self.api.registerDocument(document=self)

        return self

    def delete(self):
        """Delete this document. Requires an API to be set.
        """
        return self.api.deleteDocument(document=self)
