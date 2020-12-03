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
    and some logic.

    :param target: Dict containing `id`, `url`, and `type` keys to described
        the target document.
    :type target: dict
    :param creator: Dict containing `id`, and `type` keys to describe the
        document owner/creator.
    :type creator: dict
    :param api: Reference to a class:`base_classes.base_handler` which is
        used to communicate with the server.
    :type api: :class:`base_classes.base_handler`, optional
    :param _id: ID of the document, assigned by DANE-server
    :type _id: int, optional
    :param created_at: Creation date
    :param updated_at: Last modified date
    """

    VALID_TYPES = ["Dataset", "Image", "Video", "Sound", "Text"]
    VALID_AGENTS = ["Organization", "Human", "Software"]

    def __init__(self, target, creator, api=None, _id=None, 
            created_at=None, updated_at=None):
        
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

        self.created_at = created_at
        self.updated_at = updated_at

        self.api = api
        self._id = _id

    def __str__(self):
        return self.to_json()

    def to_json(self, indent=None):
        """Returns this document serialised as JSON, excluding the API reference.

        :return: JSON string of the document
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
        :type json_str: str or dict
        :return: JSON string of the document
        :rtype: :class:`DANE.Document`
        """

        if isinstance(json_str, str):
            json_str = json.loads(json_str)

        return Document(**json_str)

    def set_api(self, api):
        """Set the API for the document

        :param api: Reference to a :class:`base_classes.base_handler` which is
            used to communicate with the database, and queueing system.
        :type api: :class:`base_classes.base_handler`, optional
        :return: self
        """
        self.api = api
        return self

    def register(self):
        """Register this document in DANE, this will assign an _id to the
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
        if self.api is None:
            raise DANE.errors.MissingEndpointError('No API found')

        return self.api.deleteDocument(document=self)

    def getAssignedTasks(self, task_key = None):
        """Retrieve tasks assigned to this document. Accepts an optional
        task_key to filter for a specific type of tasks. Requires an
        API to be set.

        :param task_key: Key of task type to filter for
        :type task_key: string, optional
        :return: list of dicts with task keys and ids."""

        if self._id is None:
            raise DANE.errors.APIRegistrationError('Document needs to be registered')
        elif self.api is None:
            raise DANE.errors.MissingEndpointError('No endpoint found to'\
                    'query tasks')

        return self.api.getAssignedTasks(self._id, task_key)
