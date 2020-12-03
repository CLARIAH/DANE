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

INDEX = 'dane-index' # TODO read from config

class Result():
    """Class representation of a analysis result, containing the outcome and 
    logic for interacting with DANE-server through a 
    :class:`base_classes.base_handler`

    :param generator: Details of analysis that generated this result, requires
        `id`, `name`, `type`, and `homepage` fields.
    :type generator: dict
    :param payload: The actual result(s) to be stored
    :type payload: dict
    :param _id: id assigned by DANE-server to this task
    :type _id: int, optional
    """

    VALID_AGENTS = ["Organization", "Human", "Software"]
    
    def __init__(self, generator, payload={}, _id=None, api=None):

        if not {"id", "name", "type", "homepage"} <= generator.keys() \
                and len(generator['id']) > 2:
            raise KeyError("Generator object must contains at least the `id`," + \
                    "type, name, and homepage properties")

        if generator['name'] is None or generator['name'] == '':
            raise ValueError("Generator name cannot be empty string or None")
        generator['name'] = generator['name'].upper()

        if generator['type'] not in self.VALID_AGENTS:
            raise ValueError("Invalid generator type. Valid types are: {}".format(
                ", ".join(self.VALID_AGENTS)))

        self.generator = generator
        if isinstance(payload, dict):
            self.payload = payload
        else:
            raise TypeError('Payload must be of type dict')

        self._id = _id
        self.api = api

    def save(self, task_id):
        """Save this result, this will set an _id for the result
        
        :param task_id: id of the task that generated this result
        :return: self
        """

        self = self.api.registerResult(self, task_id)
        return self

    def delete(self):
        """Delete this result."""
        if self._id is None:
            raise KeyError("Cannot delete an unregistered result")

        return self.api.deleteResult(self)

    def to_json(self, indent=None):
        """Returns this result serialised as JSON

        :return: JSON string of the result
        :rtype: str
        """
        out = {}
        for kw in vars(self):
            if kw == '_id' and self._id is None:
                continue
            elif kw == 'api':
                continue
            else: 
                out[kw] = getattr(self, kw)

        return json.dumps({"result": out}, indent=indent)

    @staticmethod
    def from_json(json_str):
        """Constructs a :class:`DANE.Result` instance from a JSON string

        :param task_str: Serialised :class:`DANE.Result`
        :type task_str: str or dict
        :return: An initialised Result
        :rtype: :class:`DANE.Result`
        """

        data = json.loads(json_str)
        return Result(**data)

    def __str__(self):
        return self.to_json()
