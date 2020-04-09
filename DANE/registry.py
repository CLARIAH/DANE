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

import requests
import json
import subprocess
from urllib.parse import urljoin

class Registry():
    """The DANE registry is a utility for communicating with an annotation
    server. This annotation server is used as a persistent index to link
    worker outputs to the documents they were obtained from. 

    This registry is intended to store a reference to the worker output,
    not the worker output itself, as to not overload the annotation server.

    :param config: Configuration settings 
    :type config: :class:`DANE.config`
    """

    def __init__(self, config):
        self.annotation_api = config.REGISTRY.API
        self.token = config.REGISTRY.TOKEN
        self.user = config.REGISTRY.USER
        self.datastore = config.REGISTRY.datastore

    def save(self, entry):
        """
        Save an entry in the registry, will update if given entry
        has a `registry_id`
        
        :param entry: Entry to be saved
        :type entry: :class:`RegistryEntry`
        :return: Returns the json-encoded response text
        """
        method = 'POST'
        url = urljoin(self.annotation_api, 'annotation')

        if entry.registry_id is not None:
            method = 'PUT'
            url = urljoin(url, entry.registry_id)

        resp = requests.request(method, url, json=entry.to_json())
        if resp.status_code == 200:
            return resp.json()
        else:
            return None

    def get(self, registry_id):
        """
        Retrieve an entry from the registry by their registry_id

        :param registry_id: Id in registry of the entry to be retrieved
        :return: Registry entry or None if not found
        :rtype: class:`RegistryEntry`
        """
        url = urljoin(self.annotation_api, 'annotation', registry_id)

        resp = requests.request("GET", url)
        if resp.status_code == 200:
            return RegistryEntry.from_json(resp.json())
        else:
            return None

    def delete(self, entry):
        """
        Delete an entry from the registry.

        :param entry: Entry to be saved
        :type entry: :class:`RegistryEntry`
        :return: Whether or not the deletion succeeded
        :rtype: bool
        """
        if entry.registry_id is not None:
            url = urljoin(self.annotation_api, 'annotation', entry.registry_id)
            resp = requests.request("DELETE", url)
            if resp.status_code == 200:
                return True
            else:
                return False
        else:
            raise ValueError('Entry does not have a registry_id')

class RegistryEntry():
    """Class representation of an entry in the DANE registry.

    :param _id: PID of the document this entry concerns
    :param _type: type of the source document
    :param worker_name: Task key of the DANE worker
    :param worker_version: Version of the DANE worker
    :param worker_url: URL to website/git repo of the worker
    :param registry_id: (optional) id of the entry in the registry
    """

    VALID_TYPES = ["Dataset", "Image", "Video", "Sound", "Text"]

    def __init__(self, _id, _type, worker_name, worker_version,
            worker_url, registry_id=None):
        self._id = _id

        _type = _type.lower().capitalize()
        if _type not in self.VALID_TYPES:
            raise ValueError('Invalid type, expecting one of: {}'.format(
                ', '.join(self.VALID_TYPES)))
        self._type = _type

        self.worker_name = worker_name
        self.worker_version = worker_version
        self.worker_url = worker_url

        self.registry_id = registry_id

    def to_json(self):
        """
        Returns this entry serialised as JSON

        :return: JSON serialisation of this entry
        :rtype: str
        """
        render = {}
        render["@context"] = "http://www.w3.org/ns/anno.jsonld"
        render["type"] = "Annotation"

        if self.registry_id is not None:
            render["id"] = str(self.registry_id)

        render["target"] = {
            "id": str(self._id),
            "type": str(self._type)
        }

        render["generator"] = {
            "id": self.worker_version,
            "type": "Software",
            "name": self.worker_name,
            "homepage": self.worker_url
        }
        
        return json.dumps(render, indent=2)

    @staticmethod
    def from_json(obj):
        """
        :param obj: Dict representation of a the entry
        :type obj: dict
        :return: New Registry entry
        :rtype: class:`RegistryEntry`
        """
        return RegistryEntry(_id=obj["target"]["id"], 
            _type=obj["target"]["type"],
            worker_name=obj["generator"]["name"],
            worker_version=obj["generator"]["id"],
            worker_url=obj["generator"]["homepage"],
            registry_id=obj["id"])

def git_helper(git_dir):
    """
    Helper functions that returns the output of `git describe --always` and
    `git remote get-url origin`, which can be used as the values for  
    `worker_version` and `worker_url` in a :class:`registry.RegistryEntry`.

    :param git_dir: Directory to retrieve git information from
    :type git_dir: str
    :return: 
        - version - commit version
        - url - url to git origin
    """
    version = subprocess.check_output(["git", "describe", "--always"], 
            cwd=git_dir).strip().decode('ascii')
    url = subprocess.check_output(["git", "remote", "get-url", "origin"], 
            cwd=git_dir).strip().decode('ascii')
    return version, url
