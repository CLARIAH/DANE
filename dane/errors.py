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

class DANException(Exception):
    """Wrapper for DANE exception.
    """
    pass

class MissingEndpointError(DANException):
    """Raised when an action fails due to lack of API.
    """
    pass

class APIRegistrationError(DANException):
    """Raised when registering the API fails.
    """
    pass

class ResourceConnectionError(ConnectionError):
    """Raised when a component cant connect to a resource it depends on.

    Used for catching resource specific errors, and wrapping them
    in a soft blanket of custom error handling."""
    pass

class RefuseJobException(DANException):
    """Exception for workers to throw when they want to refuse a job
    at this point in time.

    This will result in a nack (no ack) being sent back to the queue, 
    causing the job to be requeued (at the or close to the head of the queue).
    """
    pass

class ConfigRequiredError(DANException):
    """Error to indicate that the base_config.yml is declared abstract,
    and that it requires a config.yml.
    """

class DocumentExistsError(DANException):
    """Raised when document does (not) exists.
    """

class TaskExistsError(DANException):
    """Raised when task does (not) exists.
    """

class ResultExistsError(DANException):
    """Raised when result does (not) exists.
    """

class TaskAssignedError(DANException):
    """Raised when task is already/not yet assigned.
    """

class UnregisteredError(DANException):
    """Raised when DANE object does not have an _id.
    """
