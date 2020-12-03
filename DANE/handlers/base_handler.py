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

from abc import ABC, abstractmethod

class base_handler(ABC):
    """Abstract base class for a handler. 

    A handler functions as the API used in DANE to facilitate all communication
    with the database and the queueing system. 
    
    :param config: Config settings for the handler
    :type config: dict
    """
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def registerDocument(self, document):
        """Register a document in the database

        :param document: The document
        :type document: :class:`DANE.Document`
        :return: document_id
        :rtype: int
        """
        return

    @abstractmethod
    def registerDocuments(self, documents):
        """Register list of documents in the database

        :param document: The document
        :type document: :class:`DANE.Document`
        :return: two lists with successfully and failed documents, as tuple
        """
        return

    @abstractmethod
    def deleteDocument(self, document):
        """Delete a document and its underlying tasks from the database

        :param document: The document
        :type document: :class:`DANE.Document`
        """
        return

    @abstractmethod
    def assignTask(self, task, document_id):
        """Assign a task to a document and run it.

        :param task: the task to assign
        :type task: :class:`DANE.Task`
        :param document_id: id of the document this task belongs to
        :type document_id: int
        :return: task_id
        :rtype: int
        """
        return

    @abstractmethod
    def assignTaskToMany(self, task, document_ids):
        """Assign a task to a document and run it.

        :param task: the task to assign
        :type task: :class:`DANE.Task`
        :param document_id: list of ids of the documents to assign this to
        :type document_id: [int]
        :return: task_ids
        :rtype: [int]
        """
        return

    @abstractmethod
    def deleteTask(self, task):
        """Delete a task.

        :param task: the task to delete
        :type task: :class:`DANE.Task`
        :return: bool 
        """
        return

    @abstractmethod
    def taskFromTaskId(self, task_id):
        """Retrieve task for a given task_id

        :param task_id: id of the task
        :type task_id: int
        :return: the task, or error if it doesnt exist
        :rtype: :class:`DANE.Task`
        """
        return
    
    @abstractmethod
    def getTaskState(self, task_id):
        """Retrieve state for a given task_id

        :param task_id: id of the task
        :type task_id: int
        :return: task_state
        :rtype: int
        """
        return

    @abstractmethod
    def getTaskKey(self, task_id):
        """Retrieve task_key for a given task_id

        :param task_id: id of the task
        :type task_id: int
        :return: task_key
        :rtype: str
        """
        return

    @abstractmethod
    def documentFromDocumentId(self, document_id):
        """Construct and return a :class:`DANE.Document` given a document_id
        
        :param document_id: The id for the document
        :type document_id: int
        :return: The document
        :rtype: :class:`DANE.Document`
        """
        return

    @abstractmethod
    def documentFromTaskId(self, task_id):
        """Construct and return a :class:`DANE.Document` given a task_id
        
        :param task_id: The id of a task
        :type task_id: int
        :return: The document
        :rtype: :class:`DANE.Document`
        """
        return

    @abstractmethod
    def registerResult(self, result, task_id):
        """Save a result for a task
        
        :param result: The result
        :type result: :class:`DANE.Result`
        :param task_id: id of the task that generated this result
        :return: self
        """
        return

    @abstractmethod
    def deleteResult(self, result):
        """Delete a result
        
        :param result: The result to delete
        :type result: :class:`DANE.Result`
        :return: bool 
        """
        return

    @abstractmethod
    def resultFromResultId(self, result_id):
        """Construct and return a :class:`DANE.Result` given a result_id
        
        :param result_id: The id of a result
        :type result_id: int
        :return: The result
        :rtype: :class:`DANE.Result`
        """

    @abstractmethod
    def searchResult(document_id, task_key):
        """Search for a result of a task with task_key applied to
        a specific document

        :param document_id: id of the document the task should be applied to
        :param task_key: key of the task that was applied
        :return: List of initialised :class:`DANE.Result` 
        """

    def isDone(self, task_id):
        """Verify whether a task is done.

        Doneness is determined by whether or not its state is `200`.
        
        :param task_id: The id of a task
        :type task_id: int
        :return: Task doneness
        :rtype: bool
        """
        return self.getTaskState(task_id) == 200

    @abstractmethod
    def run(self, task_id):
        """Run the task with this id, and change its task state to `102`.

        Running a task involves submitting it to a queue, so results might
        only be available much later. Expects a task to have state `201`,
        and it may retry tasks with state `502` or `503`.
        
        :param task_id: The id of a task
        :type task_id: int
        """
        return

    @abstractmethod
    def retry(self, task_id, force=False):
        """Retry the task with this id.

        Attempts to run a task which previously might have crashed. Defaults
        to skipping tasks with state 200, or 102, unless Force is specified,
        then it should rerun regardless of previous state.
        
        :param task_id: The id of a task
        :type task_id: int
        :param force: Force task to rerun regardless of previous state
        :type force: bool, optional
        """
        return

    @abstractmethod
    def callback(self, task_id, response):
        """Function that is called once a task gives back a response.

        This updates the state and response of the task in the database,
        and then tries to run the other tasks assigned to the document.

        :param task_id: The id of a task
        :type task_id: int
        :param response: Task response, should contain at least the `state`
            and a `message`
        :type response: dict
        """
        return

    @abstractmethod
    def updateTaskState(self, task_id, state, message):        
        """Update the state, message, and last updated of a task.

        :param task_id: The id of a task
        :type task_id: int, required
        :param state: The new task state
        :type state: int, required
        :param message: The new task message
        :type message: string, required
        """
        return

    @abstractmethod
    def search(self, target_id, creator_id):
        """Returns documents matching target_id and creator_id

        :param target_id: The id of the target
        :param creator_id: The id of the creator
        :return: list of found documents
        """
        return

    @abstractmethod
    def getUnfinished(self, only_runnable=False):
        """Returns tasks which are not finished, i.e., 
        tasks that dont have state `200` 

        :param only_runnable: Return only tasks that can be `run()`
        :return: ids of found tasks
        :rtype: dict
        """
        return

    @abstractmethod
    def getAssignedTasks(self, document_id, task_key=None):
        """Retrieve tasks assigned to a document. Accepts an optional
        task_key to filter for a specific type of tasks.

        :param document_id: document to of interest
        :param task_key: Key of task type to filter for
        :type task_key: string, optional
        :return: list of dicts with task ids, keys, and states."""
        return
