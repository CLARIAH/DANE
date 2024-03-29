from dane.handlers.base_handler import BaseHandler
from dane.errors import APIRegistrationError
from dane.state import ProcState
from dane import Result, Task, Document
from typing import List, Optional
import uuid


class DummyHandler(BaseHandler):
    def __init__(self):
        super().__init__({})  # pass empty config
        self.doc_register = {}
        self.task_register = {}

    def registerDocument(self, document):
        idx = str(uuid.uuid4())
        document._id = idx
        self.doc_register[idx] = document
        return idx

    def deleteDocument(self, document):
        del self.doc_register[document.document_id]

    # Note: added to make test_dane.py work, but does nothing yet
    def assignTaskToMany(self, task, document_ids):
        return

    # Note: added to make test_dane.py work, but does nothing yet
    def registerDocuments(self, documents):
        return

    def assignTask(self, task, document_id):
        if document_id in self.doc_register.keys():
            idx = str(len(self.task_register))
            task._id = idx
            # store document state as HTTP response codes
            self.task_register[idx] = (task, document_id)
            task.state = ProcState.CREATED.value
            task.msg = "Created"
        else:
            raise APIRegistrationError(
                "Unregistered document or unknown document id!"
                "Register document first"
            )
        return task.run()

    def deleteTask(self, task):
        del self.task_register[task._id]

    def taskFromTaskId(self, task_id):
        return self.task_register[task_id]

    def getTaskState(self, task_id):
        return self.taskFromTaskId(task_id)[1]

    def getTaskKey(self, task_id):
        return self.taskFromTaskId(task_id)[0]

    def documentFromDocumentId(self, document_id):
        return self.doc_register[document_id]

    def documentFromTaskId(self, task_id):
        return self.doc_register[self.task_register[task_id][2]]

    def run(self, task_id):
        task, document_id = self.task_register[task_id]
        self.updateTaskState(task._id, ProcState.SUCCESS.value, "Success!")
        print(
            "DummyEndpoint: Executed task {} for "
            "document: {}".format(task.key, document_id)
        )

    def retry(self, task_id, force=False):
        task, document_id = self.task_register[task_id]
        self.updateTaskState(task._id, ProcState.SUCCESS.value, "Retried successfully!")
        print(
            "DummyEndpoint: Retried task {} for "
            "document: {}".format(task.key, document_id)
        )

    def callback(self, task_id, response):
        print(
            "DummyEndpoint: Callback response {} for "
            "task_id: {}".format(response, task_id)
        )

    def updateTaskState(self, task_id, state, message):
        task, _ = self.task_register[task_id]
        task.state = state
        task.msg = message
        # self.task_register[task_id] = (state, message, document_id)

    def search(self, target_id, creator_id):
        return

    def getUnfinished(self):
        return [
            t._id for t, _ in self.task_register if t.state != ProcState.SUCCESS.value
        ]

    def registerResult(self, result, task_id):
        return

    def deleteResult(self, result):
        return

    def resultFromResultId(self, result_id):
        return

    def searchResult(document_id, task_key):
        return

    def getAssignedTasks(self, document_id, task_key=None):
        return

    def get_docs_of_creator(
        self, creator: str, all_docs: List[Document], offset=0, size=200
    ) -> List[Document]:
        return []

    def get_tasks_of_creator(
        self, creator: str, task_key: str, all_tasks: List[Task], offset=0, size=200
    ) -> List[Task]:
        return []

    def get_results_of_creator(
        self, creator: str, task_key: str, all_results: List[Result], offset=0, size=200
    ) -> List[Result]:
        return []

    def get_result_of_task(self, task_id: str) -> Optional[Result]:
        return None
