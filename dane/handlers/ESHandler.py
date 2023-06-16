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

from elasticsearch7 import Elasticsearch
from elasticsearch7.exceptions import ConflictError, NotFoundError
from elasticsearch7 import helpers
import json
import logging
import hashlib
import datetime
from typing import List, Optional

from dane import Document, Task, Result, ProcState
from dane.handlers.base_handler import BaseHandler
from dane.es_queries import (
    result_of_task_query,
    tasks_of_creator_query,
    docs_of_creator_query,
    results_of_creator_query,
)
from dane.errors import (
    DocumentExistsError,
    UnregisteredError,
    TaskAssignedError,
    TaskExistsError,
    ResultExistsError,
)
import threading

logger = logging.getLogger("DANE")


class ESHandler(BaseHandler):
    def __init__(self, config, queue=None):
        super().__init__(config)

        self.es = None
        self.INDEX = self.config.ELASTICSEARCH.INDEX
        self.connect()
        self.queue = queue

    def connect(self):

        self.es = Elasticsearch(
            self.config.ELASTICSEARCH.HOST,
            http_auth=(
                self.config.ELASTICSEARCH.USER,
                self.config.ELASTICSEARCH.PASSWORD,
            ),
            scheme=self.config.ELASTICSEARCH.SCHEME,
            port=self.config.ELASTICSEARCH.PORT,
            timeout=self.config.ELASTICSEARCH.TIMEOUT,
            retry_on_timeout=(self.config.ELASTICSEARCH.MAX_RETRIES > 0),
            max_retries=self.config.ELASTICSEARCH.MAX_RETRIES,
        )

        try:
            if not self.es.ping():
                logger.info(
                    "Tried connecting to ES at {}:{}".format(
                        self.config.ELASTICSEARCH.HOST, self.config.ELASTICSEARCH.PORT
                    )
                )
                raise ConnectionError("ES could not be Pinged")
        except Exception:
            logger.exception("ES Connection Failed")
            raise ConnectionError("ES Connection Failed")

        if not self.es.indices.exists(index=self.INDEX):
            self.es.indices.create(
                index=self.INDEX,
                body={
                    "settings": {
                        "index": {
                            "number_of_shards": self.config.ELASTICSEARCH.SHARDS,
                            "number_of_replicas": self.config.ELASTICSEARCH.REPLICAS,
                        }
                    },
                    "mappings": {
                        "properties": {
                            "role": {
                                "type": "join",
                                "relations": {"document": "task", "task": "result"},
                            },
                            # shared prop
                            "created_at": {
                                "type": "date",
                                "format": "date_hour_minute_second",
                            },
                            "updated_at": {
                                "type": "date",
                                "format": "date_hour_minute_second",
                            },
                            # document properties
                            "target": {
                                "properties": {
                                    "id": {"type": "keyword"},
                                    "url": {"type": "text"},
                                    "type": {"type": "keyword"},
                                }
                            },
                            "creator": {
                                "properties": {
                                    "id": {"type": "keyword"},
                                    "type": {"type": "keyword"},
                                    "name": {"type": "text"},
                                }
                            },
                            # task properties
                            "task": {
                                "properties": {
                                    "priority": {"type": "byte"},
                                    "key": {"type": "keyword"},
                                    "state": {"type": "short"},
                                    "msg": {"type": "text"},
                                    "args": {"type": "object"},
                                }
                            },
                            # result properties:
                            "result": {
                                "properties": {
                                    "generator": {
                                        "properties": {
                                            "id": {"type": "keyword"},
                                            "type": {"type": "keyword"},
                                            "name": {"type": "keyword"},
                                            "homepage": {"type": "text"},
                                        }
                                    },
                                    "payload": {"type": "object"},
                                }
                            },
                        }
                    },
                },
            )

    def registerDocument(self, document):

        doc = json.loads(document.to_json())
        doc["role"] = "document"
        doc["created_at"] = doc["updated_at"] = (
            datetime.datetime.now().replace(microsecond=0).isoformat()
        )

        _id = hashlib.sha1(
            (str(document.target["id"]) + str(document.creator["id"])).encode("utf-8")
        ).hexdigest()

        try:
            res = self.es.index(
                index=self.INDEX,
                body=json.dumps(doc),
                id=_id,
                refresh=True,
                op_type="create",
            )
        except ConflictError:
            raise DocumentExistsError(
                "A document with target.id `{}`, "
                "and creator.id `{}` already exists".format(
                    document.target["id"], document.creator["id"]
                )
            )

        document._id = res["_id"]
        document.created_at = doc["created_at"]
        document.updated_at = doc["updated_at"]
        logger.debug("Registered new document #{}".format(document._id))

        return document._id

    def registerDocuments(self, documents):

        actions = []
        for document in documents:
            doc = {}
            doc["_op_type"] = "create"
            doc["_index"] = self.INDEX

            doc["_source"] = json.loads(document.to_json())
            doc["_source"]["role"] = "document"

            doc["_source"]["created_at"] = doc["_source"]["updated_at"] = (
                datetime.datetime.now().replace(microsecond=0).isoformat()
            )

            document._id = doc["_id"] = hashlib.sha1(
                (str(document.target["id"]) + str(document.creator["id"])).encode(
                    "utf-8"
                )
            ).hexdigest()
            document.created_at = doc["_source"]["created_at"]
            document.updated_at = doc["_source"]["updated_at"]
            actions.append(doc)

        succeeded, errors = helpers.bulk(
            self.es, actions, raise_on_error=False, refresh=True
        )
        logger.debug(
            "Batch registration: Success {} Failed {}".format(succeeded, len(errors))
        )

        if len(errors) == 0:
            return documents, []
        else:
            success = []
            failed = []
            errors = {e["create"]["_id"]: e["create"] for e in errors}
            for document in documents:
                if document._id in errors.keys():
                    if errors[document._id]["status"] == ProcState.ALREADY_EXISTS.value:
                        failed.append(
                            {
                                "document": document,
                                "error": "A document with target.id `{}`, "
                                "and creator.id `{}` already exists".format(
                                    document.target["id"], document.creator["id"]
                                ),
                            }
                        )
                    else:
                        failed.append(
                            {
                                "document": document,
                                "error": "[{}] {}".format(
                                    errors[document._id]["status"],
                                    errors[document._id]["error"]["reason"],
                                ),
                            }
                        )
                else:
                    success.append(document)
            return success, failed

    def deleteDocument(self, document):
        if document._id is None:
            logger.error("Can only delete registered documents")
            raise UnregisteredError("Failed to delete unregistered document")

        try:
            # delete tasks assigned to this document first,
            # and results assigned to those tasks
            query = {
                "query": {
                    "bool": {
                        "should": [
                            {
                                "bool": {
                                    # all tasks with this as parent
                                    "must": [
                                        {
                                            "has_parent": {
                                                "parent_type": "document",
                                                "query": {
                                                    "match": {"_id": document._id}
                                                },
                                            }
                                        },
                                        {"exists": {"field": "task.key"}},
                                    ]
                                }
                            },
                            {
                                "bool": {
                                    # all results that hang below a task with this as parent
                                    "must": [
                                        {
                                            "has_parent": {
                                                "parent_type": "task",
                                                "query": {
                                                    "has_parent": {
                                                        "parent_type": "document",
                                                        "query": {
                                                            "match": {
                                                                "_id": document._id
                                                            }
                                                        },
                                                    }
                                                },
                                            }
                                        },
                                        {"exists": {"field": "result.generator.id"}},
                                    ]
                                }
                            },
                        ]
                    }
                }
            }
            self.es.delete_by_query(self.INDEX, body=query)

            self.es.delete(self.INDEX, document._id, refresh=True)
            logger.debug("Deleted document #{}".format(document._id))
            return True
        except NotFoundError:
            logger.info(
                f"Unable to delete non-existing document with ID: {document._id}"
            )
            return False

    def assignTask(self, task, document_id):
        if not self.es.get(index=self.INDEX, id=document_id)["found"]:
            raise DocumentExistsError(
                "No document with id `{}` found".format(document_id)
            )

        _id = hashlib.sha1((document_id + task.key).encode("utf-8")).hexdigest()

        task.state = ProcState.CREATED.value  # TODO update Task object to use ProcState
        task.msg = "Created"

        t = {"task": json.loads(task.to_json())}
        t["role"] = {"name": "task", "parent": document_id}
        t["created_at"] = t["updated_at"] = (
            datetime.datetime.now().replace(microsecond=0).isoformat()
        )

        try:
            res = self.es.index(
                index=self.INDEX,
                routing=document_id,
                body=json.dumps(t),
                id=_id,
                refresh=True,
                op_type="create",
            )
        except ConflictError:
            raise TaskAssignedError(
                "Task `{}` "
                "already assigned to document `{}`".format(task.key, document_id)
            )

        task._id = res["_id"]
        task.created_at = t["created_at"]
        task.updated_at = t["updated_at"]

        logger.debug(
            "Assigned task {}({}) to document #{}".format(
                task.key, task._id, document_id
            )
        )

        return task.run()

    def assignTaskToMany(self, task, document_ids):
        failed = []
        searches = []
        for document_id in document_ids:
            searches.append("{}")
            searches.append(
                json.dumps(
                    {"query": {"match": {"_id": document_id}}, "_source": "false"}
                )
            )

        docs = []
        for d, d_id in zip(
            self.es.msearch("\n".join(searches), index=self.INDEX)["responses"],
            document_ids,
        ):
            if d["hits"]["total"]["value"] == 1:
                docs.append(d_id)
            elif d["hits"]["total"]["value"] == 0:
                failed.append(
                    {
                        "document_id": d_id,
                        "error": "[404] 'No document with id `{}` found'".format(d_id),
                    }
                )
            else:
                failed.append(
                    {
                        "document_id": d_id,
                        "error": "[500] 'Multiple documents found with id `{}`'".format(
                            d_id
                        ),
                    }
                )
        document_ids = docs
        del docs

        task.state = ProcState.CREATED.value
        task.msg = "Created"

        actions = []
        tasks = []
        for document_id in document_ids:
            t = {}
            tc = task.__copy__()

            t["_source"] = {"task": json.loads(tc.to_json())}
            t["_source"]["role"] = {"name": "task", "parent": document_id}
            t["_source"]["created_at"] = t["_source"]["updated_at"] = (
                datetime.datetime.now().replace(microsecond=0).isoformat()
            )
            t["_op_type"] = "create"
            t["_index"] = self.INDEX
            t["_routing"] = document_id

            tc._id = t["_id"] = hashlib.sha1(
                (document_id + tc.key).encode("utf-8")
            ).hexdigest()
            tc.created_at = tc.updated_at = t["_source"]["created_at"]

            tasks.append(tc)
            actions.append(t)

        succeeded, errors = helpers.bulk(
            self.es, actions, raise_on_error=False, refresh=True
        )
        logger.debug(
            "Batch task registration: Success {} Failed {}".format(
                succeeded, len(errors)
            )
        )

        success = []
        errors = {e["create"]["_id"]: e["create"] for e in errors}
        for task, document_id in zip(tasks, document_ids):
            if task._id in errors.keys():
                if errors[task._id]["status"] == ProcState.ALREADY_EXISTS.value:
                    failed.append(
                        {
                            "document_id": document_id,
                            "error": "Task `{}` "
                            "already assigned to document `{}`".format(
                                task.key, document_id
                            ),
                        }
                    )
                else:
                    if "caused_by" in errors[task._id]["error"].keys():
                        errors[task._id]["error"]["reason"] += (
                            " > " + errors[task._id]["error"]["caused_by"]["reason"]
                        )

                    failed.append(
                        {
                            "document_id": document_id,
                            "error": "[{}] {}".format(
                                errors[task._id]["status"],
                                errors[task._id]["error"]["reason"],
                            ),
                        }
                    )
            else:
                success.append(task)

        # run tasks from thread, so it doesnt block API response
        t = threading.Thread(target=self._run_async, args=(success,))
        t.daemon = True
        t.start()
        return success, failed

    def _run_async(self, tasks):
        for task in tasks:
            try:
                task.run()
            except Exception:
                logger.exception("Exception during async run")
                pass  # ignore exceptions, and just GO GO GO

    def deleteTask(self, task):
        try:
            # delete results assigned to this task
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "has_parent": {
                                    "parent_type": "task",
                                    "query": {"match": {"_id": task._id}},
                                }
                            },
                            {"exists": {"field": "result.generator.id"}},
                        ]
                    }
                }
            }
            self.es.delete_by_query(self.INDEX, body=query)
            self.es.delete(self.INDEX, task._id, refresh=True)
            return True
        except NotFoundError:
            logger.info(f"Unable to delete non-existing task with ID: {task._id}")
            return False

    def taskFromTaskId(self, task_id):

        query = {
            "_source": ["task", "created_at", "updated_at"],
            "query": {
                "bool": {
                    "must": [
                        {
                            "has_parent": {
                                "parent_type": "document",
                                "query": {  # since we must have a query..
                                    "exists": {"field": "target.id"}
                                },
                            }
                        },
                        {"match": {"_id": task_id}},
                        {"exists": {"field": "task.key"}},
                    ]
                }
            },
        }

        result = self.es.search(index=self.INDEX, body=query)

        if result["hits"]["total"]["value"] == 1:
            # hacky way to pass _id to Task
            result["hits"]["hits"][0]["_source"]["task"]["_id"] = result["hits"][
                "hits"
            ][0]["_id"]
            task = Task.from_json(result["hits"]["hits"][0]["_source"])
            task.set_api(self)
            return task
        else:
            raise TaskExistsError("No result for task id: {}".format(task_id))

    def getTaskState(self, task_id):
        return int(self.taskFromTaskId(task_id).state)

    def getTaskKey(self, task_id):
        return self.taskFromTaskId(task_id).key

    def _set_task_states(self, states, task):
        tid = task.task_id
        for s in states:
            if s["task_id"] == tid:
                task.task_state = int(s["task_state"])
                task.task_msg = s["task_msg"]
                return

    def documentFromDocumentId(self, document_id):
        result = self.es.get(
            index=self.INDEX, id=document_id, _source_excludes=["role"], ignore=404
        )

        if result["found"]:
            result["_source"]["_id"] = result["_id"]
            document = Document.from_json(json.dumps(result["_source"]))
            document.set_api(self)
            return document
        else:
            raise DocumentExistsError("No result for given document id")

    def documentFromTaskId(self, task_id):
        query = {
            "_source": {"excludes": ["role"]},
            "query": {
                "bool": {
                    "must": [
                        {
                            "has_child": {
                                "type": "task",
                                "query": {"match": {"_id": task_id}},
                            }
                        }
                    ]
                }
            },
        }

        result = self.es.search(index=self.INDEX, body=query)

        if result["hits"]["total"]["value"] == 1:
            result["hits"]["hits"][0]["_source"]["_id"] = result["hits"]["hits"][0][
                "_id"
            ]

            document = Document.from_json(
                json.dumps(result["hits"]["hits"][0]["_source"])
            )
            document.set_api(self)
            return document
        else:
            raise TaskExistsError("No result for given task id")

    # Result functions
    def registerResult(self, result, task_id):

        r = json.loads(result.to_json())
        r["role"] = {"name": "result", "parent": task_id}

        r["created_at"] = r["updated_at"] = (
            datetime.datetime.now().replace(microsecond=0).isoformat()
        )

        res = self.es.index(
            index=self.INDEX, routing=task_id, body=json.dumps(r), refresh=True
        )

        result._id = res["_id"]
        result.created_at = r["created_at"]
        result.updated_at = r["updated_at"]

        return result

    def deleteResult(self, result):
        try:
            self.es.delete(self.INDEX, result._id)
            return True
        except NotFoundError:
            logger.info(f"Unable to delete non-existing result with ID: {result._id}")
            return False

    def resultFromResultId(self, result_id):
        query = {
            "_source": "result",
            "query": {
                "bool": {
                    "must": [
                        {
                            "has_parent": {
                                "parent_type": "task",
                                "query": {  # since we must have a query..
                                    "exists": {"field": "task.key"}
                                },
                            }
                        },
                        {"match": {"_id": result_id}},
                        {"exists": {"field": "result.generator.id"}},
                    ]
                }
            },
        }

        result = self.es.search(index=self.INDEX, body=query)

        if result["hits"]["total"]["value"] == 1:
            res = {"_id": result["hits"]["hits"][0]["_id"]}
            res = {**res, **result["hits"]["hits"][0]["_source"]["result"]}

            return Result.from_json(json.dumps(res))
        else:
            raise ResultExistsError("No result for given result_id")

    def searchResult(self, document_id, task_key):
        # find tasks of type task_key below this document
        query = {
            "_source": False,
            "query": {
                "bool": {
                    "must": [
                        {
                            "has_parent": {
                                "parent_type": "document",
                                "query": {"match": {"_id": document_id}},
                            }
                        },
                        {"match": {"task.key": task_key}},
                    ]
                }
            },
        }

        result = self.es.search(index=self.INDEX, body=query)

        # if we found tasks then check all of them for the result
        if result["hits"]["total"]["value"] > 0:
            shld = []
            for res in result["hits"]["hits"]:
                shld.append({"match": {"_id": res["_id"]}})

            query = {
                "_source": "result",
                "query": {
                    "bool": {
                        "must": [
                            {
                                "has_parent": {
                                    "parent_type": "task",
                                    "query": {"bool": {"should": shld}},
                                }
                            },
                            {"exists": {"field": "result.generator.id"}},
                        ]
                    }
                },
            }
            result = self.es.search(index=self.INDEX, body=query)

            if result["hits"]["total"]["value"] > 0:
                found = []
                for res in result["hits"]["hits"]:
                    r = {"_id": res["_id"]}
                    r = {**r, **res["_source"]["result"]}

                    found.append(Result.from_json(json.dumps(r)))
                return found
            else:
                raise ResultExistsError(
                    "No result found for {} assigned to {}".format(
                        task_key, document_id
                    )
                )
        else:
            raise TaskAssignedError(
                "Task {} has not been assigned to document {}".format(
                    task_key, document_id
                )
            )

    def _queue_task(self, task):
        document = self.documentFromTaskId(task._id)

        routing_key = "{}.{}".format(document.target["type"], task.key)

        try:
            logger.debug(
                "Queueing task {} ({}) for document {}".format(
                    task._id, task.key, document._id
                )
            )
            self.updateTaskState(task._id, ProcState.QUEUED.value, "Queued")
        except Exception as e:
            raise e

        try:
            self.queue.publish(routing_key, task, document)
        except Exception as e:
            self.updateTaskState(task._id, ProcState.ERROR.value, str(e))
            raise e

    def run(self, task_id):
        task = self.taskFromTaskId(task_id)
        if task.state == ProcState.CREATED.value:
            # Fresh of the press task, run it no questions asked
            self._queue_task(task)  # queue the task
        elif task.state in [
            ProcState.TASK_RESET.value,
            ProcState.UNFINISHED_DEPENDENCY.value,  # TODO how to deal with failed dependencies?
            ProcState.ERROR_INVALID_INPUT.value,
            ProcState.ERROR_PROXY.value,
        ]:  # Task that might be worth automatically retrying
            self._queue_task(task)
        else:
            # Requires manual intervention
            # and task resubmission once issue has been resolved
            pass

    def retry(self, task_id, force=False):
        task = self.taskFromTaskId(task_id)
        if (
            task.state
            not in [
                ProcState.QUEUED.value,
                ProcState.SUCCESS.value,
            ]
            or force
        ):
            # Unless its already been queued or completed, we can run this again
            # Or we can force it to run again
            self._queue_task(task)

    # handles the communication received from the workers
    def callback(self, task_id, response):
        try:
            logger.info(f"Task {task_id} came back with a response")
            task_key = self.getTaskKey(task_id)

            state = int(response.pop("state"))
            message = response.pop("message")
            logger.info(f"Task state: {state}; message: {message}")

            # update the state of the task in ES
            self.updateTaskState(task_id, state, message)

            # if the task has unfinished dependencies, create them here
            doc = None
            if state == ProcState.UNFINISHED_DEPENDENCY.value:
                logger.debug("Dependencies for task {} ({})".format(task_id, task_key))
                dependencies = response.pop("dependencies")
                if len(dependencies) > 0:
                    doc = self.documentFromTaskId(task_id)
                    doc.set_api(self)

                    for dep in dependencies:
                        if isinstance(dep, dict):
                            td = Task.from_json(dep)
                            td.set_api(self)
                        else:
                            td = Task(dep, api=self)
                        td.assign(doc._id)
                        self.run(td._id)  # run the task immediately
            elif state != ProcState.SUCCESS.value:
                logger.warning(
                    "Task {} ({}) failed with msg: #{} {}".format(
                        task_key, task_id, state, message
                    )
                )
                # only continue if the task was succesful
                return
            else:
                logger.debug("Callback for task {} ({})".format(task_id, task_key))

            # fetch the document that assigned the task
            if doc is None:
                doc = self.documentFromTaskId(task_id)
                doc.set_api(self)

            # see if any other tasks were assigned to this doc and trigger them
            assigned = doc.getAssignedTasks()
            for at in assigned:
                # don't run other tasks for same doc that are also waiting for a dependency
                if at["_id"] != task_id and (  # dont retrigger same task
                    at["state"]
                    in [  # NOTE: these states are worth a (re)try?
                        ProcState.CREATED.value,
                        ProcState.ERROR_INVALID_INPUT.value,
                        ProcState.ERROR_PROXY.value,
                    ]
                    or (  # NOTE: also trigger tasks that have unfinished dependencies??
                        at["state"] == ProcState.UNFINISHED_DEPENDENCY.value
                        and state != ProcState.UNFINISHED_DEPENDENCY.value
                    )
                ):
                    self.run(at["_id"])

        except TaskExistsError:
            logger.exception("Callback on non-existing task")
        except Exception:
            logger.exception("Unhandled error during callback")

    def updateTaskState(
        self, task_id, state, message
    ):  # TODO update using ProcState object

        self.es.update(
            index=self.INDEX,
            id=task_id,
            body={
                "doc": {
                    "task": {"state": state, "msg": message},
                    "updated_at": datetime.datetime.now()
                    .replace(microsecond=0)
                    .isoformat(),
                }
            },
            refresh=True,
        )

    def search(self, target_id, creator_id, page=1):
        page = int(max(1, page) - 1)
        perpage = 100  # TODO put in config

        query = {
            "_source": {"excludes": ["role"]},
            "from": page * perpage,
            "query": {
                "bool": {
                    "must": [
                        {"wildcard": {"target.id": {"value": target_id}}},
                        {"wildcard": {"creator.id": {"value": creator_id}}},
                    ]
                }
            },
        }

        res = self.es.search(index=self.INDEX, body=query, size=perpage)

        ret = []
        for doc in res["hits"]["hits"]:
            doc["_source"]["_id"] = doc["_id"]
            d = Document.from_json(doc["_source"])
            ret.append(json.loads(d.to_json()))

        return ret, res["hits"]["total"]["value"]

    def getUnfinished(self, only_runnable=False):
        query = {
            "_source": {"excludes": ["role"]},
            "query": {
                "bool": {
                    "must": [
                        {
                            "has_parent": {
                                "parent_type": "document",
                                "query": {  # since we must have a query..
                                    "exists": {"field": "target.id"}
                                },
                            }
                        }
                    ],
                    "must_not": [
                        {
                            "match": {"task.state": ProcState.SUCCESS.value}
                        },  # already done!
                        {
                            "match": {
                                "task.state": ProcState.UNFINISHED_DEPENDENCY.value  # will be triggered once dependency finished
                            }
                        },
                    ],
                }
            },
        }

        if only_runnable:  # TODO use state whitelist instead of blacklist?
            query["query"]["bool"]["must_not"].extend(
                [  # the first 5 states require manual intervention
                    {"match": {"task.state": ProcState.NO_ROUTE_TO_QUEUE.value}},
                    {"match": {"task.state": ProcState.ERROR.value}},
                    {"match": {"task.state": ProcState.BAD_REQUEST.value}},
                    {"match": {"task.state": ProcState.ACCESS_DENIED.value}},
                    {"match": {"task.state": ProcState.NOT_FOUND.value}},
                    {"match": {"task.state": ProcState.QUEUED.value}},
                    # {"match": {"task.state": ProcState.PROCESSING.value}}, NOTE: not active yet
                ]
            )

        result = self.es.search(
            index=self.INDEX, body=query, size=1000
        )  # TODO put size in conf?

        if result["hits"]["total"]["value"] > 0:
            ret = []
            for t in result["hits"]["hits"]:
                t["_source"]["task"]["_id"] = t["_id"]
                task = Task.from_json(t["_source"])
                ret.append(json.loads(task.to_json()))
            return ret
        else:
            return []

    def getAssignedTasks(self, document_id, task_key=None):
        # normally we'd need a has_parent query
        # but thats terribly slow, and since _routing for task
        # is set to document_id, we can abuse that
        query = {
            "_source": {"excludes": ["role"]},
            "query": {
                "bool": {
                    "must": [
                        {"match": {"_routing": document_id}},
                        {"exists": {"field": "task.key"}},
                    ]
                }
            },
        }

        if task_key is not None:
            query["query"]["bool"]["must"].append({"match": {"task.key": task_key}})

        result = self.es.search(index=self.INDEX, body=query)

        if result["hits"]["total"]["value"] > 0:
            ret = []
            for t in result["hits"]["hits"]:
                t["_source"]["task"]["_id"] = t["_id"]
                task = Task.from_json(t["_source"])
                ret.append(json.loads(task.to_json()))
            return ret
        else:
            return []

    """
    --------------------------- NEW FUNCTIONS -----------------------------
    NOTE: creator is part of the hashed doc ID, so maybe adding a Document.batch_id is better...
    """

    def get_docs_of_creator(
        self, creator: str, all_docs: List[Document], offset=0, size=200
    ) -> List[Document]:
        logger.info(f"Fetching all docs of creator: {creator} from DANE index")
        query = docs_of_creator_query(creator, offset, size)
        logger.debug(json.dumps(query, indent=4, sort_keys=True))
        result = self.es.search(
            index=self.INDEX,
            body=query,
            request_timeout=self.config.ELASTICSEARCH.TIMEOUT,
        )
        if len(result["hits"]["hits"]) <= 0:
            logger.debug(f"Done fetching all docs for creator {creator}")
            return all_docs
        else:
            for hit in result["hits"]["hits"]:
                hit["_source"]["_id"] = hit["_id"]  # weird but ok
                doc = Document.from_json(hit["_source"])
                all_docs.append(doc)
            return self.get_docs_of_creator(creator, all_docs, offset + size, size)

    def get_tasks_of_creator(
        self, creator: str, task_key: str, all_tasks: List[Task], offset=0, size=200
    ) -> List[Task]:
        logger.info(f"Fetching {task_key} tasks of creator: {creator} from DANE index")
        query = tasks_of_creator_query(creator, task_key, offset, size)
        logger.debug(json.dumps(query, indent=4, sort_keys=True))
        result = self.es.search(
            index=self.INDEX,
            body=query,
            request_timeout=self.config.ELASTICSEARCH.TIMEOUT,
        )
        if len(result["hits"]["hits"]) <= 0:
            logger.debug(f"Done fetching all tasks for creator {creator}")
            return all_tasks
        else:
            for hit in result["hits"]["hits"]:
                hit["_source"]["task"]["_id"] = hit["_id"]
                task = Task.from_json(hit["_source"])
                all_tasks.append(task)
            return self.get_tasks_of_creator(
                creator, task_key, all_tasks, offset + size, size
            )

    def get_results_of_creator(
        self, creator: str, task_key: str, all_results: List[Result], offset=0, size=200
    ) -> List[Result]:
        logger.debug(
            f"Fetching {task_key} results of creator: {creator} from DANE index"
        )
        query = results_of_creator_query(creator, task_key, offset, size)
        logger.debug(json.dumps(query, indent=4, sort_keys=True))
        result = self.es.search(
            index=self.INDEX,
            body=query,
            request_timeout=self.config.ELASTICSEARCH.TIMEOUT,
        )
        if len(result["hits"]["hits"]) <= 0:
            logger.debug(f"Done fetching all results for creator {creator}")
            return all_results
        else:
            for hit in result["hits"]["hits"]:
                logger.debug("Got a result")
                logger.debug(hit)
                r = {"_id": hit["_id"]}
                r = {**r, **hit["_source"]["result"]}
                all_results.append(Result.from_json(json.dumps(r)))
            return self.get_results_of_creator(
                creator, task_key, all_results, offset + size, size
            )

    # TODO finish this and wire it up in the api.py in DANE-server
    def get_result_of_task(self, task_id: str) -> Optional[Result]:
        query = result_of_task_query(task_id)
        logger.warning("not implemented yet")
        logger.debug(query)
        return None
