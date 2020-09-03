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

from elasticsearch import Elasticsearch
from elasticsearch import exceptions as EX
import json
import os
import logging
from functools import partial
from urllib.parse import urlsplit

import DANE
from DANE import handlers
import threading

logger = logging.getLogger('DANE')

INDEX = 'dane-index' # TODO make configurable?

class ESHandler(handlers.base_handler):

    def __init__(self, config, queue=None):
        super().__init__(config)

        self.es = None
        self.connect()
        self.queue = queue
        
    def connect(self):
        
        self.es = Elasticsearch([self.config.ELASTICSEARCH.HOST],
            http_auth=(self.config.ELASTICSEARCH.USER, self.config.ELASTICSEARCH.PASSWORD),
            scheme=self.config.ELASTICSEARCH.SCHEME,
            port=self.config.ELASTICSEARCH.PORT,
        ) 

        if not self.es.indices.exists(index=INDEX):
            self.es.indices.create(index=INDEX, body={
                "settings" : {
                    "index" : {
                        "number_of_shards" : 1,  # TODO do we need this?
                        "number_of_replicas" : 1 
                    }
                },
                "mappings": {
                    "properties": {
                        "role": {
                            "type": "join",
                            "relations": {
                                "document": "task",
                                "task": "result"
                            }
                        },
                        # document properties
                        "target": {
                            "properties": {
                                "id": { "type": "keyword" },
                                "url": { "type": "text" },
                                "type": { "type": "keyword" }
                            }
                        },
                        "creator": {
                            "properties": {
                                "id": { "type": "keyword" },
                                "type": { "type": "keyword" },
                                "name": { "type": "text" }
                            }
                        },
                        # task properties
                        "task": {
                            "properties": {
                                "priority": { "type": "byte" },
                                "key": { "type": "keyword" },
                                "state": { "type": "short" },
                                "msg": { "type": "text" },
                                "args": { "type": "object" }
                            }
                        },
                        # result properties:
                        "result": {
                            "properties": {
                                "generator": {
                                    "properties": {
                                        "id": { "type": "keyword" },
                                        "type": { "type": "keyword" },
                                        "name": { "type": "keyword" },
                                        "homepage": { "type": "text" }
                                    }
                                }, 
                                "payload": {
                                    "type": "object"
                                }
                            }
                        }
                    }
                }
            })

    def registerDocument(self, document):
        
        docs = self.search(document.target['id'],
                document.creator['id'])['documents']

        if len(docs) > 0:
            raise ValueError('A document with target.id `{}`, '\
                    'and creator.id `{}` already exists'.format(
                        document.target['id'],
                        document.creator['id']))

        doc = json.loads(document.to_json())
        doc['role'] = 'document'

        res = self.es.index(index=INDEX, body=json.dumps(doc), refresh=True)
        document._id = res['_id']
        
        logger.info("Registered new document #{}".format(document._id))
        
        return document._id

    def deleteDocument(self, document):
        if document._id is None:
            logger.error("Can only delete registered documents")
            raise KeyError("Failed to delete unregistered document")

        try:
            self.es.delete(INDEX, document._id)
            logger.info("Deleted document #{}".format(document._id))
            return True
        except EX.NotFoundError as e:
            logger.info("Unable to delete non-existing document #{}".format(
                document._id))
            return False
        
    def assignTask(self, task, document_id):
        if not self.es.get(index=INDEX, id=document_id)['found']:
            raise KeyError('No document with id `{}` found'.format(
                document_id))

        query = {
          "query": {
            "bool": {
              "must": [
                {
                  "has_child": {
                    "type": "task",
                    "query": {
                      "bool": {
                        "must": {
                          "match": {
                            "task.key": task.key
                          }
                        }
                      }
                    }
                  }
                },
                {
                  "match": {
                    "_id": document_id
                  }
                }
              ]
            }
          }
        }

        if self.es.count(index=INDEX, body=query)['count'] > 0:
            raise ValueError('Task `{}` '\
                    'already assigned to document `{}`'.format(
                        task.key,
                        document_id))

        task.state = 201
        task.msg = 'Created'

        t = json.loads(task.to_json())
        t['role'] = { 'name': 'task', 'parent': document_id }

        res = self.es.index(index=INDEX, 
                routing=document_id,
                body=json.dumps(t),
                refresh=True)

        task._id = res['_id']
        
        logger.debug("Assigned task {} () to document #{}".format(task.key,
            task._id,
            document_id))

        return task.run()

    def deleteTask(self, task):
        try:
            self.es.delete(INDEX, task._id) 
            return True
        except EX.NotFoundError as e:
            return False

    def taskFromTaskId(self, task_id):

        query = {
         "_source": "task",
          "query": {
            "bool": {
              "must": [
                {
                  "has_parent": {
                    "parent_type": "document",
                    "query": { # since we must have a query..
                      "exists": {
                        "field": "target.id"
                      }
                    }
                  }
                },
                {
                  "match": {
                    "_id": task_id
                  }
                },
                { "exists": {
                    "field": "task.key"
                  }
                }
              ]
            }
          }
        }
        
        result = self.es.search(index=INDEX, body=query)

        if result['hits']['total']['value'] == 1:
            # hacky way to pass _id to Task
            result['hits']['hits'][0]['_source']['task']['_id'] = \
                    result['hits']['hits'][0]['_id']
            task = DANE.Task.from_json(result['hits']['hits'][0]['_source'])
            task.set_api(self)
            return task
        else:
            raise KeyError("No result for given task id")

    def getTaskState(self, task_id):
        return int(self.taskFromTaskId(task_id).state)

    def getTaskKey(self, task_id): 
        return self.taskFromTaskId(task_id).key

    def _set_task_states(self, states, task):
        tid = task.task_id
        for s in states:
            if s['task_id'] == tid:
                task.task_state = int(s['task_state'])
                task.task_msg = s['task_msg']
                return

    def documentFromDocumentId(self, document_id):
        result = self.es.get(index=INDEX, id=document_id, 
                _source_excludes=['role'],
                ignore=404)

        if result['found']:
            result['_source']['_id'] = result['_id']
            document = DANE.Document.from_json(json.dumps(result['_source']))
            document.set_api(self)
            return document
        else:
            raise KeyError("No result for given document id")

    def documentFromTaskId(self, task_id):
        query = {
         "_source": {
            "excludes": [ "role" ]    
         },
          "query": {
            "bool": {
              "must": [
                {
                  "has_child": {
                    "type": "task",
                    "query": { 
                      "match": {
                        "_id": task_id
                      }
                    }
                  }
                }
              ]
            }
          }
        }
        
        result = self.es.search(index=INDEX, body=query)

        if result['hits']['total']['value'] == 1:
            result['hits']['hits'][0]['_source']['_id'] = \
                    result['hits']['hits'][0]['_id']

            document = DANE.Document.from_json(json.dumps(
                result['hits']['hits'][0]['_source']))
            document.set_api(self)
            return document
        else:
            raise KeyError("No result for given task id")

    ## Result functions
    def registerResult(self, result, task_id):

        r = json.loads(result.to_json())
        r['role'] = { 'name': 'result', 'parent': task_id }

        res = self.es.index(index=INDEX, 
                routing=task_id,
                body=json.dumps(r),
                refresh=True)

        return res['_id']

    def deleteResult(self, result):
        try:
            self.es.delete(INDEX, result._id) 
            return True
        except EX.NotFoundError as e:
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
                    "query": { # since we must have a query..
                      "exists": {
                        "field": "task.key"
                      }
                    }
                  }
                },
                {
                  "match": {
                    "_id": result_id
                  }
                },
                { "exists": {
                    "field": "result.generator.id"
                  }
                }
              ]
            }
          }
        }
        
        result = self.es.search(index=INDEX, body=query)
        
        if result['hits']['total']['value'] == 1:
            result['hits']['hits'][0]['_source']['_id'] = \
                    result['hits']['hits'][0]['_id']

            return DANE.Result.from_json(json.dumps(
                result['hits']['hits'][0]['_source']))
        else:
            raise KeyError("No result for given result_id")

    def searchResult(document_id, task_key):
        query = {
         "_source": False,
          "query": {
            "bool": {
              "must": [
                {
                  "has_parent": {
                    "parent_type": "document",
                    "query": { # since we must have a query..
                      "exists": {
                        "field": "target.id"
                      }
                    }
                  }
                },
                {
                  "match": {
                    "task.key": task_key
                  }
                }
              ]
            }
          }
        }
        
        result = self.es.search(index=INDEX, body=query)
        
        if result['hits']['total']['value'] > 0:
            shld = []
            for res in result['hits']['hits']:
                shld.append({"match": {"_id": res['_id'] }})

            query = {
             "_source": "result",
              "query": {
                "bool": {
                  "must": [
                    {
                      "has_parent": {
                        "parent_type": "task",
                        "query": { 
                          "bool":  {
                            "should": shld
                          }
                        }
                      }
                    },
                    { "exists": {
                        "field": "result.generator.id"
                      }
                    }
                  ]
                }
              }
            }
            result = es.search(index=INDEX, body=query)
            
            if result['hits']['total']['value'] > 0:
                found = []
                for res in result['hits']['hits']:
                    res['_source']['_id'] = \
                            res['_id']
                    found.append(DANE.Result.from_json(json.dumps(res['_source'])))
                return found
            else:
                raise KeyError("No result found for {} assigned to {}".format(
                    task_key, document_id))
        else:
            raise KeyError("Task {} has not been assigned to document {}".format(
                task_key, document_id))

    def _run(self, task):
        document = self.documentFromTaskId(task._id)
        
        routing_key = "{}.{}".format(document.target['type'], 
                task.key)

        logger.debug("Queueing task {} ({}) for document {}".format(task._id,
            task.key, document._id))
        self.updateTaskState(task._id, 102, 'Queued')

        self.queue.publish(routing_key, task, document)

    def run(self, task_id):
        task = self.taskFromTaskId(task_id)
        if task.state == 201:
            # Fresh of the press task, run it no questions asked
            self._run(task)
        elif task.state in [412, 502, 503]:
            # Task that might be worth automatically retrying 
            self._run(task)
        else:
            # Requires manual intervention
            # and task resubmission once issue has been resolved
            pass    
        
    def retry(self, task_id, force=False):
        task = self.taskFromTaskId(task_id)
        if task.state not in [102, 200] or force:
            # Unless its already been queued or completed, we can run this again
            # Or we can force it to run again
            self._run(task)

    def callback(self, task_id, response):
        try:
            task_key = self.getTaskKey(task_id)

            state = int(response.pop('state'))
            message = response.pop('message')

            self.updateTaskState(task_id, state, message)
            if state != 200:
                logger.warning("Task {} ({}) failed with msg: #{} {}".format(
                    task_key, task_id, state, message))            
            else:
                logger.debug("Callback for task {} ({})".format(task_id, 
                    task_key))

                # only trigger other tasks for this document if the task was succesful
                doc = self.documentFromTaskId(task_id)
                doc.set_api(self)

                assigned = doc.getAssignedTasks()
                for _id, key, st in assigned:
                    if (_id != task_id  # dont retrigger self
                            and st in [201, 412, 502, 503]):
                        self.run(_id) 

        except KeyError as e:
            logger.exception('Callback on non-existing task')
        except Exception as e:
            logger.exception('Unhandled error during callback')

    def updateTaskState(self, task_id, state, message):        
        self.es.update(index=INDEX, id=task_id, body={
            "doc": {
                "task": {
                    "state": state,
                    "msg": message
                }
            }
        }, refresh=True)

    def search(self, target_id, creator_id):
        query = {
            "_source": False,
            "query": {
                "bool": {
                    "must": [
                        { "match": { "target.id": target_id }},
                        { "match": { "creator.id": creator_id }}
                    ]
                }
            }
        }

        res = self.es.search(index=INDEX, body=query)

        return {'documents': [doc['_id'] for doc in res['hits']['hits'] ] }

    def getUnfinished(self):
        query = {
         "_source": False,
          "query": {
            "bool": {
              "must": [
                {
                  "has_parent": {
                    "parent_type": "document",
                    "query": { # since we must have a query..
                      "exists": {
                        "field": "target.id"
                      }
                    }
                  }
                }
            ], "must_not": [
                { "match": {
                    "task.state": 200 # already done!
                  }},
                {"match": {
                    "task.state": 412 # should be triggered once dependency finished
                  }}
                ]
              }
            }
          }
        
        result = self.es.search(index=INDEX, body=query)

        return {'tasks': [res['_id'] for res in result['hits']['hits']]}

    def getAssignedTasks(self, document_id, task_key=None):
        query = {
         "_source": "task",
          "query": {
            "bool": {
              "must": [
                {
                  "has_parent": {
                    "parent_type": "document",
                    "query": { 
                      "match": {
                        "_id": document_id
                      }
                    }
                  }
                },

                { "exists": {
                    "field": "task.key"
                  }
                }
              ]
            }
          }
        }

        if task_key is not None:
            query['query']['bool']['must'].append({
                  "match": {
                    "task.key": task_key
                  }
                })
        
        result = self.es.search(index=INDEX, body=query)

        if result['hits']['total']['value'] > 0:
            return [{'_id': t['_id'], 
                'key': t['_source']['task']['key'],
                'state': t['_source']['task']['state']} for t \
                    in result['hits']['hits']]
        else:
            return []
