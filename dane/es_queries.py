import logging


logger = logging.getLogger(__name__)


# query for fetching the result of a certain task
# TODO test with "a57c0e1c9c1be1e17bfdc3d75f0060f9fc217a3c"
def result_of_task_query(task_id: str):
    logger.debug("Generating result_of_task_query")
    return {"query": {"parent_id": {"type": "result", "id": task_id}}}


# query for fetching the task of the document with a certain target.id and DANE Task.key
def task_of_target_id_query(target_id: str, task_key: str, base_query: bool = True):
    task_query = {
        "bool": {
            "must": [
                {
                    "has_parent": {
                        "parent_type": "document",
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "query_string": {
                                            "default_field": "target.id",
                                            "query": target_id,
                                        }
                                    }
                                ]
                            }
                        },
                    }
                },
                {"query_string": {"default_field": "task.key", "query": task_key}},
            ]
        }
    }
    if base_query:
        query: dict = {}
        query["_source"] = ["task", "created_at", "updated_at", "role"]
        query["query"] = task_query
        return query
    return task_query


# query for fetching the result of the document with a certain target.id
def result_of_target_id_query(target_id: str, task_key: str):
    logger.debug("Generating result_of_target_id_query")
    sub_query = task_of_target_id_query(target_id, task_key, False)
    return {
        "query": {
            "bool": {
                "must": [
                    {"has_parent": {"parent_type": "task", "query": sub_query}},
                    {"exists": {"field": "result.payload"}},
                ]
            }
        }
    }


def docs_of_creator_query(
    creator: str, offset: int, size: int, base_query=True
) -> dict:
    match_creator_query = {
        "bool": {
            "must": [
                {
                    "query_string": {
                        "default_field": "creator.id",
                        "query": '"{}"'.format(creator),
                    }
                }
            ]
        }
    }
    if base_query:
        query: dict = {}
        query["_source"] = ["target", "creator", "created_at", "updated_at"]
        query["from"] = offset
        query["size"] = size
        query["query"] = match_creator_query
        return query
    return match_creator_query


# query for fetching all tasks for documents with a certain creator.id (used to record batches)
def tasks_of_creator_query(
    creator: str, task_key: str, offset: int, size: int, base_query=True
) -> dict:
    logger.debug("Generating tasks_of_creator_query")
    match_creator_query = docs_of_creator_query(creator, offset, size, False)
    tasks_query = {
        "bool": {
            "must": [
                {
                    "has_parent": {
                        "parent_type": "document",
                        "query": match_creator_query,
                    }
                },
                {
                    "query_string": {
                        "default_field": "task.key",
                        "query": task_key,
                    }
                },
            ]
        }
    }
    if base_query:
        query: dict = {}
        query["_source"] = ["task", "created_at", "updated_at", "role"]
        query["from"] = offset
        query["size"] = size
        query["query"] = tasks_query
        return query
    return tasks_query


# query for fetching all results for documents with a certain creator.id (used to record batches)
# FIXME: in case the underlying tasks mentioned: "task already assigned", the results will
# NOT be found this way
def results_of_creator_query(creator: str, task_key: str, offset: int, size: int):
    logger.debug("Generating results_of_creator_query")
    sub_query = tasks_of_creator_query(creator, task_key, offset, size, False)
    return {
        "_source": ["result", "created_at", "updated_at", "role"],
        "from": offset,
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {
                        "has_parent": {
                            "parent_type": "task",
                            "query": sub_query,
                        }
                    },
                    {
                        "exists": {"field": "result.payload"}
                    },  # only results with a payload
                ]
            }
        },
    }
