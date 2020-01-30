import json
import sys
import DANE

def parse(task_str):
    """Tries to parse a serialised :class:`DANE.Task` or
    :class:`DANE.taskContainer`, returning the correct object.

    If input is a string, it tries to parse it as json if its still a string
    after this, then its a task str, so parse it as a Task.

    Otherwise we expect it to be a length 1 dict, with format:
    { CLASSNAME : { params } } or { CLASSNAME : [ sub_tasks ] }.
    In the latter case the class is expected to be a taskContainer subclass
    and the classname should start with 'task' (lowercase)

    :param task_str: Serialised :class:`DANE.Task` or :class:`DANE.taskContainer`
    :type task_str: str or dict
    :return: A initialised Task or taskContainer
    :rtype: :class:`DANE.Task` or :class:`DANE.taskContainer`
    """
    if isinstance(task_str, str):
        try:
            task_str = json.loads(task_str)
        except json.JSONDecodeError:
            pass

    if isinstance(task_str, str):
        task = DANE.Task(task_str)
    elif isinstance(task_str, dict) and len(task_str) == 1:
        cls, params = list(task_str.items())[0]
        if cls == 'Task':
            task = DANE.Task(**params)
        elif hasattr(sys.modules['DANE.taskcontainers'], cls) \
                and cls.startswith('task'):
            cls = getattr(sys.modules['DANE.taskcontainers'], cls)
            task = cls(params)
        else: 
            raise TypeError(
                    "{} must be Task or taskContainer subclass".format(task_str))
    else:
        raise ValueError("Expected task_str to be str or serialised class dict.")
    return task
