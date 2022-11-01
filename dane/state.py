from enum import IntEnum, unique


# TODO: PROCESSING as a state has been added and uses the HTTP code priorly assigned
# to QUEUED! Make absolutely sure to handle the changed state properly accross the code


@unique
class ProcState(IntEnum):
    # PROCESSING = 102  # Task is currently being processed by a worker
    SUCCESS = 200  # Task completed successfully.
    CREATED = 201  # Task is registered, but has not been acted upon.
    QUEUED = (
        102  # Change to: 202 later. Task has been accepted and is waiting in the queue
    )
    TASK_RESET = 205  # Task reset state, typically after manual intervention
    BAD_REQUEST = 400  # Malformed request, typically the document or task description.
    ACCESS_DENIED = 403  # Access denied to underlying source material.
    NOT_FOUND = 404  # Underlying source material not found.
    ALREADY_EXISTS = 409  # Document, Task or Result already exists
    UNFINISHED_DEPENDENCY = 412  # Task has a dependency which has not completed yet.
    NO_ROUTE_TO_QUEUE = (
        422  # If a task cannot be routed to a queue, this state is returned.
    )
    ERROR = 500  # Error occurred during processing, details should be given in message.
    ERROR_INVALID_INPUT = 502  # Worker received invalid or partial input.
    ERROR_PROXY = (
        503  # Worker received an error response from a remote service it depends on.
    )
