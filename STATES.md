# States in DANE

Once a DANE worker has completed a task, or task progression has been interrupted due to an error, it should return a JSON object consisting of a `state` and a `message`.
The message is expected to be an informative, and brief, indication of what went wrong, this message is not intended for automatic processing. 

The state returned by a worker is used for automatic processing in DANE, based on this state it is determined whether a job is completed, in progress, requires retrying, or 
requires manual intervention. The state is one of the numerical [HTTP Status codes](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status), with the aim of trying to adhere
to the semantics of what the status code represents. For example, the state 200 indicates that the task has been successfully handled, whereas 102 indicates it is still in progress.
Below we provide an overview of all used state codes and how they are handled by DANE.

## State overview

* 102: Task is sent to a queue, it might be being worked on or held in queue.
* 200: Task completed successfully.
* 201: Task is registered, but has not been acted upon.
* 400: Malformed request, typically the job description.
* 403: Access denied to underlying source material.
* 404: Underlying source material not found.
* 422: If a task cannot be routed to a queue, this state is returned.
* 500: Error occured during processing, details should be given in message.
* 502: Worker received invalid or partial input.
* 503: Worker received an error response from a remote service it depends on. 

Tasks with state 502 or 503, can be retried automatically. Whereas states 400, 403, 404, 422, and 500 require manual intervention. Once a manual intervention has taken place
the job can be resumed.
