class MissingEndpointError(Exception):
    """Raised when an action fails due to lack of API.
    """
    pass

class APIRegistrationError(Exception):
    """Raised when registering the API fails.
    """
    pass

class ResourceConnectionError(ConnectionError):
    """Raised when a component cant connect to a resource it depends on.

    Used for catching resource specific errors, and wrapping them
    in a soft blanket of custom error handling."""
    pass

class RefuseJobException(Exception):
    """Exception for workers to throw when they want to refuse a job
    at this point in time.

    This will result in a nack (no ack) being sent back to the queue, 
    causing the job to be requeued (at the or close to the head of the queue).
    """
    pass
