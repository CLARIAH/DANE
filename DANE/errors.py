class MissingEndpointError(Exception):
    pass

class APIRegistrationError(Exception):
    pass

class ResourceConnectionError(ConnectionError):
    # raised when component cant connect to a resource it depends on
    # used for catching resource specific errors, and wrapping them
    # in a soft blanket of custom error handling
    pass

