class WarehouseError(Exception):
    """ Base class for exceptions in this module. """

    pass


class ArgumentError(WarehouseError):
    """ Exception for invalid combinations of function arguments """

    pass


class MetadataError(WarehouseError):
    """ Exception that is specifically for any invalid metadata. """

    pass


class OperationError(WarehouseError):
    """ Exception for invalid function calls on the warehouse for a given state. """

    pass
