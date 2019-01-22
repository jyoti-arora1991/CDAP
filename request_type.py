from enum import Enum


class RequestType(Enum):
    GET, \
    POST, \
    PUT, \
    DELETE, \
    HEAD = xrange(5)