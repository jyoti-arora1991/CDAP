import logging

import requests

from request_type import RequestType


class ApiRequest(object):
    def __init__(self):
        self._logger = logging.getLogger(__name__)

    def execute_request(self, api_request_specification):
        """Execute the API request as per the request specification object.

        :type api_request_specification: :class:`nimble.core.api.api_request_specification.ApiRequestSpecification`
        :return: Response of the API request.
        :rtype: :class:`requests.Response`
        """
        request_type = api_request_specification.request_type
        self._logger.info(
            "Executing %s request with specification: %s" % (request_type, api_request_specification.__dict__))
        if request_type == RequestType.GET:
            response = requests.get(api_request_specification.url, params=api_request_specification.query_params,
                                    headers=api_request_specification.headers,
                                    timeout=api_request_specification.timeout, verify=api_request_specification.verify,
                                    auth=api_request_specification.auth)
        elif request_type == RequestType.POST:
            response = requests.post(api_request_specification.url, data=api_request_specification.data,
                                     json=api_request_specification.json, headers=api_request_specification.headers,
                                     params=api_request_specification.query_params,
                                     timeout=api_request_specification.timeout, files=api_request_specification.files,
                                     verify=api_request_specification.verify, auth=api_request_specification.auth)
        elif request_type == RequestType.PUT:
            response = requests.put(api_request_specification.url, data=api_request_specification.data,
                                    json=api_request_specification.json, headers=api_request_specification.headers,
                                    params=api_request_specification.query_params,
                                    timeout=api_request_specification.timeout, files=api_request_specification.files,
                                    verify=api_request_specification.verify, auth=api_request_specification.auth)
        elif request_type == RequestType.DELETE:
            response = requests.delete(api_request_specification.url, headers=api_request_specification.headers,
                                       timeout=api_request_specification.timeout,
                                       verify=api_request_specification.verify, auth=api_request_specification.auth)
        else:
            raise ValueError("Request type %s not supported" % request_type)
        if response.ok:
            self._logger.info("API Response code: %s" % response.status_code)
        else:
            self._logger.error(
                "API failed with status code:- %s\n%s" % (response.status_code, response.text))
        return response
