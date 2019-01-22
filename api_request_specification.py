class ApiRequestSpecification(object):
    def __init__(self, request_type, url, headers=None, query_params=None, data=None, json=None, timeout=None,
                 files=None, verify=True, auth=None):
        """

        :param request_type: Type of the request, like GET, POST, etc.
        :param url: URL which needs to be hit.
        :type url: str
        :param headers: API request headers.
        :type headers: dict
        :param query_params: QUERY parameters appear in the URL after the question mark (?) after the resource name.
        :type query_params: dict
        :param data: Dictionary, list of tuples, bytes, or file-like object to send in the body of the request.
        :param json: JSON string to be passed in the body of the request.
        :type json: dict
        :param timeout: How many seconds to wait for the server to send data before giving up, as a float, or a
            :ref:`(connect timeout, read timeout) <timeouts>` tuple.
        :type timeout: float or tuple
        :param files: Dictionary of ``'name': file-like-objects`` (or ``{'name': file-tuple}``) for multipart encoding
            upload. ``file-tuple`` can be a 2-tuple ``('filename', fileobj)``, 3-tuple ``('filename', fileobj,
            'content_type')`` or a 4-tuple ``('filename', fileobj, 'content_type', custom_headers)``, where
            ``'content-type'`` is a string defining the content type of the given file and ``custom_headers`` a
            dict-like object containing additional headers to add for the file.
        :type files: dict
        :param verify: Either a boolean, in which case it controls whether we verify the server's TLS certificate, or a
            string, in which case it must be a path to a CA bundle to use. Defaults to `True`.
        :type verify: bool or str
        :param auth: Auth tuple or callable to enable Basic/Digest/Custom HTTP Auth.
        :type auth: tuple or :class:`typing.Callable`
        """
        self.request_type = request_type
        self.url = url
        self.headers = headers
        self.query_params = query_params
        self.data = data
        self.json = json
        self.timeout = timeout
        self.files = files
        self.verify = verify
        self.auth = auth

    def set_authorization_key(self, value):
        try:
            self.headers["Authorization"] = value
        except TypeError:
            self.headers = {"Authorization": value}

    def set_data(self, data):
        self.data = data

    def set_json(self, json):
        self.json = json

    def add_header(self, key, value):
        try:
            self.headers[key] = value
        except TypeError:
            self.headers = {key: value}

    def set_content_type(self, content_type):
        self.add_header("Content-Type", content_type.value)

    def set_bearer_token(self, token):
        self.set_authorization_key("Bearer %s" % token)

    def set_basic_token(self, token):
        self.set_authorization_key("Basic %s" % token)