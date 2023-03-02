import unittest
import requests
from unittest import mock
import tap_pendo.streams as streams

class Mockresponse:
    def __init__(self, status_code, json, raise_error, headers=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.headers = headers
        self.reason = "test"
        self.raw = '{"results": [{"key1": "value1", "key2": "value2"}]}'

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return True

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        return self.text

    def close(self):
        return None

def get_response(json={}):
    return Mockresponse(200, json, False)

@mock.patch("time.sleep")
@mock.patch('requests.Session.send')
class TestTimeOutValue(unittest.TestCase):

    def test_timeout_value_in_config__Stream(self, mocked_send, mocked_sleep):
        """
            Verify if the request was called with timeout value param passed in the config file
        """
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        # pass 'request_timeout' param in the config
        stream = streams.Stream({'x_pendo_integration_key': 'test', 'request_timeout': 100})

        stream.send_request_get_results('test_req', None, {}, 1)

        # verify if the request was called with the desired timeout
        mocked_send.assert_called_with('test_req', timeout=100.0)

    def test_timeout_value_not_in_config__Stream(self, mocked_send, mocked_sleep):
        """
            Verify if the request was called with default timeout value
            as the timeout param is not passed in the config file
        """
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        # not pass 'request_timeout' param in the config
        stream = streams.Stream({'x_pendo_integration_key': 'test'})

        stream.send_request_get_results('test_req', None, {}, 1)

        # verify if the request was called with default timeout
        mocked_send.assert_called_with('test_req', timeout=300.0)

    def test_timeout_string__Stream(self, mocked_send, mocked_sleep):
        """
            Verify if the request was called with integer timeout
            as param passed in the config file is in string
        """
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        # pass string value of 'request_timeout' in the config
        stream = streams.Stream({'x_pendo_integration_key': 'test', 'request_timeout': "100"})

        stream.send_request_get_results('test_req', None, {}, 1)

        # verify if the request was called with passed timeout param
        mocked_send.assert_called_with('test_req', timeout=100.0)

    def test_timeout_empty__Stream(self, mocked_send, mocked_sleep):
        """
            Verify if the request was called with default timeout
            as param passed in the config file is empty string
        """
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        # pass empty string value of 'request_timeout' in the config
        stream = streams.Stream({'x_pendo_integration_key': 'test', 'request_timeout': ""})

        stream.send_request_get_results('test_req', None, {}, 1)

        # verify if the request was called with passed timeout param
        mocked_send.assert_called_with('test_req', timeout=300.0)

    def test_timeout_0__Stream(self, mocked_send, mocked_sleep):
        """
            Verify if the request was called with default timeout
            as param passed in the config file is 0
        """
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        # pass empty string value of 'request_timeout' in the config
        stream = streams.Stream({'x_pendo_integration_key': 'test', 'request_timeout': 0.0})

        stream.send_request_get_results('test_req', None, {}, 1)

        # verify if the request was called with passed timeout param
        mocked_send.assert_called_with('test_req', timeout=300.0)

    def test_timeout_string_0__Stream(self, mocked_send, mocked_sleep):
        """
            Verify if the request was called with default timeout
            as param passed in the config file is string 0
        """
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        # pass empty string value of 'request_timeout' in the config
        stream = streams.Stream({'x_pendo_integration_key': 'test', 'request_timeout': "0.0"})

        stream.send_request_get_results('test_req', None, {}, 1)

        # verify if the request was called with passed timeout param
        mocked_send.assert_called_with('test_req', timeout=300.0)

    def test_timeout_value_in_config__LazyAggregation(self, mocked_send, mocked_sleep):
        """
            Verify if the request was called with timeout value param passed in the config file
        """
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        # pass 'request_timeout' param in the config
        stream = streams.LazyAggregationStream({'x_pendo_integration_key': 'test', 'request_timeout': 100})

        list(stream.send_request_get_results('test_req', None, {}, 1))

        # verify if the request was called with the desired timeout
        mocked_send.assert_called_with('test_req', stream=True, timeout=100.0)

    def test_timeout_value_not_in_config__LazyAggregation(self, mocked_send, mocked_sleep):
        """
            Verify if the request was called with default timeout value
            as the timeout param is not passed in the config file
        """
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        # not pass 'request_timeout' param in the config
        stream = streams.LazyAggregationStream({'x_pendo_integration_key': 'test'})

        list(stream.send_request_get_results('test_req', None, {}, 1))

        # verify if the request was called with default timeout
        mocked_send.assert_called_with('test_req', stream=True, timeout=300.0)

    def test_timeout_string__LazyAggregation(self, mocked_send, mocked_sleep):
        """
            Verify if the request was called with integer timeout
            as param passed in the config file is in string
        """
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        # pass string value of 'request_timeout' in the config
        stream = streams.LazyAggregationStream({'x_pendo_integration_key': 'test', 'request_timeout': "100"})

        list(stream.send_request_get_results('test_req', None, {}, 1))

        # verify if the request was called with passed timeout param
        mocked_send.assert_called_with('test_req', stream=True, timeout=100.0)

    def test_timeout_empty__LazyAggregation(self, mocked_send, mocked_sleep):
        """
            Verify if the request was called with default timeout
            as param passed in the config file is empty string
        """
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        # pass string value of 'request_timeout' in the config
        stream = streams.LazyAggregationStream({'x_pendo_integration_key': 'test', 'request_timeout': ""})

        list(stream.send_request_get_results('test_req', None, {}, 1))

        # verify if the request was called with passed timeout param
        mocked_send.assert_called_with('test_req', stream=True, timeout=300.0)

    def test_timeout_0__LazyAggregation(self, mocked_send, mocked_sleep):
        """
            Verify if the request was called with default timeout
            as param passed in the config file is 0
        """
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        # pass string value of 'request_timeout' in the config
        stream = streams.LazyAggregationStream({'x_pendo_integration_key': 'test', 'request_timeout': 0.0})

        list(stream.send_request_get_results('test_req', None, {}, 1))

        # verify if the request was called with passed timeout param
        mocked_send.assert_called_with('test_req', stream=True, timeout=300.0)

    def test_timeout_string_0__LazyAggregation(self, mocked_send, mocked_sleep):
        """
            Verify if the request was called with default timeout
            as param passed in the config file is string 0
        """
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        # pass string value of 'request_timeout' in the config
        stream = streams.LazyAggregationStream({'x_pendo_integration_key': 'test', 'request_timeout': "0.0"})

        list(stream.send_request_get_results('test_req', None, {}, 1))

        # verify if the request was called with passed timeout param
        mocked_send.assert_called_with('test_req', stream=True, timeout=300.0)
