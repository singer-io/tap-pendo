from unittest import mock
from urllib3.exceptions import ReadTimeoutError
from tap_pendo.streams import Endpoints, Server42xRateLimitError, Visitors
import unittest
import socket
from requests.models import ProtocolError
import requests

class Mockresponse:
    def __init__(self, status_code, raise_error, reason="test", headers=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.headers = headers
        self.reason = reason
        self.raw = '{"results": [{"key1": "value1", "key2": "value2"}]}'
        self.response = 'test'

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return True

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.exceptions.RequestException("Sample message", response=self)

    def close(self):
        return True

def get_response(status_code, raise_error, reason):
    return Mockresponse(status_code, raise_error, reason)

# def mocked_ijson(*args, **kwargs):
def items(*args, **kwargs):
    raise ConnectionResetError("Connection is reset.")
    yield {"key1": "value1", "key2": "value2"}

@mock.patch("time.sleep")
class TestConnectionResetError(unittest.TestCase):
    config = {'x_pendo_integration_key': "TEST_KEY"}
    stream = Visitors(config=config)
    stream.endpoint = Endpoints('', 'GET')

    @mock.patch('requests.Session.send')
    def test_connection_reset_error__from_send(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        with self.assertRaises(ConnectionResetError):
            list(self.stream.request(endpoint=None))

        # verify if the request was called 15 times
        self.assertEquals(mocked_send.call_count, 15)

    @mock.patch('http.client.HTTPResponse.readinto')
    def test_connection_reset_error__from_ijson(self, mocked_request, mocked_sleep):
        # mock request and raise error
        mocked_request.side_effect = socket.error(104, 'Connection reset by peer')

        with self.assertRaises(ProtocolError):
            list(self.stream.request(endpoint=None))

        # verify if the request was called 15 times
        self.assertEquals(mocked_request.call_count, 15)

    @mock.patch('http.client.HTTPResponse.readinto')
    def test_timeout_error__from_ijson(self, mocked_request, mocked_sleep):
        # mock request and raise error
        mocked_request.side_effect = socket.timeout('The read operation timed out')

        with self.assertRaises(ReadTimeoutError):
            list(self.stream.request(endpoint=None))

        # verify if the request was called 15 times
        self.assertEquals(mocked_request.call_count, 15)

    @mock.patch('requests.Session.send')
    @mock.patch('ijson.items')
    def test_error__from_ijson(self, mocked_ijson_items, mocked_send, mocked_sleep):
        """
            Test case to verify we backoff for errors raised from 'ijson.items'
        """
        # mock request and return dummy data
        mocked_send.return_value = get_response(200, False, "test")
        # mock ijson.items and replace with generator function that raises error
        mocked_ijson_items.side_effect = items

        with self.assertRaises(ConnectionError):
            list(self.stream.request(endpoint=None))

        # verify if the request was called 15 times
        self.assertEquals(mocked_send.call_count, 15)

    @mock.patch('requests.Session.send')
    @mock.patch('tap_pendo.streams.LOGGER.info')
    def test_429_error_backoff(self, mocked_logger_info, mocked_send, mocked_sleep):
        """
            Test case to verify we backoff for 429 error
        """
        # mock request and return dummy data
        mocked_send.return_value = get_response(429, True, "Too Many Requests")

        with self.assertRaises(Server42xRateLimitError):
            list(self.stream.request(endpoint=None))

        # verify if the request was called 15 times
        self.assertEquals(mocked_send.call_count, 15)
        mocked_logger_info.assert_called_with("Rate limit reached. Sleeping for %s seconds", 30)

    @mock.patch('requests.Session.send')
    def test_raise_error(self, mocked_send, mocked_sleep):
        """
            Test case to verify we raise any error with status code between 400 and 500 except 429 error
        """
        # mock request and return dummy data
        mocked_send.return_value = get_response(402, True, "test")

        with self.assertRaises(requests.exceptions.RequestException):
            list(self.stream.request(endpoint=None))

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 1)
