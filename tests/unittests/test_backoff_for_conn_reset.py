from unittest import mock
from tap_pendo.streams import Endpoints, Visitors
import unittest
import socket
from requests.models import ProtocolError

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
            self.stream.request(endpoint=None)

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    @mock.patch('http.client.HTTPResponse.readinto')
    def test_connection_reset_error__from_ijson(self, mocked_request, mocked_sleep):
        # mock request and raise error
        mocked_request.side_effect = socket.error(104, 'Connection reset by peer')

        with self.assertRaises(ProtocolError):
            self.stream.request(endpoint=None)

        # verify if the request was called 5 times
        self.assertEquals(mocked_request.call_count, 5)
