from unittest import mock
from tap_pendo.streams import Endpoints, Visitors
import unittest
import socket
from requests.models import ProtocolError

config = {'x_pendo_integration_key': "TEST_KEY"}
stream = Visitors(config=config)
stream.endpoint = Endpoints('', 'GET')

@mock.patch("time.sleep")
@mock.patch('requests.Session.send')
class TestConnectionResetError(unittest.TestCase):

    def test_connection_reset_error__accounts(self, mocked_send, mocked_sleep):
        # mock request and raise error
        
        config = {'x_pendo_integration_key': "TEST_KEY"}
        # initialize 'visitors' stream class
        visitors = Visitors(config=config)
        stream.endpoint = Endpoints('', 'GET')

        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        try:
            visitors.request(endpoint=None)
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)