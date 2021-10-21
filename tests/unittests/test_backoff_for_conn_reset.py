from collections.abc import Generator
from unittest import mock
from pytest import raises
from tap_pendo.streams import Endpoints, Visitors

config = {'x_pendo_integration_key': "TEST_KEY"}
stream = Visitors(config=config)
stream.endpoint = Endpoints('', 'GET')


@mock.patch('time.sleep', return_value=None)
@mock.patch('requests.Session.send')
def test_request_backoff_on_remote_timeout_conn_reset(mock_send, mock_sleep):
    mock_send.side_effect = ConnectionResetError()

    with raises(ConnectionResetError) as ex:
        stream.request(endpoint=None)
    # Assert backoff retry count as expected
    # because there is a time.sleep in pendo_utils.rate_limit and in the backoff implementation also
    # so the total count sums up to 4 + 4 = 8
    assert mock_sleep.call_count == 8 