import unittest
import requests
from unittest import mock
from tap_pendo.streams import Visitors
from singer.utils import now, strftime

class Mockresponse:
    def __init__(self, resp, status_code, headers=None, raise_error=False):
        self.status_code = status_code
        self.raw = resp
        self.headers = headers
        self.raise_error = raise_error
        self.reason = "error"

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("mock sample message")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        return True

    def close(self):
        return True

# Mocking sync of substream
def mocked_substream(state, parent, sub_stream, parent_response):
    for record in parent_response:
        pass

class TestLazyAggregationSync(unittest.TestCase):
    '''
        Verify that sync of LazzyAggregation is return all the data
    '''

    @mock.patch("requests.Session.send")
    @mock.patch("tap_pendo.streams.Stream.is_selected")
    @mock.patch("tap_pendo.streams.Stream.sync_substream", side_effect=mocked_substream)
    def test_lazzy_aggregation_with_sub_stream(self, mocked_substream, mocked_selected, mocked_request):
        '''
            Verify that if sub stream is present then also all data should be returned for a super stream
            and sync_substream should be called
        '''
        # Set expectation data with primary key(id) and replication key(lastupdated) value.
        expected_data = [
            {'id': 1, 'metadata': {'auto': {'lastupdated': 1631515992001}}},
            {'id': 2, 'metadata': {'auto': {'lastupdated': 1631515992001}}},
            {'id': 3, 'metadata': {'auto': {'lastupdated': 1631515992001}}}
        ]
        records = '{"results": [{"id":1, "metadata": {"auto": {"lastupdated": 1631515992001}}},\
                                {"id":2, "metadata": {"auto": {"lastupdated": 1631515992001}}},\
                                {"id":3, "metadata": {"auto": {"lastupdated": 1631515992001}}}]}'
        mocked_selected.return_value = True # Sub stream is selected
        mocked_request.return_value = Mockresponse(records, 200, raise_error=False)
        config = {'start_date': strftime(now()),
                  'x_pendo_integration_key': 'test'}

        lazzy_aggr = Visitors(config)
        stream, stream_response = lazzy_aggr.sync({})

        self.assertEqual(list(stream_response), expected_data) # parent stream get all expected data
        self.assertEqual(mocked_substream.call_count, 1)

    @mock.patch("requests.Session.send")
    @mock.patch("tap_pendo.streams.Stream.is_selected")
    @mock.patch("tap_pendo.streams.Stream.sync_substream", side_effect=mocked_substream)
    def test_lazzy_aggregation_without_sub_stream(self, mocked_substream, mocked_selected, mocked_request):
        '''
            Verify that if sub stream is not selected then also all data should be returned for a super stream
            and sync_substream should not be called 
        '''
        # Set expectation data with primary key(id) and replication key(lastupdated) value.
        expected_data = [
            {'id': 1, 'metadata': {'auto': {'lastupdated': 1631515992001}}},
            {'id': 2, 'metadata': {'auto': {'lastupdated': 1631515992001}}},
            {'id': 3, 'metadata': {'auto': {'lastupdated': 1631515992001}}}
        ]
        records = '{"results": [{"id":1, "metadata": {"auto": {"lastupdated": 1631515992001}}},\
                                {"id":2, "metadata": {"auto": {"lastupdated": 1631515992001}}},\
                                {"id":3, "metadata": {"auto": {"lastupdated": 1631515992001}}}]}'
        mocked_selected.return_value = False # Sub stream is not selected
        mocked_request.return_value = Mockresponse(records, 200, raise_error=False)
        config = {'start_date': strftime(now()),
                  'x_pendo_integration_key': 'test'}

        lazzy_aggr = Visitors(config)
        stream, stream_response = lazzy_aggr.sync({})

        self.assertEqual(list(stream_response), expected_data)
        self.assertEqual(mocked_substream.call_count, 0)
