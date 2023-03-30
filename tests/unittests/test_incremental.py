import unittest
from unittest import mock

import singer
import tap_pendo.streams as streams
from tap_pendo.sync import sync_stream

class Schema:
    schema = None

    def __init__(self, schema):
        self.schema = schema

    def to_dict(self):
        return self.schema

class MockStream:
    tap_stream_id = None
    schema = None
    metadata = {}

    def __init__(self, id):
        self.tap_stream_id = id
        self.schema = Schema({})

@mock.patch("requests.Session.send")
@mock.patch("tap_pendo.streams.Stream.sync")
@mock.patch("singer.write_record")
@mock.patch("singer.write_state")
class TestIncremental(unittest.TestCase):

    def test_scenario_1(self, mocked_state, mocked_write, mocked_sync, mocked_request):
        '''
            Verify that all records are written as both are as
            the replication key is later than start date
        '''
        mock_config = mock_state = {}
        mock_start_date = "2021-01-10T00:00:00Z"
        mock_records = [{"id":1, "lastupdated": "2021-01-12T00:00:00Z"},
                        {"id":2, "lastupdated": "2021-01-15T00:00:00Z"}]
        mocked_sync.return_value = (MockStream('test'), mock_records), False

        stream_instance = streams.Stream(mock_config)
        stream_instance.replication_key = 'lastupdated'
        stream_instance.stream = MockStream('test')
        sync_stream(mock_state, mock_start_date, stream_instance)

        # Verify that write record is called for 2 records 
        self.assertEqual(mocked_write.call_count, 2)

    def test_scenario_2(self, mocked_state, mocked_write, mocked_sync, mocked_request):
        '''
            Verify that only 1 record is written as
            it is updated later than the start date
        '''
        mock_config = mock_state = {}
        mock_start_date = "2021-01-10T00:00:00Z"
        mock_records = [{"id":1, "lastupdated": "2021-01-12T00:00:00Z"},
                        {"id":2, "lastupdated": "2021-01-08T00:00:00Z"}]
        mocked_sync.return_value = (MockStream('test'), mock_records), False

        stream_instance = streams.Stream(mock_config)
        stream_instance.replication_key = 'lastupdated'
        stream_instance.stream = MockStream('test')
        sync_stream(mock_state, mock_start_date, stream_instance)

        # Verify that write record is called for 1 records 
        self.assertEqual(mocked_write.call_count, 1)

    def test_scenario_3(self, mocked_state, mocked_write, mocked_sync, mocked_request):
        '''
            Verify that none of the records were written
            as both were updated before the start date
        '''
        mock_config = mock_state = {}
        mock_start_date = "2021-01-10T00:00:00Z"
        mock_records = [{"id":1, "lastupdated": "2021-01-01T00:00:00Z"},
                        {"id":2, "lastupdated": "2021-01-08T00:00:00Z"}]
        mocked_sync.return_value = (MockStream('test'), mock_records), False

        stream_instance = streams.Stream(mock_config)
        stream_instance.replication_key = 'lastupdated'
        stream_instance.stream = MockStream('test')
        sync_stream(mock_state, mock_start_date, stream_instance)

        # Verify that write record is called for 0 records 
        self.assertEqual(mocked_write.call_count, 0)
