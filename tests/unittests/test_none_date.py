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

class TestNoneReplicatioKeys(unittest.TestCase):
    '''
        Verify that none value for replication key in data is handled properly
    '''

    @mock.patch("requests.Session.send")
    @mock.patch("tap_pendo.streams.Stream.sync")
    @mock.patch("singer.write_record")
    def test_valid_value_for_replication_key(self, mocked_write, mocked_sync, mocked_request):
        '''
            Verify that if replication key value are present in valid form then tap
            write all valid records in tap output
        '''
        mock_config = mock_state = {}
        mock_start_date = "2021-01-01T00:00:00Z"
        mock_records = [{"id":1, "lastupdated": "2021-09-01T00:00:00Z"},
                        {"id":2, "lastupdated": "2021-09-02T00:00:00Z"}]
        mocked_sync.return_value = MockStream('test'), mock_records

        stream_instance = streams.Stream(mock_config)
        stream_instance.replication_key = 'lastupdated'# set replication ley
        stream_instance.stream = MockStream('test')
        no_of_record = sync_stream(mock_state, mock_start_date, stream_instance)

        # Verify that write record is called for 2 records 
        self.assertEqual(mocked_write.call_count, 2)
        self.assertEqual(no_of_record, 2)

    @mock.patch("requests.Session.send")
    @mock.patch("tap_pendo.streams.Stream.sync")
    @mock.patch("singer.write_record")
    def test_none_or_no_value_for_replication_key(self, mocked_write, mocked_sync, mocked_request):
        '''
            Verify that if replication key not present or null value in data then tap should not break and
            write all such records in tap output
        '''
        mock_config = mock_state = {}
        mock_start_date = "2021-01-01T00:00:00Z"
        mock_records = [{"id":1},# No replication key present
                        {"id":2, "lastupdated": "2021-09-01T00:00:00Z"},
                        {"id":3, "lastupdated": None}] # Replication key with None value
        mocked_sync.return_value = MockStream('test'), mock_records

        stream_instance = streams.Stream(mock_config)
        stream_instance.replication_key = 'lastupdated'# set replication ley
        stream_instance.stream = MockStream('test')
        no_of_record = sync_stream(mock_state, mock_start_date, stream_instance)

        # Verify that write record is called for 3 records 
        self.assertEqual(mocked_write.call_count, 3)
        self.assertEqual(no_of_record, 3)



class TestNoneReplicatioKeysInSubStreams(unittest.TestCase):
    '''
        Verify that none value for replication key in data is handled properly
    '''

    @mock.patch("requests.Session.send")
    @mock.patch("tap_pendo.streams.Stream.sync")
    @mock.patch("singer.write_record")
    def test_valid_value_for_replication_key_sub_stream(self, mocked_write, mocked_sync, mocked_request):
        '''
            Verify that if replication key value are present in valid form then tap
            write all valid records in tap output for sub stream
        '''
        mock_config = {"start_date": "2021-01-01T00:00:00Z"} 
        mock_state = {}
        mock_parent_data = [{"id": 1}]
        mock_records = [{"id":1, "lastupdated": "2021-09-01T00:00:00Z"},
                        {"id":2, "lastupdated": "2021-09-02T00:00:00Z"}]
        mocked_sync.return_value = mock_records

        parent_instance = streams.Stream(mock_config)
        sub_stream = streams.Stream(mock_config)
        sub_stream.replication_key = 'lastupdated'# set replication ley
        sub_stream.stream = MockStream('test')
        parent_instance.sync_substream(mock_state, parent_instance, sub_stream, mock_parent_data)

        # Verify that write record is called for 2 records 
        self.assertEqual(mocked_write.call_count, 2)

    @mock.patch("requests.Session.send")
    @mock.patch("tap_pendo.streams.Stream.sync")
    @mock.patch("singer.write_record")
    def test_none_or_no_value_for_replication_key_sub_stream(self, mocked_write, mocked_sync, mocked_request):
        '''
            Verify that if replication key not present or null value in data then tap should not break and
            write all such records in tap output for sub stream
        '''
        mock_config = {"start_date": "2021-01-01T00:00:00Z"} 
        mock_state = {}
        mock_parent_data = [{"id": 1}]
        mock_records = [{"id":1},# No replication key present
                        {"id":2, "lastupdated": "2021-09-01T00:00:00Z"},
                        {"id":3, "lastupdated": None}] # Replication key with None value
        mocked_sync.return_value = mock_records

        parent_instance = streams.Stream(mock_config)
        sub_stream = streams.Stream(mock_config)
        sub_stream.replication_key = 'lastupdated'# set replication ley
        sub_stream.stream = MockStream('test')
        parent_instance.sync_substream(mock_state, parent_instance, sub_stream, mock_parent_data)

        # Verify that write record is called for 3 records 
        self.assertEqual(mocked_write.call_count, 3)
