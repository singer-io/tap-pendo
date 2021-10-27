import unittest
from unittest import mock
from tap_pendo.sync import sync_full_table
import tap_pendo.streams as streams

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

class TestFullTableSyncRecordCount(unittest.TestCase):

    @mock.patch("tap_pendo.streams.Stream.sync")
    @mock.patch("singer.write_record")
    def test_valid_value_for_replication_key(self, mocked_write, mocked_sync):
        """
            Verify that 'counter.value' ie. number of records returned from
            'sync_full_table' is same as the number of records
        """

        mock_config = mock_state = {}

        # create dummy records
        mock_records = [{"id":1, "name": "test1"},
                        {"id":2, "name": "test2"},
                        {"id":2, "name": "test3"}]

        # 'sync' returns Stream class and records
        mocked_sync.return_value = MockStream('test'), mock_records

        stream_instance = streams.Stream(mock_config)
        stream_instance.stream = MockStream('test')

        # call the full table sync function
        counter = sync_full_table(mock_state, stream_instance)

        # verify that the counter is same as the number of dummy records
        self.assertEqual(counter, len(mock_records))
