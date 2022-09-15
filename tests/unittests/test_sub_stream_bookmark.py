import unittest
import tap_pendo.streams as streams
from unittest import mock
from singer.utils import strptime_to_utc

# Mocked helper class for ChildStream
class Schema:
    schema = None
    def __init__(self, schema):
        self.schema = schema

    def to_dict(self):
        return self.schema

class Test:
    schema = Schema({})
    metadata = {}
    tap_stream_id = "test"

# Mocked child stream class
class ChildStream:
    schema = None
    stream = Test()
    config = None
    name = "test_sub_stream"
    replication_key = "date"
    key_properties = ["id"]

    # return the data which was passed as argument for transformation in the argument
    def transform(*args, **kwargs):
        return args[1]

    def sync(*args, **kwargs):
        return [{"id": 1, "date": "2021-02-01T00:00:00Z"},
                {"id": 2, "date": "2021-03-01T00:00:00Z"}]

    def __init__(self, config):
        self.config = config

# Mocked parent stream class
class ParentStream:
    schema = None
    name = "test_stream"
    key_properties = ["id"]

    def transform(*args, **kwargs):
        return {}

    def sync(*args, **kwargs):
        return []

def update_bookmark(state, stream, bookmark_value, bookmark_key):
    if not state.get("bookmarks").get(stream):
        state["bookmarks"][stream] = {}
    state["bookmarks"][stream][bookmark_key] = bookmark_value

def transform(*args, **kwargs):
    # return the data with was passed for transformation in the argument
    return args[0]

@mock.patch("singer.write_schema")
@mock.patch("tap_pendo.streams.Stream.update_bookmark", side_effect=update_bookmark)
@mock.patch("tap_pendo.streams.update_currently_syncing")
@mock.patch("singer.metadata.to_map")
@mock.patch("singer.Transformer.transform", side_effect = transform)
@mock.patch("singer.write_records")
@mock.patch("tap_pendo.streams.now")
class TestSubStreamBookmarking(unittest.TestCase):

    def test_sub_stream_bookmarking_in_normal_sync(self, mocked_now, mocked_write_records, mocked_transform, mocked_metadata_to_map, mocked_update_currently_syncing, mocked_update_bookmark, mocked_write_schema):
        """
            Test case for verifying if that update_bookmark is called properly for normal sync.
                1 time for writing 'in_progress_sync_start' in starting.
                2 times for first parent's child sync to write 'last_processed' and 'max_bookmark_seen'
                2 times for second parent's child sync to write 'last_processed' and 'max_bookmark_seen'
                1 time for writing final bookmark at last
        """
        # config file, mocked parent's records and now() function
        config = {"start_date": "2021-01-01T00:00:00Z"}
        mock_records = [{"id":"parent1"}, {"id":"parent2"}]
        mocked_now.return_value = strptime_to_utc("2021-02-01T00:00:00Z")

        # Expected update_bookmark calls
        state = {'bookmarks': {'test_sub_stream': {'date': '2021-02-01T00:00:00.000000Z'}}}
        sub_stream = 'test_sub_stream'
        expected_update_bookmark_calls = [
            # Verify that in_progress_sync_start is added to STATE before sub stream sync
            mock.call(bookmark_key='in_progress_sync_start', bookmark_value='2021-02-01T00:00:00.000000Z', state=state, stream=sub_stream),
            # Verify that last_processed is updated with 'parent1' and max_bookmark_seen is updated after first parent
            mock.call(bookmark_key='last_processed', bookmark_value='parent1', state=state, stream=sub_stream),
            mock.call(bookmark_key='max_bookmark_seen', bookmark_value='2021-03-01T00:00:00.000000Z', state=state, stream=sub_stream),
            # Verify that last_processed is updated with 'parent2' and max_bookmark_seen is updated after second parent
            mock.call(bookmark_key='last_processed', bookmark_value='parent2', state=state, stream=sub_stream),
            mock.call(bookmark_key='max_bookmark_seen', bookmark_value='2021-03-01T00:00:00.000000Z', state=state, stream=sub_stream),
            # Verify final bookmark is updated after syncing child stream for all parent records 
            mock.call(bookmark_key='date', bookmark_value='2021-02-01T00:00:00.000000Z', state=state, stream=sub_stream)
        ]
        
        # call sync_substream function
        stream_instance = streams.Stream(config)
        stream_instance.sync_substream({"bookmarks": {}}, ParentStream(), ChildStream(config), mock_records)

        # Verify that update_bookmark called with expected calls
        self.assertEquals(mocked_update_bookmark.mock_calls, expected_update_bookmark_calls)

    def test_sub_stream_bookmarking_in_interrupted_sync(self, mocked_now, mocked_write_records, mocked_transform, mocked_metadata_to_map, mocked_update_currently_syncing, mocked_update_bookmark, mocked_write_schema):
        """
            Test case for verifying if that update_bookmark is called properly for interrupted sync.
                Not called to update 'in_progress_sync_start' as it's interrupted sync.
                Not called for first parent as "last_processed" is "parent2" so resume from there
                2 times for second parent's child sync to write 'last_processed' and 'max_bookmark_seen'
                1 time for writing final bookmark at last
        """
        # config file, interrupted state, mocked parent's records and now() function
        config = {"start_date": "2021-01-01T00:00:00Z"}
        interrupted_state = {'bookmarks': {"test_sub_stream": {"in_progress_sync_start": "2021-02-01T00:00:00.000000Z", "last_processed": "parent2", "max_bookmark_seen": "2021-03-01T00:00:00.000000Z"}}}
        mock_records = [{"id":"parent1"}, {"id":"parent2"}]
        mocked_now.return_value = strptime_to_utc("2021-02-01T00:00:00Z")

        # Expected update_bookmark calls
        state = {'bookmarks': {'test_sub_stream': {'date': '2021-02-01T00:00:00.000000Z'}}}
        sub_stream = 'test_sub_stream'
        expected_update_bookmark_calls = [
            # Verify that last_processed is updated with parent2 and max_bookmark_seen is updated after second parent
            mock.call(bookmark_key='last_processed', bookmark_value='parent2', state=state, stream=sub_stream),
            mock.call(bookmark_key='max_bookmark_seen', bookmark_value='2021-03-01T00:00:00.000000Z', state=state, stream=sub_stream),
            # Verify final bookmark is updated after syncing child stream for all parent records 
            mock.call(bookmark_key='date', bookmark_value='2021-02-01T00:00:00.000000Z', state=state, stream=sub_stream)
        ]
        
        # call sync_substream function with interrupted state
        stream_instance = streams.Stream(config)
        stream_instance.sync_substream(interrupted_state, ParentStream(), ChildStream(config), mock_records)

        # Verify that update_bookmark called with expected calls
        self.assertEquals(mocked_update_bookmark.mock_calls, expected_update_bookmark_calls)
