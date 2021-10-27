import unittest
import tap_pendo.streams as streams
from unittest import mock
from singer.utils import strftime
from dateutil.parser import parse

# stores the arguments that are passed in the 'sync'
# function of child stream for assertion
TEST = []

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

# dummy child stream class
class ChildStream:
    schema = None
    stream = Test()
    config = None
    name = "test_stream"
    replication_key = "date"
    key_properties = ["id"]

    # return the data which was passed as argument for transformation in the argument
    def transform(*args, **kwargs):
        return args[1]

    def sync(*args, **kwargs):
        # append 'args' in the TEST variable for assertion
        TEST.append(args)
        # return dummy data
        return [{"id": 1, "date": "2021-02-01T00:00:00Z"},
                {"id": 2, "date": "2021-03-01T00:00:00Z"}]

    def __init__(self, config):
        self.config = config

# dummy parent stream class
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

class TestStartDateOfChildStream(unittest.TestCase):

    @mock.patch("singer.write_schema")
    @mock.patch("tap_pendo.streams.Stream.update_bookmark")
    @mock.patch("tap_pendo.streams.update_currently_syncing")
    @mock.patch("singer.metadata.to_map")
    @mock.patch("singer.Transformer.transform")
    @mock.patch("singer.write_records")
    def test_run(self, mocked_write_records, mocked_transform, mocked_metadata_to_map, mocked_update_currently_syncing, mocked_update_bookmark, mocked_write_schema):
        """
            Test case for verifying if the start date / bookmark is used for fetching records
            of child stream rather than the updated bookmark from previous child stream sync
        """
        # config file
        config = {"start_date": "2021-01-01T00:00:00Z"}

        # create dummy parent records
        mock_records = [{"id":1}, {"id":2}, {"id":3}]

        # mock update bookmark
        mocked_update_bookmark.side_effect = update_bookmark
        # mock singer transform
        mocked_transform.side_effect = transform

        stream_instance = streams.Stream(config)

        # call function
        stream_instance.sync_substream({"bookmarks": {}}, ParentStream(), ChildStream(config), mock_records)

        # iterate over 'TEST' and verify if the start date was passed as argument rather than the updated bookmark
        for test in TEST:
            # get start date from TEST
            start_date = test[2]
            # parse start date as it is in the format: 2021-01-01T00:00:00.000000Z
            parsed_start_date = parse(strftime(start_date)).strftime("%Y-%m-%dT%H:%M:%SZ")
            # verify if the 'parsed_start_date' is same as the start date from config file
            self.assertEquals(parsed_start_date, config.get("start_date"))
