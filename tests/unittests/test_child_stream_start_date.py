import unittest
import tap_pendo.streams as streams
from unittest import mock
from singer.utils import now, strftime, strptime_to_utc
from dateutil.parser import parse
from datetime import timedelta

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
        return ("stream", [{"id": 1, "date": "2021-02-01T00:00:00Z"},
                           {"id": 2, "date": "2021-03-01T00:00:00Z"}]), False

    def __init__(self, config):
        self.config = config


class SeptemberChildStream(ChildStream):
    """A child stream whose only event is far ahead of the scan's original start date"""

    def sync(*args, **kwargs):
        TEST.append(args)
        return ("stream", [{"id": 1, "date": "2021-09-01T00:00:00Z"}]), False


class FebruaryChildStream(ChildStream):
    """A child stream whose only event is shortly after the scan's original start date"""

    def sync(*args, **kwargs):
        TEST.append(args)
        return ("stream", [{"id": 2, "date": "2021-02-01T00:00:00Z"}]), False

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

    def test_visitor_history_start_date_greater_than_180_days(self):
        """
        Verify that if visitor history start date is older than 179 days then it is set (now - 179 days)
        """
        start_date = strptime_to_utc("2021-01-01T00:00:00Z")
        expected_start_date = streams.round_times(now(), now())[0] - timedelta(179)
        self.assertEqual(streams.get_absolute_start_end_time(start_date)[0],
                         expected_start_date,
                         msg="Older than 179 days visitor history start date is resetting to (now - 179 days)")

    def test_visitor_history_start_date_equals_180_days(self):
        """
        Verify that if visitor history start date is set properly if it is exactly 179 days older
        """
        start_date = now() - timedelta(179)
        expected_start_date = streams.round_times(start_date, start_date)[0]
        self.assertEqual(streams.get_absolute_start_end_time(start_date)[0],
                         expected_start_date,
                         msg="Exactly 179 days older visitor history start date is not getting set properly")

    def test_visitor_history_start_date_lesser_than_180_days(self):
        """
        Verify that if visitor history start date is set properly if it is lesser than 179 days older
        """
        start_date = now() - timedelta(50)
        expected_start_date = streams.round_times(start_date, start_date)[0]
        self.assertEqual(streams.get_absolute_start_end_time(start_date)[0],
                         expected_start_date,
                         msg="Lesser than 179 days older visitor history start date is not getting set properly")

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

        TEST.clear()
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
            self.assertEqual(parsed_start_date, config.get("start_date"))

    @mock.patch("singer.write_schema")
    @mock.patch("tap_pendo.streams.Stream.update_bookmark")
    @mock.patch("tap_pendo.streams.update_currently_syncing")
    @mock.patch("singer.metadata.to_map")
    @mock.patch("singer.Transformer.transform")
    @mock.patch("singer.write_records")
    def test_lower_bound_fixed_across_parent_batches(self, mocked_write_records, mocked_transform,
                                                     mocked_metadata_to_map, mocked_update_currently_syncing,
                                                     mocked_update_bookmark, mocked_write_schema):
        """
        Regression test for sync_substream silently dropping child events across parent batches:
        sync_substream is invoked once per parent batch (page) of one full parent-level scan. A
        September child event in the first batch must not advance the lower bound used to sync a
        later batch's parent, or that later parent's February child event would be wrongly
        excluded by the (incorrectly advanced) bookmark. The lower bound must only commit the
        accumulated maximum once the final batch is processed.
        """
        config = {"start_date": "2021-01-01T00:00:00Z"}
        mocked_update_bookmark.side_effect = update_bookmark
        mocked_transform.side_effect = transform

        state = {"bookmarks": {}}
        # Same stream_instance (as `self`/parent) is reused across both batches, matching
        # production usage where one Features/Guides/Pages/TrackTypes instance persists across
        # its whole top-level sync (see Stream.sync()'s pagination loop). A real replication_key
        # (matching a field present on the parent records below) is required so sync_substream's
        # own per-record "is this a resumed parent" check doesn't spuriously match every record
        # (as it would if both sides compared None==None).
        stream_instance = streams.Stream(config)
        stream_instance.replication_key = "lastUpdatedAt"

        TEST.clear()

        # Batch 1 (not the final batch of the scan): one parent whose only child event is in
        # September
        stream_instance.sync_substream(state, ParentStream(), SeptemberChildStream(config),
                                       [{"id": 1, "last_updated_at": 1}], is_last_batch=False)

        # The lower bound must still read the scan's original start date after batch 1, since the
        # parent scan isn't finished yet
        self.assertEqual(state["bookmarks"]["test_stream"]["previous_sync_completed_ts"],
                         config["start_date"])

        # Batch 2 (the final batch): a different parent whose only child event is in February
        stream_instance.sync_substream(state, ParentStream(), FebruaryChildStream(config),
                                       [{"id": 2, "last_updated_at": 2}], is_last_batch=True)

        # Both parents (batch 1 and batch 2) must have been synced with the ORIGINAL (January)
        # lower bound - batch 2's parent must not have seen September, or its February event
        # would have been wrongly excluded by the child's own event-time filtering
        start_dates_passed = [parse(strftime(test[2])).strftime("%Y-%m-%dT%H:%M:%SZ") for test in TEST]
        self.assertEqual(start_dates_passed, [config["start_date"], config["start_date"]])

        # Only now, with the final batch processed, is the accumulated maximum (September)
        # committed as the new lower bound for the next sync
        self.assertEqual(strptime_to_utc(state["bookmarks"]["test_stream"]["previous_sync_completed_ts"]),
                         strptime_to_utc("2021-09-01T00:00:00Z"))
