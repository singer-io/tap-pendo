import copy
import unittest
import weakref
from unittest import mock

from singer.utils import strptime_to_utc

import tap_pendo.streams as streams
from tap_pendo.sync import sync_stream

START_DATE = strptime_to_utc("2021-01-01T00:00:00Z")
T0 = int(START_DATE.timestamp() * 1000)
HOUR_MS = 60 * 60 * 1000
APP_IDS = 'expandAppIds("*")'


def accounts(*offsets):
    return {"results": [{"account_id": f"a{o}", "metadata": {"auto": {"lastupdated": T0 + o}}} for o in offsets]}


def features(*offsets):
    return {"results": [{"id": f"f{o}", "last_updated_at": T0 + o} for o in offsets]}


def feature_events(*hours):
    return {"results": [{"visitor_id": f"v{h}", "hour": T0 + h * HOUR_MS} for h in hours]}


class Answers:
    """Stands in for Stream.request: keeps a copy of each body, since the caller edits it afterwards"""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.bodies = []

    def __call__(self, endpoint, json=None, **kwargs):
        self.bodies.append(copy.deepcopy(json))
        return self.answers.pop(0)


def sync_until_done(stream, answers, *args):
    """Calls stream.sync until it reports no more pages; returns (ids, more, requests so far) per call"""
    calls = []
    more = True
    while more:
        (_, records), more = stream.sync({}, START_DATE, *args)
        calls.append(([r.get("account_id") or r.get("visitor_id") or r.get("id") for r in records],
                      more, len(answers.bodies)))
    return calls


@mock.patch("singer.write_state")
@mock.patch("tap_pendo.streams.Stream.request")
class TestOnePagePerSync(unittest.TestCase):
    """A full page goes back to the caller at once, so the tap never holds two pages"""

    def test_accounts(self, mocked_request, mocked_write_state):
        mocked_request.side_effect = answers = Answers(accounts(1, 2), accounts(2, 3), accounts(3))
        stream = streams.Accounts({"record_limit": 2, "app_ids": APP_IDS})

        self.assertEqual(sync_until_done(stream, answers),
                         [(["a1"], True, 1), (["a2"], True, 2), (["a3"], False, 3)])
        self.assertEqual([body["request"]["pipeline"][2]["filter"] for body in answers.bodies],
                         [f"metadata.auto.lastupdated>={T0 + n}" for n in (0, 2, 3)])

    def test_event_stream(self, mocked_request, mocked_write_state):
        mocked_request.side_effect = answers = Answers(feature_events(1, 2), feature_events(2, 3),
                                                       feature_events(3))
        stream = streams.FeatureEvents({"period": "hourRange", "record_limit": 2})

        self.assertEqual(sync_until_done(stream, answers, "FEATURE-ID"),
                         [(["v1"], True, 1), (["v2"], True, 2), (["v3"], False, 3)])
        self.assertEqual([body["request"]["pipeline"][0]["source"]["timeSeries"]["first"] for body in answers.bodies],
                         [T0, T0 + 2 * HOUR_MS, T0 + 3 * HOUR_MS])

    @mock.patch("tap_pendo.streams.Stream.sync_substream")
    @mock.patch("tap_pendo.streams.Stream.is_selected", return_value=True)
    def test_parent_of_a_child_stream_keeps_whole_batches(self, mocked_selected, mocked_substream,
                                                          mocked_request, mocked_write_state):
        """sync_substream moves the child bookmark once per call, so it must see every parent of a batch"""
        mocked_request.side_effect = answers = Answers(features(1, 2), features(2, 3), features(3))
        stream = streams.Features({"record_limit": 2, "app_ids": APP_IDS})

        self.assertEqual(sync_until_done(stream, answers), [(["f1", "f2"], True, 2), (["f3"], False, 3)])
        self.assertEqual([[r["id"] for r in call.args[3]] for call in mocked_substream.call_args_list],
                         [["f1", "f2"], ["f3"]])


class Page(list):
    """A list that weakref can follow"""


class Schema:
    def to_dict(self):
        return {}


class CatalogStream:
    tap_stream_id = "pages_of_one"
    schema = Schema()
    metadata = []


class PagesOfOne(streams.Stream):
    """Returns two one-record pages and notes whether the first is still alive when the second is requested"""
    name = "pages_of_one"
    replication_key = "lastupdated"

    def __init__(self):
        super().__init__({"start_date": "2021-01-01T00:00:00Z"})
        self.stream = CatalogStream()
        self.previous = None
        self.previous_alive = []

    def sync(self, state, start_date=None, key_id=None, parent_last_updated=None):
        if self.previous is not None:
            self.previous_alive.append(self.previous() is not None)
        page = Page([{"id": len(self.previous_alive), "lastupdated": "2021-01-02T00:00:00Z"}])
        self.previous = weakref.ref(page)
        return (self.stream, page), not self.previous_alive


@mock.patch("singer.write_schema")
@mock.patch("singer.write_record")
@mock.patch("singer.write_state")
class TestPreviousPageIsFreed(unittest.TestCase):
    """The caller drops a page before it asks for the next one"""

    def test_sync_stream(self, mocked_write_state, mocked_write_record, mocked_write_schema):
        stream = PagesOfOne()

        self.assertEqual(sync_stream({}, "2021-01-01T00:00:00Z", stream), 2)
        self.assertEqual(stream.previous_alive, [False])

    def test_sync_substream(self, mocked_write_state, mocked_write_record, mocked_write_schema):
        parent = streams.Stream({"start_date": "2021-01-01T00:00:00Z"})
        child = PagesOfOne()

        parent.sync_substream({}, parent, child, [{"id": "parent-1"}])

        self.assertEqual(mocked_write_record.call_count, 2)
        self.assertEqual(child.previous_alive, [False])
