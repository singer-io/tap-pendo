import unittest
from parameterized import parameterized
from tap_pendo.streams import Events, FeatureEvents, GuideEvents, PageEvents, PollEvents, TrackEvents

EVENT_STREAMS = [(cls.__name__, cls) for cls in (Events, FeatureEvents, GuideEvents, PageEvents, PollEvents, TrackEvents)]


def summary_row(**overrides):
    """An hourly row as Pendo returns it. SAC-32693: two rows for one visitor and hour differ only
    in analyticsSessionId and their counts and times."""
    row = {
        "accountId": "Koelpin Inc", "analyticsSessionId": "XyjTPuekwRWJGoZa", "appId": -323232,
        "hour": 1784440800000, "pageId": "page-1", "featureId": "feature-1", "trackTypeId": "track-1",
        "numEvents": 2, "numMinutes": 1, "firstTime": 1784441217514, "lastTime": 1784441217522,
        "remoteIp": "66.249.79.68", "server": "rawcontent", "tabId": "",
        "userAgent": "Mozilla/5.0 (compatible; Googlebot/2.1)", "visitorId": "mbles",
    }
    row.update(overrides)
    return row


def single_event(**overrides):
    """A guide event as Pendo returns it, with an empty eventId"""
    event = {
        "accountId": "Koelpin Inc", "analyticsSessionId": "XyjTPuekwRWJGoZa", "appId": -323232,
        "browserTime": 1784441218006, "eventId": "", "guideId": "guide-1", "guideStepId": "step-1",
        "remoteIp": "66.249.79.68", "serverName": "rawcontent", "tabId": "ehhs2M3NnMrTI7hx",
        "type": "guideSeen", "url": "https://example.com/a", "userAgent": "Googlebot", "visitorId": "mbles",
    }
    event.update(overrides)
    return event


class TestEventStreamsPrimaryKeys(unittest.TestCase):

    @parameterized.expand(EVENT_STREAMS)
    def test_primary_key(self, _, stream_class):
        for period in ('hourRange', 'dayRange'):
            self.assertEqual(stream_class({'period': period}).key_properties, ['_sdc_record_hash'])

    @parameterized.expand(EVENT_STREAMS)
    def test_primary_key_unchanged_by_more_instances(self, _, stream_class):
        """__init__ used to append the period to the class's shared list on every instantiation"""
        for _ in range(3):
            stream = stream_class({'period': 'hourRange'})
        self.assertEqual(stream.key_properties, ['_sdc_record_hash'])

    @parameterized.expand(EVENT_STREAMS)
    def test_record_hash_in_schema(self, _, stream_class):
        self.assertIn('_sdc_record_hash', stream_class({}).load_schema()['properties'])


class TestSummaryRowCompositeId(unittest.TestCase):
    STREAMS = [(cls.__name__, cls) for cls in (Events, FeatureEvents, PageEvents, TrackEvents)]

    @parameterized.expand(STREAMS)
    def test_rows_of_different_sessions_get_different_ids(self, _, stream_class):
        stream = stream_class({'period': 'hourRange'})
        one = stream.transform(summary_row())
        two = stream.transform(summary_row(analyticsSessionId="t4NbzeqHFuxxHNk0", numEvents=1,
                                           firstTime=1784441217501, lastTime=1784441217501))
        self.assertNotEqual(one['_sdc_record_hash'], two['_sdc_record_hash'])

    @parameterized.expand(STREAMS)
    def test_id_ignores_counts_and_times(self, _, stream_class):
        """A row of an open hour has larger counts on the next sync and must update the same row"""
        stream = stream_class({'period': 'hourRange'})
        first = stream.transform(summary_row())
        later = stream.transform(summary_row(numEvents=9, numMinutes=4, lastTime=1784441400000))
        self.assertEqual(first['_sdc_record_hash'], later['_sdc_record_hash'])

    @parameterized.expand([
        ('app', {'appId': 5}),
        ('server', {'server': 'other.example.com'}),
        ('tab', {'tabId': 'tab-2'}),
        ('hour', {'hour': 1784444400000}),
        ('remote_ip', {'remoteIp': '10.0.0.1'}),
    ])
    def test_id_differs_by_field(self, _, overrides):
        stream = Events({'period': 'hourRange'})
        self.assertNotEqual(stream.transform(summary_row())['_sdc_record_hash'],
                            stream.transform(summary_row(**overrides))['_sdc_record_hash'])

    def test_stream_specific_fields(self):
        cases = [(PageEvents, {'pageId': 'page-2'}), (PageEvents, {'parameters': 'a=1'}),
                 (FeatureEvents, {'featureId': 'feature-2'}), (FeatureEvents, {'parameters': 'a=1'}),
                 (TrackEvents, {'trackTypeId': 'track-2'})]
        for stream_class, overrides in cases:
            stream = stream_class({'period': 'hourRange'})
            with self.subTest(stream=stream_class.__name__, overrides=overrides):
                self.assertNotEqual(stream.transform(summary_row())['_sdc_record_hash'],
                                    stream.transform(summary_row(**overrides))['_sdc_record_hash'])


class TestSingleEventCompositeId(unittest.TestCase):

    @parameterized.expand([(cls.__name__, cls) for cls in (GuideEvents, PollEvents)])
    def test_events_with_empty_event_id_get_different_ids(self, _, stream_class):
        stream = stream_class({})
        one = stream.transform(single_event())
        two = stream.transform(single_event(browserTime=1784441218007))
        self.assertNotEqual(one['_sdc_record_hash'], two['_sdc_record_hash'])

    def test_id_differs_by_field(self):
        stream = GuideEvents({})
        base = stream.transform(single_event())['_sdc_record_hash']
        for overrides in ({'type': 'guideDismissed'}, {'guideStepId': 'step-2'}, {'serverName': 'other'},
                          {'tabId': 'tab-2'}, {'analyticsSessionId': 'other'}, {'url': 'https://example.com/b'}):
            with self.subTest(overrides=overrides):
                self.assertNotEqual(base, stream.transform(single_event(**overrides))['_sdc_record_hash'])

    def test_poll_id_is_part_of_id(self):
        stream = PollEvents({})
        self.assertNotEqual(stream.transform(single_event(pollId='poll-1'))['_sdc_record_hash'],
                            stream.transform(single_event(pollId='poll-2'))['_sdc_record_hash'])

    def test_same_event_gets_same_id(self):
        stream = GuideEvents({})
        self.assertEqual(stream.transform(single_event())['_sdc_record_hash'],
                         stream.transform(single_event())['_sdc_record_hash'])
