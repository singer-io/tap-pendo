import copy
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import requests
from parameterized import parameterized

from tap_pendo.streams import DAY_MS, HOUR_MS, Events, FeatureEvents
from tap_pendo.sync import sync_stream

CONFIG = {'x_pendo_integration_key': 'test', 'period': 'hourRange', 'app_ids': 'expandAppIds("*")'}
START = datetime(2021, 1, 1, tzinfo=timezone.utc)
START_MS = int(START.timestamp() * 1000)
REASONS = {200: 'OK', 400: 'Bad Request', 408: 'Request Timeout', 503: 'Service Unavailable'}


def response(status, body):
    resp = requests.Response()
    resp.status_code = status
    resp.reason = REASONS[status]
    resp.url = 'https://app.pendo.io/api/v1/aggregation'
    resp._content = (body if isinstance(body, str) else json.dumps(body)).encode()
    return resp


def rows(*hours, key='hour'):
    """A 200 response with one row per hour offset from START"""
    return response(200, {'results': [{key: START_MS + h * HOUR_MS, 'visitorId': f'v{i}'}
                                      for i, h in enumerate(hours)]})


def window(body):
    series = body['request']['pipeline'][0]['source']['timeSeries']
    return series['first'], series['last']


class FakePendo:
    """Stands in for Session.send: records each request body and answers in order, repeating the last answer"""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.bodies = []

    def __call__(self, req, **kwargs):
        self.bodies.append(json.loads(req.body))
        answer = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(answer, Exception):
            raise answer
        return answer

    def windows(self):
        return [window(body) for body in self.bodies]

    def widths(self):
        return [last - first for first, last in self.windows()]


class Schema:
    def to_dict(self):
        return {'type': 'object', 'properties': {
            'hour': {'type': ['null', 'string'], 'format': 'date-time'},
            'visitor_id': {'type': ['null', 'string']}}}


class CatalogStream:
    tap_stream_id = 'events'
    schema = Schema()
    metadata = []


@mock.patch('singer.write_state')
@mock.patch('time.sleep')
@mock.patch('tap_pendo.streams.now')
@mock.patch('requests.Session.send')
class TestEventsWindows(unittest.TestCase):

    def events(self, mocked_now, now_offset, **config):
        mocked_now.return_value = START + now_offset
        return Events({**CONFIG, **config})

    def test_window_bodies(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state):
        """Each window asks for buckets that start in [first, last); the last window ends at now"""
        mocked_send.side_effect = pendo = FakePendo(rows())
        events = self.events(mocked_now, timedelta(days=2, hours=12))

        (_, records), more = events.sync({}, START)

        self.assertEqual((records, more), ([], False))
        self.assertEqual(pendo.windows(), [(START_MS, START_MS + DAY_MS),
                                           (START_MS + DAY_MS, START_MS + 2 * DAY_MS),
                                           (START_MS + 2 * DAY_MS, START_MS + 2 * DAY_MS + 12 * HOUR_MS)])
        body = pendo.bodies[0]['request']['pipeline']
        self.assertEqual(body[0]['source']['timeSeries']['period'], 'hourRange')
        self.assertEqual(body[0]['source']['events'], {'appId': 'expandAppIds("*")'})
        self.assertEqual(body[1:], [{'sort': ['hour']},
                                    {'filter': f'hour>={START_MS}&&hour<{START_MS + DAY_MS}'},
                                    {'limit': events.record_limit}])

    def test_empty_windows_advance_the_cursor(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state):
        """Empty windows advance the cursor in one call; a window with records returns at once"""
        mocked_send.side_effect = pendo = FakePendo(rows(), rows(), rows(50), rows())
        events = self.events(mocked_now, timedelta(days=4))

        (_, records), more = events.sync({}, START)
        self.assertEqual([r['hour'] for r in records], [START_MS + 50 * HOUR_MS])
        self.assertTrue(more)
        self.assertEqual(len(pendo.bodies), 3)

        (_, records), more = events.sync({}, START)
        self.assertEqual((records, more), ([], False))
        self.assertEqual(pendo.windows()[-1], (START_MS + 3 * DAY_MS, START_MS + 4 * DAY_MS))

    def test_bookmark_per_window(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state):
        """sync_stream writes the records and the bookmark of each window before the next window"""
        mocked_send.side_effect = FakePendo(rows(1, 5), rows(), rows(50))
        events = self.events(mocked_now, timedelta(days=3))
        events.stream = CatalogStream()
        bookmarks = []
        mocked_write_state.side_effect = lambda state: bookmarks.append(
            copy.deepcopy(state.get('bookmarks', {}).get('events', {}).get('hour')))

        with mock.patch('singer.write_record') as mocked_write_record:
            self.assertEqual(sync_stream({}, '2021-01-01T00:00:00Z', events), 3)

        self.assertEqual(mocked_write_record.call_count, 3)
        self.assertEqual(list(dict.fromkeys(b for b in bookmarks if b)),
                         ['2021-01-01T05:00:00.000000Z', '2021-01-03T02:00:00.000000Z'])

    def test_503_halves_the_window_down_to_one_hour_then_backs_off(self, mocked_send, mocked_now, mocked_sleep,
                                                                    mocked_write_state):
        mocked_send.side_effect = pendo = FakePendo(response(503, 'upstream timed out'))
        events = self.events(mocked_now, timedelta(days=1))

        with self.assertRaises(requests.exceptions.HTTPError):
            events.sync({}, START)

        hours = [width // HOUR_MS for width in pendo.widths()]
        self.assertEqual(hours, [24, 12, 6, 3] + [1] * 7)
        self.assertTrue(all(first == START_MS for first, _ in pendo.windows()))

    @parameterized.expand([
        ('408', response(408, 'Request Timeout')),
        ('400_memory_limit', response(400, '{"message": "Query exceeded memory limit"}')),
        ('read_timeout', requests.exceptions.ReadTimeout('Read timed out. (read timeout=300)')),
    ])
    def test_query_too_large_halves_the_window(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state,
                                               name, failure):
        mocked_send.side_effect = pendo = FakePendo(failure, rows(1), rows(13))
        events = self.events(mocked_now, timedelta(days=1))

        (_, first_half), more = events.sync({}, START)
        self.assertTrue(more)
        (_, second_half), more = events.sync({}, START)
        self.assertFalse(more)

        self.assertEqual([r['hour'] for r in first_half + second_half], [START_MS + HOUR_MS, START_MS + 13 * HOUR_MS])
        self.assertEqual(pendo.windows(), [(START_MS, START_MS + DAY_MS),
                                           (START_MS, START_MS + 12 * HOUR_MS),
                                           (START_MS + 12 * HOUR_MS, START_MS + DAY_MS)])

    def test_other_400_fails_without_halving(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state):
        mocked_send.side_effect = pendo = FakePendo(response(400, '{"message": "invalid pipeline"}'))
        events = self.events(mocked_now, timedelta(days=1))

        with self.assertRaises(requests.exceptions.HTTPError):
            events.sync({}, START)
        self.assertEqual(len(pendo.bodies), 1)

    def test_halving_resumes_at_the_pending_page(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state):
        """A failure after an earlier call returned part of the window resumes at the first record not returned"""
        mocked_send.side_effect = pendo = FakePendo(rows(1, 2), rows(2, 3), response(503, ''), rows(3), rows())
        events = self.events(mocked_now, timedelta(days=1), record_limit=2)

        returned = []
        more = True
        while more:
            (_, records), more = events.sync({}, START)
            returned += [r['hour'] for r in records]

        self.assertEqual(returned, [START_MS + h * HOUR_MS for h in (1, 2, 3)])
        self.assertEqual(pendo.windows()[2:], [(START_MS + 3 * HOUR_MS, START_MS + DAY_MS),
                                               (START_MS + 3 * HOUR_MS, START_MS + 15 * HOUR_MS),
                                               (START_MS + 15 * HOUR_MS, START_MS + DAY_MS)])

    def test_halving_mid_call_restarts_the_window(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state):
        """Records of earlier pages in the failed call were never returned, so the window starts over"""
        mocked_send.side_effect = pendo = FakePendo(rows(1, 2), response(503, ''), rows(1))
        events = self.events(mocked_now, timedelta(hours=12), record_limit=2)

        (_, records), _ = events.sync({}, START)

        self.assertEqual([r['hour'] for r in records], [START_MS + HOUR_MS])
        self.assertEqual(pendo.windows()[2], (START_MS, START_MS + 6 * HOUR_MS))

    def test_window_of_one_bucket_backs_off(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state):
        """A last window shorter than an hour cannot shrink, so the usual 7 tries apply"""
        mocked_send.side_effect = pendo = FakePendo(response(503, ''))
        events = self.events(mocked_now, timedelta(minutes=30))

        with self.assertRaises(requests.exceptions.HTTPError):
            events.sync({}, START)
        self.assertEqual(pendo.widths(), [30 * 60 * 1000] * 7)

    def test_day_range_halves_down_to_one_day(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state):
        mocked_send.side_effect = pendo = FakePendo(response(503, ''))
        events = self.events(mocked_now, timedelta(days=4), period='dayRange', events_date_window='4')

        with self.assertRaises(requests.exceptions.HTTPError):
            events.sync({}, START)

        self.assertEqual([width // DAY_MS for width in pendo.widths()], [4, 2] + [1] * 7)
        self.assertEqual(pendo.bodies[0]['request']['pipeline'][2],
                         {'filter': f'day>={START_MS}&&day<{START_MS + 4 * DAY_MS}'})

    def test_start_after_now_requests_nothing(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state):
        events = self.events(mocked_now, timedelta(0))

        self.assertEqual(events.sync({}, START + timedelta(hours=1))[1], False)
        mocked_send.assert_not_called()

    def test_lookback_moves_the_first_window(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state):
        mocked_send.side_effect = pendo = FakePendo(rows())
        events = self.events(mocked_now, timedelta(days=1), lookback_window='2')

        events.sync({}, START)

        self.assertEqual(pendo.windows()[0], (START_MS - 2 * DAY_MS, START_MS - DAY_MS))
        self.assertEqual(len(pendo.bodies), 3)


class TestEventsWindowSettings(unittest.TestCase):

    @parameterized.expand([
        ('default', None, DAY_MS),
        ('empty', '', DAY_MS),
        ('zero', 0, DAY_MS),
        ('string', '3', 3 * DAY_MS),
        ('integer', 7, 7 * DAY_MS),
    ])
    def test_window_size(self, name, value, expected):
        self.assertEqual(Events({**CONFIG, 'events_date_window': value}).window_size, expected)

    @parameterized.expand([('negative', -1), ('not_a_number', 'abc')])
    def test_invalid_window_size(self, name, value):
        with self.assertRaises(ValueError):
            Events({**CONFIG, 'events_date_window': value})

    def test_body_without_window_reads_until_now(self):
        """Discovery and child event streams keep the open-ended request"""
        for stream in (Events(CONFIG), FeatureEvents(CONFIG)):
            body = stream.get_body('key', 'hourRange', 1)
            self.assertEqual(body['request']['pipeline'][0]['source']['timeSeries']['last'], 'now()')
            self.assertEqual(body['request']['pipeline'][2], {'filter': 'hour>=1'})
