import copy
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import requests
from parameterized import parameterized
from urllib3.exceptions import ReadTimeoutError

from tap_pendo.streams import DAY_MS, HOUR_MS, SPARSE_WINDOW_ROWS, Events, FeatureEvents
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


def visitors(count, hour=0):
    """A 200 response with `count` rows in one hour"""
    return response(200, {'results': [{'hour': START_MS + hour * HOUR_MS, 'visitorId': f'v{i}'}
                                      for i in range(count)]})


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
                                           (START_MS + DAY_MS, START_MS + 2 * DAY_MS + 12 * HOUR_MS)])
        body = pendo.bodies[0]['request']['pipeline']
        self.assertEqual(body[0]['source']['timeSeries']['period'], 'hourRange')
        self.assertEqual(body[0]['source']['events'], {'appId': 'expandAppIds("*")'})
        self.assertEqual(body[1:], [{'sort': ['hour']},
                                    {'filter': f'hour>={START_MS}&&hour<{START_MS + DAY_MS}'},
                                    {'limit': events.record_limit}])

    def test_empty_windows_advance_the_cursor(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state):
        """Empty windows advance the cursor in one call; a window with records returns at once"""
        mocked_send.side_effect = pendo = FakePendo(rows(), rows(), rows(80), rows())
        events = self.events(mocked_now, timedelta(days=10))

        (_, records), more = events.sync({}, START)
        self.assertEqual([r['hour'] for r in records], [START_MS + 80 * HOUR_MS])
        self.assertTrue(more)
        self.assertEqual(len(pendo.bodies), 3)

        (_, records), more = events.sync({}, START)
        self.assertEqual((records, more), ([], False))
        self.assertEqual(pendo.windows()[-1], (START_MS + 7 * DAY_MS, START_MS + 10 * DAY_MS))

    def test_sparse_windows_double_up_to_32_days(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state):
        mocked_send.side_effect = pendo = FakePendo(rows())
        events = self.events(mocked_now, timedelta(days=100))

        events.sync({}, START)

        self.assertEqual([width // DAY_MS for width in pendo.widths()], [1, 2, 4, 8, 16, 32, 32, 5])

    @parameterized.expand([
        ('sparse', SPARSE_WINDOW_ROWS - 1, [1, 2, 4, 8, 5]),
        ('dense', SPARSE_WINDOW_ROWS, [1, 2, 4, 1, 2, 4, 6]),
    ])
    def test_window_after_records(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state, name, count,
                                  expected_days):
        """A window with fewer rows than SPARSE_WINDOW_ROWS doubles the next one; a denser one resets it"""
        mocked_send.side_effect = pendo = FakePendo(rows(), rows(), visitors(count, hour=80), rows())
        events = self.events(mocked_now, timedelta(days=20))

        more = True
        while more:
            _, more = events.sync({}, START)

        self.assertEqual([width // DAY_MS for width in pendo.widths()], expected_days)

    def test_dense_window_returns_to_the_configured_size(self, mocked_send, mocked_now, mocked_sleep,
                                                         mocked_write_state):
        mocked_send.side_effect = pendo = FakePendo(rows(), visitors(SPARSE_WINDOW_ROWS, hour=50), rows())
        events = self.events(mocked_now, timedelta(days=14), events_date_window='2')

        more = True
        while more:
            _, more = events.sync({}, START)

        self.assertEqual([width // DAY_MS for width in pendo.widths()], [2, 4, 2, 4, 2])

    def test_sparse_windows_grow_again_after_a_halving(self, mocked_send, mocked_now, mocked_sleep,
                                                        mocked_write_state):
        """One failure does not keep a long sparse history in small windows"""
        mocked_send.side_effect = pendo = FakePendo(rows(), rows(), response(503, ''), rows())
        events = self.events(mocked_now, timedelta(days=12))

        events.sync({}, START)

        self.assertEqual([width // DAY_MS for width in pendo.widths()], [1, 2, 4, 2, 4, 3])
        self.assertEqual(pendo.windows()[3], (START_MS + 3 * DAY_MS, START_MS + 5 * DAY_MS))

    def test_dense_windows_stay_at_or_below_a_halved_window(self, mocked_send, mocked_now, mocked_sleep,
                                                             mocked_write_state):
        """After a failure, dense data keeps the halved size instead of failing again on every window"""
        mocked_send.side_effect = pendo = FakePendo(response(503, ''), visitors(SPARSE_WINDOW_ROWS))
        events = self.events(mocked_now, timedelta(days=2))

        more = True
        while more:
            _, more = events.sync({}, START)

        self.assertEqual([width // HOUR_MS for width in pendo.widths()], [24, 12, 12, 12, 12])

    def test_window_above_the_growth_limit_keeps_its_size(self, mocked_send, mocked_now, mocked_sleep,
                                                          mocked_write_state):
        mocked_send.side_effect = pendo = FakePendo(rows())
        events = self.events(mocked_now, timedelta(days=100), events_date_window='40')

        events.sync({}, START)

        self.assertEqual([width // DAY_MS for width in pendo.widths()], [40, 40, 20])

    def test_bookmark_per_window(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state):
        """sync_stream writes the records and the bookmark of each window before the next window"""
        mocked_send.side_effect = FakePendo(rows(1, 5), rows(), rows(100))
        events = self.events(mocked_now, timedelta(days=7))
        events.stream = CatalogStream()
        bookmarks = []
        mocked_write_state.side_effect = lambda state: bookmarks.append(
            copy.deepcopy(state.get('bookmarks', {}).get('events', {}).get('hour')))

        with mock.patch('singer.write_record') as mocked_write_record:
            self.assertEqual(sync_stream({}, '2021-01-01T00:00:00Z', events), 3)

        self.assertEqual(mocked_write_record.call_count, 3)
        self.assertEqual(list(dict.fromkeys(b for b in bookmarks if b)),
                         ['2021-01-01T05:00:00.000000Z', '2021-01-05T04:00:00.000000Z'])

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
        ('400_memory_limit_capitalized', response(400, '{"message": "Query Exceeded Memory Limit"}')),
        ('read_timeout', requests.exceptions.ReadTimeout('Read timed out. (read timeout=300)')),
        ('body_read_timeout', requests.exceptions.ConnectionError(
            ReadTimeoutError(None, None, 'Read timed out. (read timeout=300)'))),
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

    def test_connection_error_backs_off_without_halving(self, mocked_send, mocked_now, mocked_sleep,
                                                        mocked_write_state):
        """Only a read timeout inside a ConnectionError says the query is too large"""
        mocked_send.side_effect = pendo = FakePendo(requests.exceptions.ConnectionError('Connection aborted.'))
        events = self.events(mocked_now, timedelta(days=1))

        with self.assertRaises(requests.exceptions.ConnectionError):
            events.sync({}, START)
        self.assertEqual(pendo.widths(), [DAY_MS] * 7)

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

    def test_each_full_page_returns_at_once(self, mocked_send, mocked_now, mocked_sleep, mocked_write_state):
        """A window of several pages comes back one page per call; each call resumes at the held-back hour"""
        mocked_send.side_effect = pendo = FakePendo(rows(1, 2), rows(2, 3), rows(3))
        events = self.events(mocked_now, timedelta(days=1), record_limit=2)

        calls = [events.sync({}, START) for _ in range(3)]

        self.assertEqual([([r['hour'] for r in records], more) for (_, records), more in calls],
                         [([START_MS + HOUR_MS], True), ([START_MS + 2 * HOUR_MS], True),
                          ([START_MS + 3 * HOUR_MS], False)])
        self.assertEqual(pendo.windows(), [(START_MS + h * HOUR_MS, START_MS + DAY_MS) for h in (0, 2, 3)])

    def test_failure_after_a_one_value_page_restarts_the_window(self, mocked_send, mocked_now, mocked_sleep,
                                                                 mocked_write_state):
        """A page whose rows share one hour adds no records, so a failure on the bigger retry loses none"""
        mocked_send.side_effect = pendo = FakePendo(rows(1, 1), response(503, ''), rows(1, 1))
        events = self.events(mocked_now, timedelta(hours=12), record_limit=2)

        (_, records), _ = events.sync({}, START)

        self.assertEqual([r['hour'] for r in records], [START_MS + HOUR_MS] * 2)
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

        self.assertEqual(pendo.windows(), [(START_MS - 2 * DAY_MS, START_MS - DAY_MS),
                                           (START_MS - DAY_MS, START_MS + DAY_MS)])


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
