import collections
import json
import math
import random
import re
import unittest
from datetime import UTC, datetime, timedelta
from unittest import mock

import humps
import requests
from parameterized import parameterized

from tap_pendo import streams
from tap_pendo.sync import sync_stream

HOUR_MS = streams.HOUR_MS
DAY_MS = streams.DAY_MS
START = datetime(2021, 1, 1, tzinfo=UTC)
START_MS = int(START.timestamp() * 1000)
CONFIG = {'x_pendo_integration_key': 'test', 'app_ids': 'expandAppIds("*")', 'start_date': '2021-01-01T00:00:00Z',
          'period': 'hourRange', 'record_limit': 3}
TOKEN = re.compile(r'"(?:[^"\\]|\\.)*"|\d+|\w+|>=|==|&&|\|\||[<>()]')
# (bucket, visitorId, rows): with pages of 3 rows, buckets need pages that end inside them, one visitor
# outgrows a page, and visitor IDs need escaping or sort by code point
GROUPS = [(0, 'a', 1),
          (1, 'A', 2), (1, 'B', 1), (1, 'a', 3), (1, 'a"b', 2), (1, '\u00e9', 1), (1, '\U0001F600', 2),
          (2, 'A', 1), (2, 'a\\b', 4), (2, 'ab', 1),
          (6, 'a b', 2), (6, '_PENDO_T_x', 3), (6, '\U0001F600', 1),
          (30, 'B', 2), (30, 'a', 1),
          (50, 'a', 3)]
MISSING = object()  # a row without visitorId


def dataset(key, size, prefix, groups=GROUPS, **fields):
    """Rows as Pendo stores them, in no particular order; the second row of a group repeats the first"""
    rows, number = [], 0
    for bucket, visitor, count in groups:
        for i in range(count):
            if i != 1:
                number += 1
            rows.append({key: START_MS + bucket * size, 'visitorId': visitor, 'row': f'{prefix}{number}', **fields})
    random.Random(0).shuffle(rows)
    return rows


def response(status, body):
    resp = requests.Response()
    resp.status_code = status
    resp.reason = {200: 'OK', 400: 'Bad Request', 503: 'Service Unavailable'}[status]
    resp.url = 'https://app.pendo.io/api/v1/aggregation'
    resp._content = json.dumps(body).encode()
    return resp


class Null:
    """A missing or null field, which Pendo compares as false"""

    def __ge__(self, other):
        return False

    __gt__ = __le__ = __lt__ = __ge__


def passes(expression, row):
    """Evaluates the filters the tap writes: comparisons joined by && and || with parentheses"""
    tokens = TOKEN.findall(expression)
    assert ''.join(tokens) == expression, expression
    python = ' '.join({'&&': 'and', '||': 'or'}.get(token, repr(json.loads(token)) if token[0] == '"' else token)
                      for token in tokens)
    names = collections.defaultdict(Null, {name: Null() if value is None else value for name, value in row.items()})
    return eval(python, {'__builtins__': {}}, names)  # pylint: disable=eval-used


class FakePendo:
    """Stands in for Session.send and answers like Pendo: the rows of the timeSeries buckets and the page,
    then sort, filter and limit. Like Pendo, it answers 400 to an escaped surrogate pair in a filter"""

    def __init__(self, rows, fail=None, status=503):
        self.rows = rows
        self.fail = fail
        self.status = status
        self.bodies = []

    def __call__(self, req, **kwargs):
        body = json.loads(req.body)
        self.bodies.append(body)
        stages = {name: value for stage in body['request']['pipeline'] for name, value in stage.items()}
        if self.fail and self.fail(stages['filter']):
            self.fail = None
            return response(self.status, {})
        if re.search(r'\\ud[89a-f]', stages['filter'], re.IGNORECASE):
            return response(400, {'message': 'invalid filter'})
        series, sort = stages['source']['timeSeries'], stages['sort']
        bucket = DAY_MS if series['period'] == 'dayRange' else HOUR_MS
        last = math.inf if series['last'] == 'now()' else series['last'] // bucket * bucket + bucket
        page_id = stages['source'].get('pageEvents', {}).get('pageId')
        rows = [row for row in self.rows
                if series['first'] // bucket * bucket <= row[sort[0]] < last and row.get('pageId') == page_id]
        # Pendo sorts missing and null values first, in no fixed order between them
        rows = sorted(rows, key=lambda row: [(row.get(k) is not None, row.get(k)) for k in sort])
        rows = [row for row in rows if passes(stages['filter'], row)]
        return response(200, {'results': rows[:stages['limit']]})

    def filters(self):
        return [stage['filter'] for body in self.bodies for stage in body['request']['pipeline'] if 'filter' in stage]

    def limits(self):
        return [stage['limit'] for body in self.bodies for stage in body['request']['pipeline'] if 'limit' in stage]


class Schema:
    def __init__(self, key):
        self.key = key

    def to_dict(self):
        return {'type': 'object', 'properties': {self.key: {'type': ['null', 'string'], 'format': 'date-time'},
                                                 'visitor_id': {'type': ['null', 'string']},
                                                 'row': {'type': ['null', 'string']}}}


class CatalogStream:
    def __init__(self, tap_stream_id, key):
        self.tap_stream_id = tap_stream_id
        self.schema = Schema(key)
        self.metadata = []


def row_counts(rows):
    return collections.Counter(row['row'] for row in rows)


@mock.patch('singer.write_state')
@mock.patch('time.sleep')
@mock.patch('tap_pendo.streams.API_RECORD_LIMIT', 5)
class TestPagingInsideAValue(unittest.TestCase):
    """Pages of event streams end inside a replication key value and resume at the held-back visitor"""

    def sync_events(self, pendo, period, until):
        key = 'day' if period == 'dayRange' else 'hour'
        stream = streams.Events({**CONFIG, 'period': period})
        stream.stream = CatalogStream('events', key)
        with mock.patch('tap_pendo.streams.now', return_value=START + until), \
                mock.patch('requests.Session.send', side_effect=pendo), \
                mock.patch('singer.write_record') as write_record:
            sync_stream({}, CONFIG['start_date'], stream)
        return collections.Counter(call.args[1]['row'] for call in write_record.call_args_list)

    @parameterized.expand([('hourRange', 'hour', HOUR_MS, timedelta(days=3)),
                           ('dayRange', 'day', DAY_MS, timedelta(days=60))])
    def test_events_emit_every_row_once(self, mocked_sleep, mocked_write_state, period, key, size, until):
        rows = dataset(key, size, 'e')
        pendo = FakePendo(rows)

        self.assertEqual(self.sync_events(pendo, period, until), row_counts(rows))
        self.assertTrue(any('visitorId>=' in flt for flt in pendo.filters()))
        self.assertIn(5, pendo.limits())

    def test_child_stream_emits_every_row_once(self, mocked_sleep, mocked_write_state):
        rows = dataset('hour', HOUR_MS, 'p1-', pageId='p1') + dataset('hour', HOUR_MS, 'p2-', pageId='p2')
        pendo = FakePendo(rows)
        parent = streams.Pages(CONFIG)
        child = streams.PageEvents(CONFIG)
        child.stream = CatalogStream('page_events', 'hour')

        with mock.patch('requests.Session.send', side_effect=pendo), mock.patch('singer.write_schema'), \
                mock.patch('singer.write_record') as write_record:
            parent.sync_substream({}, parent, child, [{'id': 'p1', 'last_updated_at': 1},
                                                      {'id': 'p2', 'last_updated_at': 2}])

        self.assertEqual(collections.Counter(call.args[1]['row'] for call in write_record.call_args_list),
                         row_counts(rows))
        self.assertTrue(any('visitorId>=' in flt for flt in pendo.filters()))

    def test_failed_parent_does_not_filter_the_next_parent(self, mocked_sleep, mocked_write_state):
        """p1 holds back visitor z of hour 1 and its next request fails; p2 still starts at its bookmark"""
        hour_1 = START_MS + HOUR_MS
        rows = [{'hour': hour, 'visitorId': visitor, 'pageId': page, 'row': f'{page}-{hour}-{visitor}'}
                for page, hour, visitor in [('p1', START_MS, 'a'), ('p1', hour_1, 'a'), ('p1', hour_1, 'z'),
                                            ('p2', START_MS, 'b'), ('p2', hour_1, 'a'), ('p2', hour_1 + HOUR_MS, 'a')]]
        pendo = FakePendo(rows, fail=lambda flt: 'visitorId>="z"' in flt, status=400)
        parent = streams.Pages(CONFIG)
        child = streams.PageEvents(CONFIG)
        child.stream = CatalogStream('page_events', 'hour')

        with mock.patch('requests.Session.send', side_effect=pendo), mock.patch('singer.write_schema'), \
                mock.patch('singer.write_record') as write_record:
            parent.sync_substream({}, parent, child, [{'id': 'p1', 'last_updated_at': 1},
                                                      {'id': 'p2', 'last_updated_at': 2}])

        self.assertEqual([call.args[1]['row'] for call in write_record.call_args_list],
                         [row['row'] for row in rows if row['row'] != f'p1-{hour_1}-z'])

    @parameterized.expand([(f'{stream_class.__name__}_{period}', stream_class, period)
                           for stream_class in (streams.Events, streams.PollEvents, streams.FeatureEvents,
                                                streams.PageEvents, streams.GuideEvents, streams.TrackEvents)
                           for period in ('hourRange', 'dayRange')])
    def test_rows_without_visitor_after_a_cut(self, mocked_sleep, mocked_write_state, _name, stream_class, period):
        """Pendo sorts missing and null visitor IDs first and compares them as false, so they never follow a cut
        inside their value, and the next value returns them whatever visitor the cut held back"""
        stream = stream_class({**CONFIG, 'period': period})
        key = humps.camelize(stream.replication_key)
        size = DAY_MS if key == 'day' else HOUR_MS
        cells = [(0, 'a'), (0, 'b'), (0, 'z'), (1, None), (1, MISSING), (1, ''), (1, 'a'), (1, 'b'),
                 (2, None), (2, MISSING), (2, ''), (2, 'a')]
        page = {'pageId': 'p'} if stream_class is streams.PageEvents else {}
        rows = [{key: START_MS + bucket * size, 'row': str(number), **page,
                 **({} if visitor is MISSING else {'visitorId': visitor})}
                for number, (bucket, visitor) in enumerate(cells)]
        pendo = FakePendo(rows)

        emitted = collections.Counter()
        with mock.patch('tap_pendo.streams.now', return_value=START + timedelta(days=4)), \
                mock.patch('requests.Session.send', side_effect=pendo):
            for _call in range(20):
                (_, records), more = stream.sync({}, START, 'p')
                emitted.update(record['row'] for record in records)
                if not more:
                    break

        self.assertEqual(emitted, row_counts(rows))
        self.assertTrue(any('visitorId>="z"' in flt for flt in pendo.filters()))
        self.assertTrue(any('visitorId>=""' in flt for flt in pendo.filters()))

    def test_body_after_a_cut_inside_an_hour(self, mocked_sleep, mocked_write_state):
        groups = [(1, 'A', 1), (1, 'a"b', 2), (1, '\U0001F600', 2), (2, 'B', 1)]
        pendo = FakePendo(dataset('hour', HOUR_MS, 'e', groups))
        end = START_MS + DAY_MS

        self.sync_events(pendo, 'hourRange', timedelta(days=1))

        hour_1, hour_2 = START_MS + HOUR_MS, START_MS + 2 * HOUR_MS
        self.assertEqual(pendo.filters(), [f'hour>={START_MS}&&hour<{end}',
                                           f'(hour>{hour_1}||(hour=={hour_1}&&visitorId>="a\\"b"))&&hour<{end}',
                                           f'(hour>{hour_1}||(hour=={hour_1}&&visitorId>="\U0001F600"))&&hour<{end}',
                                           f'(hour>{hour_2}||(hour=={hour_2}&&visitorId>="B"))&&hour<{end}'])
        self.assertEqual([body['request']['pipeline'][1] for body in pendo.bodies],
                         [{'sort': ['hour', 'visitorId']}] * 4)

    def test_halved_window_resumes_at_the_held_back_visitor(self, mocked_sleep, mocked_write_state):
        """A 503 on the page that starts at a"b, after A, B and a of the same hour, repeats none of them"""
        rows = dataset('hour', HOUR_MS, 'e')
        pendo = FakePendo(rows, fail=lambda flt: 'visitorId>="a\\"b"' in flt)

        self.assertEqual(self.sync_events(pendo, 'hourRange', timedelta(days=3)), row_counts(rows))

        hour_1 = START_MS + HOUR_MS
        cursor = f'(hour>{hour_1}||(hour=={hour_1}&&visitorId>="a\\"b"))'
        self.assertEqual([flt for flt in pendo.filters() if flt.startswith(cursor)],
                         [f'{cursor}&&hour<{START_MS + DAY_MS}', f'{cursor}&&hour<{hour_1 + 12 * HOUR_MS}'])


@mock.patch('singer.write_state')
@mock.patch('tap_pendo.streams.Stream.request')
class TestRowsWithoutVisitor(unittest.TestCase):

    def test_page_holds_back_the_whole_value(self, mocked_request, mocked_write_state):
        """Without a visitor ID to resume from, a full page holds back all rows of its last value"""
        mocked_request.side_effect = [{'results': [{'hour': 1000, 'visitor_id': 'a'}, {'hour': 2000, 'visitor_id': 'a'},
                                                   {'hour': 2000}]},
                                      {'results': []}]
        stream = streams.FeatureEvents(CONFIG)

        (_, records), more = stream.sync({}, START, 'FEATURE-ID')
        self.assertEqual((records, more), ([{'hour': 1000, 'visitor_id': 'a'}], True))
        self.assertEqual(len(stream.last_processed), 2)

        stream.sync({}, START, 'FEATURE-ID')
        self.assertEqual(mocked_request.call_args_list[1].kwargs['json']['request']['pipeline'][2],
                         {'filter': 'hour>=2000'})
