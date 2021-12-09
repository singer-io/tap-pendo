import unittest
from unittest import mock
from datetime import datetime, timezone
from tap_pendo.streams import Events

@mock.patch("tap_pendo.streams.now")
@mock.patch("tap_pendo.streams.Events.get_body")
@mock.patch("tap_pendo.streams.Events.request")
class EventsDateWindow(unittest.TestCase):

    @mock.patch("tap_pendo.streams.Events.update_bookmark")
    def test_dayRange(self, mocked_update_bookmark, mocked_request, mocked_get_body, mocked_singer_now):

        # expected records
        expected_records = [{'day': 1610150400000}, {'day': 1611100800000}, {'day': 1612483200000}]
        # mock request and return records
        mocked_request.side_effect = [
            [expected_records[0], expected_records[1]],
            [expected_records[2]]
        ]
        # mock now() and return the expected date to stop the loop
        mocked_singer_now.return_value = datetime(2021, 2, 15)

        # initialize 'Events' stream
        events = Events({'x_pendo_integration_key': 'test', 'period': 'dayRange'})

        # set window start date
        window_start_date = datetime(2021, 1, 1)
        # function call
        events_data = list(events.get_events(window_start_date, {}, datetime(2020, 1, 1, tzinfo=timezone.utc)))

        # create date for assertion of 'get_body' function
        get_body_calls = [
            mock.call('dayRange', 'date(2021, 1, 1)', 'date(2021, 1, 31)'),
            mock.call('dayRange', 'date(2021, 2, 1)', 'date(2021, 3, 3)')
        ]
        # create date for assertion of 'update_bookmark' function
        update_bookmark_calls = [
            mock.call({'currently_syncing': 'events'}, 'events', '2021-01-20T00:00:00.000000Z', 'day'),
            mock.call({'currently_syncing': 'events'}, 'events', '2021-02-05T00:00:00.000000Z', 'day'),
        ]

        # verify the 'get_body' was called with expected arguments
        self.assertEquals(mocked_get_body.mock_calls, get_body_calls)
        # verify the 'update_bookmark' was called with expected arguments
        self.assertEquals(mocked_update_bookmark.mock_calls, update_bookmark_calls)
        # verify the data we mocked was returned
        self.assertEquals(len(events_data), len(expected_records))
        self.assertEquals(events_data, expected_records)

    @mock.patch("tap_pendo.streams.Events.update_bookmark")
    def test_hourRange(self, mocked_update_bookmark, mocked_request, mocked_get_body, mocked_singer_now):

        # expected records
        expected_records = [{'hour': 1610168400000}, {'hour': 1611136800000}, {'hour': 1612540800000}]
        # mock request and return records
        mocked_request.side_effect = [
            [expected_records[0], expected_records[1]],
            [expected_records[2]]
        ]
        # mock now() and return the expected date to stop the loop
        mocked_singer_now.return_value = datetime(2021, 2, 15)

        # initialize 'Events' stream
        events = Events({'x_pendo_integration_key': 'test', 'period': 'hourRange'})

        # set window start date
        window_start_date = datetime(2021, 1, 1)
        # function call
        events_data = list(events.get_events(window_start_date, {}, datetime(2020, 1, 1, tzinfo=timezone.utc)))

        # create date for assertion of 'get_body' function
        get_body_calls = [
            mock.call('hourRange', 'date(2021, 1, 1)', 'date(2021, 1, 31)'),
            mock.call('hourRange', 'date(2021, 1, 31)', 'date(2021, 3, 2)')
        ]
        # create date for assertion of 'update_bookmark' function
        update_bookmark_calls = [
            mock.call({'currently_syncing': 'events'}, 'events', '2021-01-20T10:00:00.000000Z', 'hour'),
            mock.call({'currently_syncing': 'events'}, 'events', '2021-02-05T16:00:00.000000Z', 'hour'),
        ]

        # verify the 'get_body' was called with expected arguments
        self.assertEquals(mocked_get_body.mock_calls, get_body_calls)
        # verify the 'update_bookmark' was called with expected arguments
        self.assertEquals(mocked_update_bookmark.mock_calls, update_bookmark_calls)
        # verify the data we mocked was returned
        self.assertEquals(len(events_data), len(expected_records))
        self.assertEquals(events_data, expected_records)

    def test_custom_window_size_dayRange(self, mocked_request, mocked_get_body, mocked_singer_now):

        # mock request
        mocked_request.return_value = []
        # mock now() and return the expected date to stop the loop
        mocked_singer_now.return_value = datetime(2021, 3, 15)

        # initialize 'Events' stream with custom 'events_date_window' size as 20
        events = Events({'x_pendo_integration_key': 'test', 'period': 'dayRange', 'events_date_window': 20})

        # set window start date
        window_start_date = datetime(2021, 1, 1)
        # function call
        events_data = list(events.get_events(window_start_date, {}, datetime(2020, 1, 1, tzinfo=timezone.utc)))

        # create date for assertion of 'get_body' function
        get_body_calls = [
            mock.call('dayRange', 'date(2021, 1, 1)', 'date(2021, 1, 21)'),
            mock.call('dayRange', 'date(2021, 1, 22)', 'date(2021, 2, 11)'),
            mock.call('dayRange', 'date(2021, 2, 12)', 'date(2021, 3, 4)'),
            mock.call('dayRange', 'date(2021, 3, 5)', 'date(2021, 3, 25)')
        ]

        # verify the 'get_body' was called with expected arguments
        self.assertEquals(mocked_get_body.mock_calls, get_body_calls)

    def test_custom_window_size_hourRange(self, mocked_request, mocked_get_body, mocked_singer_now):

        # mock request
        mocked_request.return_value = []
        # mock now() and return the expected date to stop the loop
        mocked_singer_now.return_value = datetime(2021, 3, 15)

        # initialize 'Events' stream with custom 'events_date_window' size as 20
        events = Events({'x_pendo_integration_key': 'test', 'period': 'hourRange', 'events_date_window': 20})

        # set window start date
        window_start_date = datetime(2021, 1, 1)
        # function call
        events_data = list(events.get_events(window_start_date, {}, datetime(2020, 1, 1, tzinfo=timezone.utc)))

        # create date for assertion of 'get_body' function
        get_body_calls = [
            mock.call('hourRange', 'date(2021, 1, 1)', 'date(2021, 1, 21)'),
            mock.call('hourRange', 'date(2021, 1, 21)', 'date(2021, 2, 10)'),
            mock.call('hourRange', 'date(2021, 2, 10)', 'date(2021, 3, 2)'),
            mock.call('hourRange', 'date(2021, 3, 2)', 'date(2021, 3, 22)')
        ]

        # verify the 'get_body' was called with expected arguments
        self.assertEquals(mocked_get_body.mock_calls, get_body_calls)

    @mock.patch("tap_pendo.streams.Events.update_bookmark")
    def test_bookmark_value(self, mocked_update_bookmark, mocked_request, mocked_get_body, mocked_singer_now):

        # expected records
        expected_records = [{'day': 1610150400000}, {'day': 1611100800000}, {'day': 1612483200000}]
        # mock request and return records
        mocked_request.side_effect = [
            [expected_records[0]],
            [expected_records[1]],
            [expected_records[2]],
        ]
        # mock now() and return the expected date to stop the loop
        mocked_singer_now.return_value = datetime(2021, 2, 15)

        # initialize 'Events' stream with custom 'events_date_window' size as 15
        events = Events({'x_pendo_integration_key': 'test', 'period': 'dayRange', 'events_date_window': 15})

        # set window start date
        window_start_date = datetime(2021, 1, 1)
        # function call
        events_data = list(events.get_events(window_start_date, {}, datetime(2021, 1, 15, tzinfo=timezone.utc)))

        # create date for assertion of 'update_bookmark' function as the bookmark date is 2021-01-15
        # the 1st record's (2021-1-9) replication should not be written as bookmark and
        # next 2 record (2021-1-21, 2021-2-5) values should be updated as bookmark
        update_bookmark_calls = [
            mock.call({'currently_syncing': 'events'}, 'events', '2021-01-15T00:00:00.000000Z', 'day'),
            mock.call({'currently_syncing': 'events'}, 'events', '2021-01-20T00:00:00.000000Z', 'day'),
            mock.call({'currently_syncing': 'events'}, 'events', '2021-02-05T00:00:00.000000Z', 'day'),
        ]

        # verify the 'update_bookmark' was called with expected arguments
        self.assertEquals(mocked_update_bookmark.mock_calls, update_bookmark_calls)
