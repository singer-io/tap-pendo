import unittest
from unittest import mock

from tap_pendo.discover import discover_streams, _apply_access_checks
from tap_pendo.streams import (
    STREAMS,
    PendoForbiddenError,
)


class TestDiscovery(unittest.TestCase):
    """Test cases for discovery with access control checks"""

    def setUp(self):
        self.config = {
            "x_pendo_integration_key": "test_key",
            "start_date": "2021-01-01T00:00:00Z",
            "period": "day",
            "app_ids": []
        }

    @mock.patch('tap_pendo.discover._apply_access_checks')
    def test_discover_error_when_no_accessible_streams(self, mock_apply_access):
        """Test that discovery fails gracefully when no streams accessible"""
        mock_apply_access.side_effect = PendoForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have 'read' access to any supported streams."
        )

        with self.assertRaises(PendoForbiddenError) as context:
            discover_streams(self.config)

        assert "do not have 'read' access to any supported streams" in str(context.exception)

    @mock.patch('tap_pendo.streams.EventsBase.check_access')
    @mock.patch('tap_pendo.streams.Stream.check_access')
    def test_apply_access_checks_no_accessible_streams_raises_error(self, mock_check_access, mock_events_check_access):
        """Test _apply_access_checks raises error when no streams accessible"""
        mock_check_access.return_value = False
        mock_events_check_access.return_value = False

        with self.assertRaises(PendoForbiddenError) as context:
            _apply_access_checks(self.config)

        error_msg = str(context.exception)
        assert "HTTP-error-code: 403" in error_msg
        assert "do not have 'read' access to any supported streams" in error_msg


class TestCheckAccess(unittest.TestCase):
    """Test cases for the check_access method"""

    def setUp(self):
        self.config = {
            "x_pendo_integration_key": "test_key",
            "start_date": "2021-01-01T00:00:00Z",
            "period": "dayRange",
            "app_ids": []
        }

    def test_check_access_child_streams_always_return_true(self):
        """Child streams return True without making any API call —
        their access is governed entirely by their parent stream."""
        child_instances = [
            STREAMS['visitor_history'](self.config),
            STREAMS['feature_events'](self.config),
            STREAMS['page_events'](self.config),
            STREAMS['guide_events'](self.config),
            STREAMS['track_events'](self.config),
        ]
        for child in child_instances:
            with self.subTest(stream=child.name):
                result = child.check_access()
                assert result is True

    @mock.patch('tap_pendo.streams.Stream.request')
    def test_check_access_parent_stream_returns_true_on_success(self, mock_request):
        """check_access returns True when the API accepts the request (200)."""
        mock_request.return_value = {"results": []}

        accounts_stream = STREAMS['accounts'](self.config)
        result = accounts_stream.check_access()

        assert result is True
        mock_request.assert_called_once()
        # Verify a valid get_body() payload was passed (not a fake probe body)
        call_kwargs = mock_request.call_args
        assert call_kwargs[0][0] == 'accounts'
        assert 'json' in call_kwargs[1]
        body = call_kwargs[1]['json']
        assert body is not None
        # The accounts get_body produces a proper aggregation pipeline
        assert 'request' in body
        assert 'pipeline' in body['request']

    @mock.patch('tap_pendo.streams.Stream.request')
    def test_check_access_parent_stream_returns_false_on_forbidden(self, mock_request):
        """check_access returns False when the API returns 403 Forbidden."""
        mock_request.side_effect = PendoForbiddenError("HTTP-error-code: 403, Error: Forbidden")

        accounts_stream = STREAMS['accounts'](self.config)
        result = accounts_stream.check_access()

        assert result is False

    @mock.patch('tap_pendo.streams.Stream.request')
    def test_check_access_propagates_unexpected_errors(self, mock_request):
        """check_access lets non-403 errors propagate — they indicate real problems."""
        mock_request.side_effect = Exception("Network error")

        accounts_stream = STREAMS['accounts'](self.config)
        with self.assertRaises(Exception):
            accounts_stream.check_access()

    # ------------------------------------------------------------------
    # EventsBase override tests (Events and PollEvents are parent streams
    # that extend EventsBase whose get_body() requires positional args)
    # ------------------------------------------------------------------

    @mock.patch('tap_pendo.streams.EventsBase.request')
    def test_check_access_events_stream_returns_true_on_success(self, mock_request):
        """Events.check_access() returns True when API accepts the request."""
        mock_request.return_value = {"results": []}

        events_stream = STREAMS['events'](self.config)
        result = events_stream.check_access()

        assert result is True
        mock_request.assert_called_once()

    @mock.patch('tap_pendo.streams.EventsBase.request')
    def test_check_access_events_stream_uses_valid_body(self, mock_request):
        """Events.check_access() builds a real aggregation body with period and
        a recent timestamp — not a fake/broken probe body."""
        mock_request.return_value = {"results": []}

        events_stream = STREAMS['events'](self.config)
        events_stream.check_access()

        call_kwargs = mock_request.call_args
        assert call_kwargs[0][0] == 'events'
        body = call_kwargs[1]['json']
        assert body is not None
        pipeline = body['request']['pipeline']
        # EventsBase builds a timeSeries pipeline with period and first
        source = pipeline[0]['source']
        assert 'timeSeries' in source
        ts = source['timeSeries']
        assert ts['period'] == self.config['period']
        assert isinstance(ts['first'], int) and ts['first'] > 0

    @mock.patch('tap_pendo.streams.EventsBase.request')
    def test_check_access_events_stream_returns_false_on_forbidden(self, mock_request):
        """Events.check_access() returns False on 403 Forbidden."""
        mock_request.side_effect = PendoForbiddenError("HTTP-error-code: 403, Error: Forbidden")

        events_stream = STREAMS['events'](self.config)
        result = events_stream.check_access()

        assert result is False

    @mock.patch('tap_pendo.streams.EventsBase.request')
    def test_check_access_poll_events_returns_true_on_success(self, mock_request):
        """PollEvents.check_access() also uses the EventsBase override."""
        mock_request.return_value = {"results": []}

        poll_stream = STREAMS['poll_events'](self.config)
        result = poll_stream.check_access()

        assert result is True
