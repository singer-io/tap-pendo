import unittest
import requests
import socket
from unittest import mock
import tap_pendo.streams as streams
from requests.models import ProtocolError

class Mockresponse:
    def __init__(self, status_code, json, raise_error, headers=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.headers = headers
        self.reason = "test"
        self.raw = '{"results": [{"key1": "value1", "key2": "value2"}]}'

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return True

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        return self.text

    def close(self):
        return True

def get_response(json={}):
    return Mockresponse(200, json, False)

@mock.patch("time.sleep")
@mock.patch('requests.Session.send')
class TestTimeOut(unittest.TestCase):

    def test_timeout__accounts(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'accounts' stream class
        accounts = streams.Accounts({'x_pendo_integration_key': 'test'})

        try:
            accounts.request('accounts')
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__features(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'features' stream class
        features = streams.Features({'x_pendo_integration_key': 'test'})

        try:
            features.request('features')
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__guides(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'guides' stream class
        guides = streams.Guides({'x_pendo_integration_key': 'test'})

        try:
            guides.request('guides')
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__pages(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'pages' stream class
        pages = streams.Pages({'x_pendo_integration_key': 'test'})

        try:
            pages.request('pages')
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__feature_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'feature_events' stream class
        feature_events = streams.FeatureEvents({'x_pendo_integration_key': 'test'})

        try:
            feature_events.request('feature_events')
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__page_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'page_events' stream class
        page_events = streams.PageEvents({'x_pendo_integration_key': 'test'})

        try:
            page_events.request('page_events')
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__guide_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'guide_events' stream class
        guide_events = streams.GuideEvents({'x_pendo_integration_key': 'test'})

        try:
            guide_events.request('guide_events')
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__poll_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'poll_events' stream class
        poll_events = streams.PollEvents({'x_pendo_integration_key': 'test'})

        try:
            poll_events.request('poll_events')
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__track_types(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'track_types' stream class
        track_types = streams.TrackTypes({'x_pendo_integration_key': 'test'})

        try:
            track_types.request('track_types')
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__track_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'track_events' stream class
        track_events = streams.TrackEvents({'x_pendo_integration_key': 'test'})

        try:
            track_events.request('track_events')
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__metadata_accounts(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'metadata_accounts' stream class
        metadata_accounts = streams.MetadataAccounts({'x_pendo_integration_key': 'test'})

        try:
            metadata_accounts.request('metadata_accounts')
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__metadata_visitors(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'metadata_visitors' stream class
        metadata_visitors = streams.MetadataVisitors({'x_pendo_integration_key': 'test'})

        try:
            metadata_visitors.request('metadata_visitors')
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__visitor_history(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'visitor_history' stream class
        visitor_history = streams.VisitorHistory({'x_pendo_integration_key': 'test'})

        try:
            visitor_history.request('visitor_history', visitorId=1)
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__visitors(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'visitors' stream class
        visitors = streams.Visitors({'x_pendo_integration_key': 'test'})

        try:
            visitors.request('visitors')
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = requests.exceptions.Timeout

        # initialize 'events' stream class
        events = streams.Events({'x_pendo_integration_key': 'test'})

        try:
            events.request('events')
        except requests.exceptions.Timeout:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

@mock.patch("time.sleep")
@mock.patch('requests.Session.send')
class TestConnectionResetError(unittest.TestCase):

    def test_connection_reset_error__accounts(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'accounts' stream class
        accounts = streams.Accounts({'x_pendo_integration_key': 'test'})

        try:
            accounts.request('accounts')
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__features(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'features' stream class
        features = streams.Features({'x_pendo_integration_key': 'test'})

        try:
            features.request('features')
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__guides(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'guides' stream class
        guides = streams.Guides({'x_pendo_integration_key': 'test'})

        try:
            guides.request('guides')
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__pages(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'pages' stream class
        pages = streams.Pages({'x_pendo_integration_key': 'test'})

        try:
            pages.request('pages')
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__feature_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'feature_events' stream class
        feature_events = streams.FeatureEvents({'x_pendo_integration_key': 'test'})

        try:
            feature_events.request('feature_events')
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__page_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'page_events' stream class
        page_events = streams.PageEvents({'x_pendo_integration_key': 'test'})

        try:
            page_events.request('page_events')
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__guide_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'guide_events' stream class
        guide_events = streams.GuideEvents({'x_pendo_integration_key': 'test'})

        try:
            guide_events.request('guide_events')
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__poll_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'poll_events' stream class
        poll_events = streams.PollEvents({'x_pendo_integration_key': 'test'})

        try:
            poll_events.request('poll_events')
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__track_types(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'track_types' stream class
        track_types = streams.TrackTypes({'x_pendo_integration_key': 'test'})

        try:
            track_types.request('track_types')
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__track_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'track_events' stream class
        track_events = streams.TrackEvents({'x_pendo_integration_key': 'test'})

        try:
            track_events.request('track_events')
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__metadata_accounts(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'metadata_accounts' stream class
        metadata_accounts = streams.MetadataAccounts({'x_pendo_integration_key': 'test'})

        try:
            metadata_accounts.request('metadata_accounts')
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__metadata_visitors(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'metadata_visitors' stream class
        metadata_visitors = streams.MetadataVisitors({'x_pendo_integration_key': 'test'})

        try:
            metadata_visitors.request('metadata_visitors')
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__visitor_history(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'visitor_history' stream class
        visitor_history = streams.VisitorHistory({'x_pendo_integration_key': 'test'})

        try:
            visitor_history.request('visitor_history', visitorId=1)
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__visitors(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'visitors' stream class
        visitors = streams.Visitors({'x_pendo_integration_key': 'test'})

        try:
            visitors.request('visitors')
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        # initialize 'events' stream class
        events = streams.Events({'x_pendo_integration_key': 'test'})

        try:
            events.request('events')
        except ConnectionResetError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

@mock.patch("time.sleep")
@mock.patch('requests.Session.send')
class TestProtocolError(unittest.TestCase):

    def test_protocol_error__accounts(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'accounts' stream class
        accounts = streams.Accounts({'x_pendo_integration_key': 'test'})

        try:
            accounts.request('accounts')
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__features(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'features' stream class
        features = streams.Features({'x_pendo_integration_key': 'test'})

        try:
            features.request('features')
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__guides(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'guides' stream class
        guides = streams.Guides({'x_pendo_integration_key': 'test'})

        try:
            guides.request('guides')
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__pages(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'pages' stream class
        pages = streams.Pages({'x_pendo_integration_key': 'test'})

        try:
            pages.request('pages')
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__feature_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'feature_events' stream class
        feature_events = streams.FeatureEvents({'x_pendo_integration_key': 'test'})

        try:
            feature_events.request('feature_events')
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__page_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'page_events' stream class
        page_events = streams.PageEvents({'x_pendo_integration_key': 'test'})

        try:
            page_events.request('page_events')
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__guide_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'guide_events' stream class
        guide_events = streams.GuideEvents({'x_pendo_integration_key': 'test'})

        try:
            guide_events.request('guide_events')
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__poll_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'poll_events' stream class
        poll_events = streams.PollEvents({'x_pendo_integration_key': 'test'})

        try:
            poll_events.request('poll_events')
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__track_types(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'track_types' stream class
        track_types = streams.TrackTypes({'x_pendo_integration_key': 'test'})

        try:
            track_types.request('track_types')
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__track_events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'track_events' stream class
        track_events = streams.TrackEvents({'x_pendo_integration_key': 'test'})

        try:
            track_events.request('track_events')
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__metadata_accounts(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'metadata_accounts' stream class
        metadata_accounts = streams.MetadataAccounts({'x_pendo_integration_key': 'test'})

        try:
            metadata_accounts.request('metadata_accounts')
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__metadata_visitors(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'metadata_visitors' stream class
        metadata_visitors = streams.MetadataVisitors({'x_pendo_integration_key': 'test'})

        try:
            metadata_visitors.request('metadata_visitors')
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__visitor_history(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'visitor_history' stream class
        visitor_history = streams.VisitorHistory({'x_pendo_integration_key': 'test'})

        try:
            visitor_history.request('visitor_history', visitorId=1)
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__visitors(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'visitors' stream class
        visitors = streams.Visitors({'x_pendo_integration_key': 'test'})

        try:
            visitors.request('visitors')
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__events(self, mocked_send, mocked_sleep):
        # mock request and raise error
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        # initialize 'events' stream class
        events = streams.Events({'x_pendo_integration_key': 'test'})

        try:
            events.request('events')
        except ProtocolError:
            pass

        # verify if the request was called 5 times
        self.assertEquals(mocked_send.call_count, 5)

@mock.patch('requests.Session.send')
class Positive(unittest.TestCase):

    def test_positive__accounts(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'accounts' stream class
        accounts = streams.Accounts({'x_pendo_integration_key': 'test'})

        resp = accounts.request('accounts')
        
        # verify if the desired data was returned from the request
        self.assertEquals(resp, json)

    def test_positive__features(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'features' stream class
        features = streams.Features({'x_pendo_integration_key': 'test'})

        resp = features.request('features')
        
        # verify if the desired data was returned from the request
        self.assertEquals(resp, json)

    def test_positive__guides(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'guides' stream class
        guides = streams.Guides({'x_pendo_integration_key': 'test'})

        resp = guides.request('guides')
        
        # verify if the desired data was returned from the request
        self.assertEquals(resp, json)

    def test_positive__pages(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'pages' stream class
        pages = streams.Pages({'x_pendo_integration_key': 'test'})

        resp = pages.request('pages')
        
        # verify if the desired data was returned from the request
        self.assertEquals(resp, json)

    def test_positive__feature_events(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'feature_events' stream class
        feature_events = streams.FeatureEvents({'x_pendo_integration_key': 'test'})

        resp = feature_events.request('feature_events')
        
        # verify if the desired data was returned from the request
        self.assertEquals(resp, json)

    def test_positive__page_events(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'page_events' stream class
        page_events = streams.PageEvents({'x_pendo_integration_key': 'test'})

        resp = page_events.request('page_events')
        
        # verify if the desired data was returned from the request
        self.assertEquals(resp, json)

    def test_positive__guide_events(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'guide_events' stream class
        guide_events = streams.GuideEvents({'x_pendo_integration_key': 'test'})

        resp = guide_events.request('guide_events')
        
        # verify if the desired data was returned from the request
        self.assertEquals(resp, json)

    def test_positive__poll_events(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'poll_events' stream class
        poll_events = streams.PollEvents({'x_pendo_integration_key': 'test'})

        resp = poll_events.request('poll_events')
        
        # verify if the desired data was returned from the request
        self.assertEquals(resp, json)

    def test_positive__track_types(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'track_types' stream class
        track_types = streams.TrackTypes({'x_pendo_integration_key': 'test'})

        resp = track_types.request('track_types')
        
        # verify if the desired data was returned from the request
        self.assertEquals(resp, json)

    def test_positive__track_events(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'track_events' stream class
        track_events = streams.TrackEvents({'x_pendo_integration_key': 'test'})

        resp = track_events.request('track_events')
        
        # verify if the desired data was returned from the request
        self.assertEquals(resp, json)

    def test_positive__metadata_accounts(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'metadata_accounts' stream class
        metadata_accounts = streams.MetadataAccounts({'x_pendo_integration_key': 'test'})

        resp = metadata_accounts.request('metadata_accounts')
        
        # verify if the desired data was returned from the request
        self.assertEquals(resp, json)

    def test_positive__metadata_visitors(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'metadata_visitors' stream class
        metadata_visitors = streams.MetadataVisitors({'x_pendo_integration_key': 'test'})

        resp = metadata_visitors.request('metadata_visitors')
        
        # verify if the desired data was returned from the request
        self.assertEquals(resp, json)

    def test_positive__visitor_history(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'visitor_history' stream class
        visitor_history = streams.VisitorHistory({'x_pendo_integration_key': 'test'})

        resp = visitor_history.request('visitor_history', visitorId=1)
        
        # verify if the desired data was returned from the request
        self.assertEquals(resp, json)

    def test_positive__visitors(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'visitors' stream class
        visitors = streams.Visitors({'x_pendo_integration_key': 'test'})

        resp = visitors.request('visitors')
        
        # verify if the desired data was returned from the request
        self.assertEquals(list(resp), [json])

    def test_positive__events(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        # mock request and return dummy data
        mocked_send.return_value = get_response(json)

        # initialize 'events' stream class
        events = streams.Events({'x_pendo_integration_key': 'test'})

        resp = events.request('events')
        
        # verify if the desired data was returned from the request
        self.assertEquals(list(resp), [json])
