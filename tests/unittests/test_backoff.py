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

def get_response(json={}):
    return Mockresponse(200, json, False)

@mock.patch("time.sleep")
@mock.patch('requests.Session.send')
class TestTimeOut(unittest.TestCase):

    def test_timeout__accounts(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        accounts = streams.Accounts({'x_pendo_integration_key': 'test'})

        try:
            accounts.request('accounts')
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__features(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        features = streams.Features({'x_pendo_integration_key': 'test'})

        try:
            features.request('features')
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__guides(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        guides = streams.Guides({'x_pendo_integration_key': 'test'})

        try:
            guides.request('guides')
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__pages(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        pages = streams.Pages({'x_pendo_integration_key': 'test'})

        try:
            pages.request('pages')
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__feature_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        feature_events = streams.FeatureEvents({'x_pendo_integration_key': 'test'})

        try:
            feature_events.request('feature_events')
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__page_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        page_events = streams.PageEvents({'x_pendo_integration_key': 'test'})

        try:
            page_events.request('page_events')
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__guide_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        guide_events = streams.GuideEvents({'x_pendo_integration_key': 'test'})

        try:
            guide_events.request('guide_events')
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__poll_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        poll_events = streams.PollEvents({'x_pendo_integration_key': 'test'})

        try:
            poll_events.request('poll_events')
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__track_types(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        track_types = streams.TrackTypes({'x_pendo_integration_key': 'test'})

        try:
            track_types.request('track_types')
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__track_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        track_events = streams.TrackEvents({'x_pendo_integration_key': 'test'})

        try:
            track_events.request('track_events')
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__metadata_accounts(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        metadata_accounts = streams.MetadataAccounts({'x_pendo_integration_key': 'test'})

        try:
            metadata_accounts.request('metadata_accounts')
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    
    def test_timeout__metadata_visitors(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        metadata_visitors = streams.MetadataVisitors({'x_pendo_integration_key': 'test'})

        try:
            metadata_visitors.request('metadata_visitors')
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__metadata_visitor_history(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        visitor_history = streams.VisitorHistory({'x_pendo_integration_key': 'test'})

        try:
            visitor_history.request('visitor_history', visitorId=1)
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__visitors(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        visitors = streams.Visitors({'x_pendo_integration_key': 'test'})

        try:
            visitors.request('visitors')
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_timeout__events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = requests.exceptions.Timeout

        events = streams.Events({'x_pendo_integration_key': 'test'})

        try:
            events.request('events')
        except requests.exceptions.Timeout:
            pass

        self.assertEquals(mocked_send.call_count, 5)

@mock.patch("time.sleep")
@mock.patch('requests.Session.send')
class TestConnectionResetError(unittest.TestCase):

    def test_connection_reset_error__accounts(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        accounts = streams.Accounts({'x_pendo_integration_key': 'test'})

        try:
            accounts.request('accounts')
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__features(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        features = streams.Features({'x_pendo_integration_key': 'test'})

        try:
            features.request('features')
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__guides(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        guides = streams.Guides({'x_pendo_integration_key': 'test'})

        try:
            guides.request('guides')
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__pages(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        pages = streams.Pages({'x_pendo_integration_key': 'test'})

        try:
            pages.request('pages')
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__feature_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        feature_events = streams.FeatureEvents({'x_pendo_integration_key': 'test'})

        try:
            feature_events.request('feature_events')
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__page_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        page_events = streams.PageEvents({'x_pendo_integration_key': 'test'})

        try:
            page_events.request('page_events')
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__guide_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        guide_events = streams.GuideEvents({'x_pendo_integration_key': 'test'})

        try:
            guide_events.request('guide_events')
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__poll_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        poll_events = streams.PollEvents({'x_pendo_integration_key': 'test'})

        try:
            poll_events.request('poll_events')
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__track_types(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        track_types = streams.TrackTypes({'x_pendo_integration_key': 'test'})

        try:
            track_types.request('track_types')
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__track_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        track_events = streams.TrackEvents({'x_pendo_integration_key': 'test'})

        try:
            track_events.request('track_events')
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__metadata_accounts(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        metadata_accounts = streams.MetadataAccounts({'x_pendo_integration_key': 'test'})

        try:
            metadata_accounts.request('metadata_accounts')
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    
    def test_connection_reset_error__metadata_visitors(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        metadata_visitors = streams.MetadataVisitors({'x_pendo_integration_key': 'test'})

        try:
            metadata_visitors.request('metadata_visitors')
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)


    def test_connection_reset_error__metadata_visitor_history(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        visitor_history = streams.VisitorHistory({'x_pendo_integration_key': 'test'})

        try:
            visitor_history.request('visitor_history', visitorId=1)
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__visitors(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        visitors = streams.Visitors({'x_pendo_integration_key': 'test'})

        try:
            visitors.request('visitors')
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_connection_reset_error__events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = socket.error(104, 'Connection reset by peer')

        events = streams.Events({'x_pendo_integration_key': 'test'})

        try:
            events.request('events')
        except ConnectionResetError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

@mock.patch("time.sleep")
@mock.patch('requests.Session.send')
class TestProtocolError(unittest.TestCase):

    def test_protocol_error__accounts(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        accounts = streams.Accounts({'x_pendo_integration_key': 'test'})

        try:
            accounts.request('accounts')
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__features(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        features = streams.Features({'x_pendo_integration_key': 'test'})

        try:
            features.request('features')
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__guides(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        guides = streams.Guides({'x_pendo_integration_key': 'test'})

        try:
            guides.request('guides')
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__pages(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        pages = streams.Pages({'x_pendo_integration_key': 'test'})

        try:
            pages.request('pages')
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__feature_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        feature_events = streams.FeatureEvents({'x_pendo_integration_key': 'test'})

        try:
            feature_events.request('feature_events')
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__page_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        page_events = streams.PageEvents({'x_pendo_integration_key': 'test'})

        try:
            page_events.request('page_events')
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__guide_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        guide_events = streams.GuideEvents({'x_pendo_integration_key': 'test'})

        try:
            guide_events.request('guide_events')
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__poll_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        poll_events = streams.PollEvents({'x_pendo_integration_key': 'test'})

        try:
            poll_events.request('poll_events')
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__track_types(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        track_types = streams.TrackTypes({'x_pendo_integration_key': 'test'})

        try:
            track_types.request('track_types')
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__track_events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        track_events = streams.TrackEvents({'x_pendo_integration_key': 'test'})

        try:
            track_events.request('track_events')
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__metadata_accounts(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        metadata_accounts = streams.MetadataAccounts({'x_pendo_integration_key': 'test'})

        try:
            metadata_accounts.request('metadata_accounts')
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    
    def test_protocol_error__metadata_visitors(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        metadata_visitors = streams.MetadataVisitors({'x_pendo_integration_key': 'test'})

        try:
            metadata_visitors.request('metadata_visitors')
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)


    def test_protocol_error__metadata_visitor_history(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        visitor_history = streams.VisitorHistory({'x_pendo_integration_key': 'test'})

        try:
            visitor_history.request('visitor_history', visitorId=1)
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__visitors(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        visitors = streams.Visitors({'x_pendo_integration_key': 'test'})

        try:
            visitors.request('visitors')
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

    def test_protocol_error__events(self, mocked_send, mocked_sleep):
        mocked_send.side_effect = ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')", ConnectionResetError(104, 'Connection reset by peer'))

        events = streams.Events({'x_pendo_integration_key': 'test'})

        try:
            events.request('events')
        except ProtocolError:
            pass

        self.assertEquals(mocked_send.call_count, 5)

@mock.patch('requests.Session.send')
class Positive(unittest.TestCase):

    def test_positive__accounts(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        accounts = streams.Accounts({'x_pendo_integration_key': 'test'})

        resp = accounts.request('accounts')
        
        self.assertEquals(resp, json)

    def test_positive__features(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        features = streams.Features({'x_pendo_integration_key': 'test'})

        resp = features.request('features')
        
        self.assertEquals(resp, json)

    def test_positive__guides(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        guides = streams.Guides({'x_pendo_integration_key': 'test'})

        resp = guides.request('guides')
        
        self.assertEquals(resp, json)

    def test_positive__pages(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        pages = streams.Pages({'x_pendo_integration_key': 'test'})

        resp = pages.request('pages')
        
        self.assertEquals(resp, json)

    def test_positive__feature_events(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        feature_events = streams.FeatureEvents({'x_pendo_integration_key': 'test'})

        resp = feature_events.request('feature_events')
        
        self.assertEquals(resp, json)

    def test_positive__page_events(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        page_events = streams.PageEvents({'x_pendo_integration_key': 'test'})

        resp = page_events.request('page_events')
        
        self.assertEquals(resp, json)

    def test_positive__guide_events(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        guide_events = streams.GuideEvents({'x_pendo_integration_key': 'test'})

        resp = guide_events.request('guide_events')
        
        self.assertEquals(resp, json)

    def test_positive__poll_events(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        poll_events = streams.PollEvents({'x_pendo_integration_key': 'test'})

        resp = poll_events.request('poll_events')
        
        self.assertEquals(resp, json)

    def test_positive__track_types(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        track_types = streams.TrackTypes({'x_pendo_integration_key': 'test'})

        resp = track_types.request('track_types')
        
        self.assertEquals(resp, json)

    def test_positive__track_events(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        track_events = streams.TrackEvents({'x_pendo_integration_key': 'test'})

        resp = track_events.request('track_events')
        
        self.assertEquals(resp, json)

    def test_positive__metadata_accounts(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        metadata_accounts = streams.MetadataAccounts({'x_pendo_integration_key': 'test'})

        resp = metadata_accounts.request('metadata_accounts')
        
        self.assertEquals(resp, json)

    
    def test_positive__metadata_visitors(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        metadata_visitors = streams.MetadataVisitors({'x_pendo_integration_key': 'test'})

        resp = metadata_visitors.request('metadata_visitors')
        
        self.assertEquals(resp, json)


    def test_positive__metadata_visitor_history(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        visitor_history = streams.VisitorHistory({'x_pendo_integration_key': 'test'})

        resp = visitor_history.request('visitor_history', visitorId=1)
        
        self.assertEquals(resp, json)

    def test_positive__visitors(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        visitors = streams.Visitors({'x_pendo_integration_key': 'test'})

        resp = visitors.request('visitors')
        
        self.assertEquals(resp, [json])

    def test_positive__events(self, mocked_send):
        json = {"key1": "value1", "key2": "value2"}
        mocked_send.return_value = get_response(json)

        events = streams.Events({'x_pendo_integration_key': 'test'})

        resp = events.request('events')
        
        self.assertEquals(resp, [json])
