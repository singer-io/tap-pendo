import unittest
from tap_pendo.streams import TrackEvents

class TestTrackEventsPrimaryKey(unittest.TestCase):

    def test_track_events_primary_key_with_hourRange(self):
        '''
            Verify that primary keys should have expected fields with 'hour' field when period is hourRange 
        '''
        # Reset key properties to default value
        TrackEvents.key_properties = ["track_type_id", "visitor_id", "account_id", "server", "remote_ip", "user_agent"]
        config = {"period": "hourRange"} # set hourRange as a period

        expected_primary_keys = ["track_type_id", "visitor_id", "account_id", "server", "remote_ip", "user_agent", "hour"]

        event_stream1 = TrackEvents(config) # Initialize TrackEvents object which sets primary keys

        # verify if the key properties macthes the expectation
        self.assertEqual(event_stream1.key_properties, expected_primary_keys)

    def test_track_events_primary_key_with_dayRange(self):
        '''
            Verify that primary keys should have expected fields with 'day' field when period is dayRange 
        '''
        # Reset key properties to default value
        TrackEvents.key_properties = ["track_type_id", "visitor_id", "account_id", "server", "remote_ip", "user_agent"]
        config = {"period": "dayRange"} # set dayRange as a period

        expected_primary_keys = ["track_type_id", "visitor_id", "account_id", "server", "remote_ip", "user_agent", "day"]        

        event_stream2 = TrackEvents(config) # Initialize TrackEvents object which sets primary keys

        # verify if the key properties macthes the expectation
        self.assertEqual(event_stream2.key_properties, expected_primary_keys)
