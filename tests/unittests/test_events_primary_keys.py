import unittest
from tap_pendo.streams import Events

class TestEventsPrimaryKeys(unittest.TestCase):

    def test_event_primary_key_with_hourRange(self):
        '''
            Verify that primary keys should have expected fields with 'hour' field when period is hourRange 
        '''
        # Reset key properties to default value
        Events.key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip', 'user_agent']
        config = {"period": "hourRange"} # set hourRange as a period
        expected_primary_keys = ["visitor_id", "account_id", "server", "remote_ip", "user_agent", "hour"]

        event_stream1 = Events(config) # Initialize Events object which sets primary keys

        self.assertEqual(event_stream1.key_properties, expected_primary_keys)

        

    def test_event_primary_key_with_dayRange(self):
        '''
            Verify that primary keys should have expected fields with 'day' field when period is dayRange 
        '''
        # Reset key properties to default value
        Events.key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip', 'user_agent']
        config = {"period": "dayRange"} # set dayRange as a period
        expected_primary_keys = ["visitor_id", "account_id", "server", "remote_ip", "user_agent", "day"]
    
        event_stream2 = Events(config) # Initialize Events object which sets primary keys

        self.assertEqual(event_stream2.key_properties, expected_primary_keys)
