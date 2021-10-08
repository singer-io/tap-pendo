import unittest
from tap_pendo.streams import FeatureEvents

class TestFeatureEventsPrimaryKeys(unittest.TestCase):

    def test_feature_event_primary_key_with_hourRange(self):
        '''
            Verify that primary keys should have expected fields with 'hour' field when period is hourRange 
        '''
        config = {"period": "hourRange"} # set hourRange as a period
        expected_primary_keys = ["feature_id", "visitor_id", "account_id", "server", "remote_ip", "user_agent", "hour"]
        feature_event_stream1 = FeatureEvents(config) # Initialize FeatuereEvents object which sets primary keys

        self.assertEqual(feature_event_stream1.key_properties, expected_primary_keys)

        # Reset key properties for other test as it's class variable
        FeatureEvents.key_properties = ['feature_id', 'visitor_id', 'account_id', 'server', 'remote_ip', 'user_agent']

    def test_feature_event_primary_key_with_dayRange(self):
        '''
            Verify that primary keys should have expected fields with 'day' field when period is dayRange 
        '''
        config = {"period": "dayRange"} # set dayRange as a period
        expected_primary_keys = ["feature_id", "visitor_id", "account_id", "server", "remote_ip", "user_agent", "day"]        
        feature_event_stream2 = FeatureEvents(config) # Initialize Events object which sets primary keys

        self.assertEqual(feature_event_stream2.key_properties, expected_primary_keys)

        # Reset key properties for other test as it's class variable
        FeatureEvents.key_properties = ['feature_id', 'visitor_id', 'account_id', 'server', 'remote_ip', 'user_agent']