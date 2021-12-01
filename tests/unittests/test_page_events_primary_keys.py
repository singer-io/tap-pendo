import unittest
from tap_pendo.streams import PageEvents

class TestPageEventsPrimaryKeys(unittest.TestCase):

    def test_feature_event_primary_key_with_hourRange(self):
        '''
            Verify that primary keys should have expected fields with 'hour' field when period is hourRange 
        '''
        # Reset key properties to default value
        PageEvents.key_properties = ['page_id', 'visitor_id', 'account_id', 'server', 'remote_ip', 'user_agent', '_sdc_parameters_hash']
        config = {"period": "hourRange"} # set hourRange as a period
        expected_primary_keys = ["page_id", "visitor_id", "account_id", "server", "remote_ip", "user_agent", "_sdc_parameters_hash", "hour"]

        page_event_stream1 = PageEvents(config) # Initialize PageEvents object which sets primary keys

        self.assertEqual(page_event_stream1.key_properties, expected_primary_keys)

    def test_feature_event_primary_key_with_dayRange(self):
        '''
            Verify that primary keys should have expected fields with 'day' field when period is dayRange 
        '''
        # Reset key properties to default value
        PageEvents.key_properties = ['page_id', 'visitor_id', 'account_id', 'server', 'remote_ip', 'user_agent', '_sdc_parameters_hash']
        config = {"period": "dayRange"} # set dayRange as a period
        expected_primary_keys = ["page_id", "visitor_id", "account_id", "server", "remote_ip", "user_agent", "_sdc_parameters_hash", "day"]

        page_event_stream2 = PageEvents(config) # Initialize PageEvents object which sets primary keys

        self.assertEqual(page_event_stream2.key_properties, expected_primary_keys)
