import unittest
from tap_pendo.streams import Events

class TestEventsPrimaryKeys(unittest.TestCase):

    def test_event_primary_key_with_hourRange(self):
        '''
            Verify that primary keys should not have expected fields with 'hour' field when period is hourRange
        '''
        config = {"period": "hourRange"} # set hourRange as a period
        event_stream1 = Events(config) # Initialize Events object which sets primary keys

        self.assertNotIn("hour", event_stream1.key_properties)



    def test_event_primary_key_with_dayRange(self):
        '''
            Verify that primary keys should not have expected fields with 'day' field when period is dayRange
        '''
        config = {"period": "dayRange"} # set dayRange as a period
        event_stream2 = Events(config) # Initialize Events object which sets primary keys

        self.assertNotIn("day", event_stream2.key_properties)
