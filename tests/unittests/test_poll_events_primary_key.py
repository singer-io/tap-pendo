import unittest
from tap_pendo.streams import PollEvents

class TestPollEventsPrimaryKey(unittest.TestCase):

    def test_poll_event_primary_key(self):
        '''
            Verify that primary keys for 'poll_events' stream
        '''
        # initialize config
        config = {}
        # expected primary key
        expected_primary_keys = ['visitor_id', 'account_id', 'poll_id', 'browser_time']

        # Initialize PollEvents object which sets primary keys
        poll_events = PollEvents(config)

        # verify the Primary Key
        self.assertEqual(poll_events.key_properties, expected_primary_keys)
