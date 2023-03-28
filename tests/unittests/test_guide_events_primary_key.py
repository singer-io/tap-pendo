import unittest
from tap_pendo.streams import GuideEvents

class TestGuideEventsPrimaryKey(unittest.TestCase):

    def test_guide_event_primary_key(self):
        '''
            Verify the primary keys for 'guide_events' stream
        '''
        # initialize config
        config = {}
        # expected primary key
        expected_primary_keys = ['guide_id', 'guide_step_id', 'visitor_id', 'type', 'account_id', 'browser_time', 'url']

        # Initialize GuideEvents object which sets primary keys
        guide_events = GuideEvents(config)

        # verify the Primary Key
        self.assertEqual(guide_events.key_properties, expected_primary_keys)
