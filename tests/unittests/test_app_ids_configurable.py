import unittest
from tap_pendo.streams import Features


class TestAppIdConfiguration(unittest.TestCase):
    def test_app_ids_not_in_config(self):
        """
            To verify that if app_ids is not in config than select all apps.
        """
        config = {}
        stream_obj = Features(config)
        self.assertEqual(stream_obj.app_ids,  "expandAppIds(\"*\")", "Both values are not same")
        
    def test_app_ids_empty_in_config(self):
        """
            To verify that if app_ids is blank string or empty string in config than select all apps.
        """
        config = {"app_ids": " "}
        stream_obj = Features(config)
        self.assertEqual(stream_obj.app_ids,  "expandAppIds(\"*\")", "Both values are not same")
    
    def test_app_ids_comaseperated_string_in_config(self):
        """
            To verify that f app_ids is comma seperated string in config than get list of those app_ids.
        """
        config = {"app_ids": "test1, test2"}
        stream_obj = Features(config)
        self.assertEqual(stream_obj.app_ids,  ["test1", "test2"], "Both values are not same")
        