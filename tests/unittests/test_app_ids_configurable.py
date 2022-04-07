import unittest
from unittest import mock
from tap_pendo.streams import Features


class TestAppIdConfiguration(unittest.TestCase):
    def test_app_ids_not_in_config(self):
        """
            To verify that if app_ids is not in config then select all apps.
        """
        config = {}
        stream_obj = Features(config)
        self.assertEqual(stream_obj.app_ids,  "expandAppIds(\"*\")", "Both values are not same")
        
    def test_app_ids_empty_in_config(self):
        """
            To verify that if app_ids is blank string or empty string in config then select all apps.
        """
        config = {"app_ids": " "}
        stream_obj = Features(config)
        self.assertEqual(stream_obj.app_ids,  "expandAppIds(\"*\")", "Both values are not same")
    
    def test_app_ids_coma_seperated_string_in_config(self):
        """
            To verify that if app_ids is comma seperated string in config then get list of those app_ids.
        """
        config = {"app_ids": "test1, test2"}
        stream_obj = Features(config)
        self.assertEqual(stream_obj.app_ids,  ["test1", "test2"], "Both values are not same")
        
    def test_app_ids_coma_seperated_blanks_in_config(self):
        """
            To verify that if app_ids is comma seperated blanks in config then exception is raised.
        """
        
        config = {"app_ids": ","}
        with self.assertRaises(Exception) as e:
            stream_obj = Features(config)

        self.assertEqual(str(e.exception), "All app_ids provided in a configuration are blank.")
        
    @mock.patch("tap_pendo.streams.LOGGER.warning")
    def test_app_ids_blank_ids_with_non_blank_ids_in_config(self, mocked_logger):
        """
            To verify that if app_ids is comma seperated blanks with string in config then logger gives warning
        """
        
        config = {"app_ids": "test1, "}
        stream_obj = Features(config)

        self.assertTrue(stream_obj.app_ids==["test1"])

        mocked_logger.assert_called_with("The app_ids provided in a configuration contain some blank values and the tap will ignore blank values.")