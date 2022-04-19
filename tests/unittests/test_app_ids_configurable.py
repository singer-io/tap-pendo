import unittest
from unittest import mock
from argparse import Namespace
from tap_pendo import main


class TestAppIdConfiguration(unittest.TestCase):
    @mock.patch("tap_pendo.utils.parse_args", side_effect=lambda required_config_keys: Namespace(config={"start_date": "", "x_pendo_integration_key": "", "period":""},  discover=True))
    @mock.patch("tap_pendo.discover_streams", side_effect=lambda config: {})
    @mock.patch('tap_pendo.do_discover')
    def test_app_ids_not_in_config(self, mocked_do_discover, mocked_discover_stream, mocked_parse_args):
        """
            To verify that if app_ids is not in configure file then select all apps and run discover mode.
        """
        main()
        config = {'app_ids': 'expandAppIds("*")', 'start_date': '', 'x_pendo_integration_key': '', 'period': ''}
        mocked_do_discover.assert_called_with(config)
        
    @mock.patch("tap_pendo.utils.parse_args", side_effect=lambda required_config_keys: Namespace(config={"app_ids": "123, 456","start_date": "", "x_pendo_integration_key": "", "period":""},  discover=True))
    @mock.patch("tap_pendo.discover_streams", side_effect=lambda config: {})
    @mock.patch('tap_pendo.do_discover')
    def test_app_ids_coma_seperated_string_in_config(self, mocked_do_discover, mocked_discover_stream, mocked_parse_args):
        """
            To verify that if app_ids is comma seperated string in configure file then get list of those app_ids and run discover mode.
        """
        main()
        config = {'app_ids': ['123', '456'], 'start_date': '', 'x_pendo_integration_key': '', 'period': ''}
        mocked_do_discover.assert_called_with(config)
        
    @mock.patch("tap_pendo.utils.parse_args", side_effect=lambda required_config_keys: Namespace(config={"app_ids": "","start_date": "", "x_pendo_integration_key": "", "period":""},  discover=True))
    @mock.patch("tap_pendo.discover_streams", side_effect=lambda config: {})
    @mock.patch('tap_pendo.do_discover')
    def test_app_ids_empty_in_config(self, mocked_do_discover, mocked_discover_stream, mocked_parse_args):
        """
            To verify that if app_ids is blank string or empty string in configure file then select all apps and run discover mode.
        """
        main()
        config = {'app_ids': 'expandAppIds("*")', 'start_date': '', 'x_pendo_integration_key': '', 'period': ''}
        mocked_do_discover.assert_called_with(config)
    
    @mock.patch("tap_pendo.utils.parse_args", side_effect=lambda required_config_keys: Namespace(config={"app_ids": "123, test, test123, 123test,  ","start_date": "", "x_pendo_integration_key": "", "period":""},  discover=True))
    def test_app_ids_valid_app_ids_with_invalid_app_ids_config(self, mocked_parse_args):
        """
            To verify that if app_ids is comma seperated blanks with string in configure file then then raise exception.
        """
        
        with self.assertRaises(Exception) as e:
            main()
            
        self.assertEqual(str(e.exception), "Invalid appIDs provided during the configuration:['test', 'test123', '123test', '']", "Not get expected exception")