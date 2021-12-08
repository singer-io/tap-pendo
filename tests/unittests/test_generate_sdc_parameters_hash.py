import unittest
from unittest import mock
from tap_pendo.streams import PageEvents

@mock.patch("hashlib.sha256")
class TestPageEventsPrimaryKeyHash(unittest.TestCase):

    def test_page_events_null_parameters(self, mocked_sha256):
        """
            Test case to verify the hash for 'parameters' having 'null' value
        """

        config = {"period": "hourRange"}
        # mock record
        records = [
            {
                "accountId": "Acme Corp",
                "visitorId": "10",
                "numEvents": 1,
                "numMinutes": 1,
                "server": "www.pendosandbox.com",
                "remoteIp": "110.227.245.175",
                "userAgent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
                "day": 1506484800000,
                "pageId": "aJ0KOADQZVL5h9zQhiQ0q26PNTM",
                "parameters": None
            }
        ]

        # create 'PageEvents' class
        page_event = PageEvents(config)
        # function call
        new_records = page_event.generate_sdc_parameters_hash(records)

        # verify that we called hash function with empty string
        mocked_sha256.assert_called_with(b"")

    def test_page_events_empty_string_value(self, mocked_sha256):
        """
            Test case to verify the hash for 'parameters' containing empty string as value
        """

        # mock parameters
        parameters = {"param": ""}
        config = {"period": "hourRange"}
        # mock rcords
        records = [
            {
                "accountId": "Acme Corp",
                "visitorId": "10",
                "numEvents": 1,
                "numMinutes": 1,
                "server": "www.pendosandbox.com",
                "remoteIp": "110.227.245.175",
                "userAgent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
                "day": 1506484800000,
                "pageId": "aJ0KOADQZVL5h9zQhiQ0q26PNTM",
                "parameters": parameters
            }
        ]

        # create 'PageEvents' class
        page_event = PageEvents(config)
        # function call
        new_records = page_event.generate_sdc_parameters_hash(records)

        # verify that we called hash function with key and value as empty string
        mocked_sha256.assert_called_with(b"param")

    def test_page_events_key_value(self, mocked_sha256):
        """
            Test case to verify the hash for 'parameters' containing key-value pairs
        """

        # mock parameters
        parameters = {"param": "value"}
        config = {"period": "hourRange"}
        # mock records
        records = [
            {
                "accountId": "Acme Corp",
                "visitorId": "10",
                "numEvents": 1,
                "numMinutes": 1,
                "server": "www.pendosandbox.com",
                "remoteIp": "110.227.245.175",
                "userAgent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
                "day": 1506484800000,
                "pageId": "aJ0KOADQZVL5h9zQhiQ0q26PNTM",
                "parameters": parameters
            }
        ]

        # create 'PageEvents' class
        page_event = PageEvents(config)
        # function call
        new_records = page_event.generate_sdc_parameters_hash(records)

        # verify that we called hash function with key and value
        mocked_sha256.assert_called_with(b"paramvalue")
