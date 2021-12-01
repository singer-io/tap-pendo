import unittest
import hashlib
from tap_pendo.streams import PageEvents

class TestPageEventsPrimaryKeyHash(unittest.TestCase):

    def test_page_events_null_parameters(self):
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

        # create empty string for calculating hash
        empty_string = ""
        # encode the created string
        empty_string_bytes = empty_string.encode('utf-8')
        # calculate the hash of the string for assertion
        empty_string_hash = hashlib.sha256(empty_string_bytes).hexdigest()

        # create 'PageEvents' class
        page_event = PageEvents(config)
        # function call
        records = page_event.generate_sdc_parameters_hash(records)

        # verify the hash we calculated and the hash we get in the record
        self.assertEquals(empty_string_hash, records[0].get("_sdc_parameters_hash"))

    def test_page_events_empty_string_value(self):
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

        # create empty string for calculating hash
        parameters_string = ""
        # create key-value tuple
        parameters_pairs = sorted(tuple((k, v) for k, v in parameters.items()), key=lambda x: x[0])
        for pair in parameters_pairs:
            # create string of key-values ie. key1value1key2value2...
            parameters_string += "".join(pair)
        # encode the created string
        parameters_string_bytes = parameters_string.encode('utf-8')
        # calculate the hash of the string for assertion
        parameters_string_hash = hashlib.sha256(parameters_string_bytes).hexdigest()

        # create 'PageEvents' class
        page_event = PageEvents(config)
        # function call
        new_records = page_event.generate_sdc_parameters_hash(records)
        # verify the hash we calculated and the hash we get in the record
        self.assertEquals(parameters_string_hash, new_records[0].get("_sdc_parameters_hash"))

    def test_page_events_key_value(self):
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

        # create empty string for calculating hash
        parameters_string = ""
        # create key-value tuple
        parameters_pairs = sorted(tuple((k, v) for k, v in parameters.items()), key=lambda x: x[0])
        for pair in parameters_pairs:
            # create string of key-values ie. key1value1key2value2...
            parameters_string += "".join(pair)
        # encode the created string
        parameters_string_bytes = parameters_string.encode('utf-8')
        # calculate the hash of the string for assertion
        parameters_string_hash = hashlib.sha256(parameters_string_bytes).hexdigest()

        # create 'PageEvents' class
        page_event = PageEvents(config)
        # function call
        new_records = page_event.generate_sdc_parameters_hash(records)
        # verify the hash we calculated and the hash we get in the record
        self.assertEquals(parameters_string_hash, new_records[0].get("_sdc_parameters_hash"))
