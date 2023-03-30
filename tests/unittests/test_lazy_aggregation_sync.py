import unittest
import requests
from unittest import mock
from tap_pendo.streams import Visitors

class Mockresponse:
    def __init__(self, resp, status_code, headers=None, raise_error=False):
        self.status_code = status_code
        self.raw = resp
        self.headers = headers
        self.raise_error = raise_error
        self.reason = "error"

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("mock sample message")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        return True

    def close(self):
        return True

# Mocking sync of substream
def mocked_substream(state, parent, sub_stream, parent_response):
    for record in parent_response:
        pass

class TestLazyAggregationSync(unittest.TestCase):
    '''
        Verify that sync of LazzyAggregation is return all the data
    '''

    @mock.patch("requests.Session.send")
    @mock.patch("tap_pendo.streams.Stream.is_selected")
    @mock.patch("tap_pendo.streams.Stream.sync_substream", side_effect=mocked_substream)
    def test_lazzy_aggregation_with_sub_stream(self, mocked_substream, mocked_selected, mocked_request):
        '''
            Verify that if sub stream is present then also all data should be return for super stream
            and sync_substream should be called
        '''
        expected_data = [{"id":1}, {"id":2}, {"id":3}]
        records = '{"results": [{"id":1}, {"id":2}, {"id":3}]}'
        mocked_selected.return_value = True # Sub stream is selected
        mocked_request.return_value = Mockresponse(records, 200, raise_error=False)
        config = {'start_date': '2021-01-01T00:00:00Z',
                  'x_pendo_integration_key': 'test'}

        lazzy_aggr = Visitors(config)
        (_, stream_response), _ = lazzy_aggr.sync({})

        self.assertEqual(list(stream_response), expected_data) # parent stream get all expected data
        self.assertEqual(mocked_substream.call_count, 1)

    @mock.patch("requests.Session.send")
    @mock.patch("tap_pendo.streams.Stream.is_selected")
    @mock.patch("tap_pendo.streams.Stream.sync_substream", side_effect=mocked_substream)
    def test_lazzy_aggregation_without_sub_stream(self, mocked_substream, mocked_selected, mocked_request):
        '''
            Verify that if sub stream is not selected then also all data should be return for super stream
            and sync_substream should not be called
        '''
        expected_data = [{"id":1}, {"id":2}, {"id":3}]
        records = '{"results": [{"id":1}, {"id":2}, {"id":3}]}'
        mocked_selected.return_value = False # Sub stream is not selected
        mocked_request.return_value = Mockresponse(records, 200, raise_error=False)
        config = {'start_date': '2021-01-01T00:00:00Z',
                  'x_pendo_integration_key': 'test'}

        lazzy_aggr = Visitors(config)
        (_, stream_response), _ = lazzy_aggr.sync({})

        self.assertEqual(list(stream_response), expected_data)
        self.assertEqual(mocked_substream.call_count, 0)

class TestConfigParsing(unittest.TestCase):

    def test_reading_include_anonymous_visitors_provided_valid_input_true(self):
        config = {
            'include_anonymous_visitors': 'true'
        }

        my_visitor = Visitors(config)


        return_value = my_visitor.get_body()
        expected_value = True

        # This is the value that matches the config
        actual_value = not return_value['request']['pipeline'][0]['source']['visitors']['identified']

        self.assertEqual(expected_value, actual_value)

    def test_reading_include_anonymous_visitors_provided_valid_input_TRUE(self):
        config = {
            'include_anonymous_visitors': 'TRUE'
        }

        my_visitor = Visitors(config)


        return_value = my_visitor.get_body()
        expected_value = True

        # This is the value that matches the config
        actual_value = not return_value['request']['pipeline'][0]['source']['visitors']['identified']

        self.assertEqual(expected_value, actual_value)

    def test_reading_include_anonymous_visitors_provided_valid_input_false(self):
        config = {
            'include_anonymous_visitors': 'false'
        }

        my_visitor = Visitors(config)


        return_value = my_visitor.get_body()
        expected_value = False

        # This is the value that matches the config
        actual_value = not return_value['request']['pipeline'][0]['source']['visitors']['identified']

        self.assertEqual(expected_value, actual_value)

    def test_reading_include_anonymous_visitors_provided_invalid_input(self):
        config = {
            'include_anonymous_visitors': 'bad input'
        }

        my_visitor = Visitors(config)

        return_value = my_visitor.get_body()
        expected_value = False
        actual_value = not return_value['request']['pipeline'][0]['source']['visitors']['identified']
        self.assertEqual(expected_value, actual_value)

    def test_reading_include_anonymous_visitors_provided_none(self):
        config = {
            'include_anonymous_visitors': None
        }

        my_visitor = Visitors(config)

        return_value = my_visitor.get_body()
        expected_value = False
        actual_value = not return_value['request']['pipeline'][0]['source']['visitors']['identified']
        self.assertEqual(expected_value, actual_value)

    def test_reading_include_anonymous_visitors_no_provided_input(self):
        config = {}

        my_visitor = Visitors(config)

        return_value = my_visitor.get_body()
        expected_value = False
        actual_value = not return_value['request']['pipeline'][0]['source']['visitors']['identified']
        self.assertEqual(expected_value, actual_value)

    def test_reading_include_anonymous_visitors_provided_boolean_true(self):
        config = {
            'include_anonymous_visitors': True
        }

        my_visitor = Visitors(config)

        return_value = my_visitor.get_body()
        expected_value = True
        actual_value = not return_value['request']['pipeline'][0]['source']['visitors']['identified']
        self.assertEqual(expected_value, actual_value)

    def test_reading_include_anonymous_visitors_provided_boolean_false(self):
        config = {
            'include_anonymous_visitors': False
        }

        my_visitor = Visitors(config)

        return_value = my_visitor.get_body()
        expected_value = False
        actual_value = not return_value['request']['pipeline'][0]['source']['visitors']['identified']
        self.assertEqual(expected_value, actual_value)
