import unittest
from unittest import mock
from parameterized import parameterized
from singer.utils import strptime_to_utc
from tap_pendo.streams import EventsBase, API_RECORD_LIMIT


RECORD_LIMIT = 2

def generate_records(num_records, replication_value):
    return [{"appID": 10000, "hour": replication_value+1} for i in range(num_records)]


class TestRecordLimit(unittest.TestCase):
    @parameterized.expand(
        [('None_value', None, API_RECORD_LIMIT),
         ('zero_value', 0, API_RECORD_LIMIT),
         ('white_spaces', '\t  ', API_RECORD_LIMIT),
         ('integer_value', 1000, 1000),
         ('float_value', 4567.5, 4567),
         ('numeric_integer_string', "    1234   ", 1234),
         ('numeric_float_float', "1234.5", 1234),
         ('limit_minus_one_value', API_RECORD_LIMIT - 1, API_RECORD_LIMIT - 1),
         ('max_record_limit', API_RECORD_LIMIT, API_RECORD_LIMIT),
         ('limit_plus_one_value', API_RECORD_LIMIT + 1, API_RECORD_LIMIT + 1)])
    def test_record_limit(self, name, record_limit, expected_record_limit):
        """
            Verify record limit is set properly as per tap configuration
        """
        event_obj = EventsBase({"record_limit": record_limit} if record_limit else {})
        self.assertEqual(event_obj.record_limit, expected_record_limit)

    @parameterized.expand([
        ['alphabets', ['abcd', ValueError], 'Invalid numeric value: '],
        ['alphanumeric', ['123abc', ValueError], 'Invalid numeric value: ']])
    def test_invalid_record_limit(self, name, test_data, exception_string):
        with self.assertRaises(test_data[1]) as e:
            EventsBase({"record_limit": test_data[0]} if test_data[0] else {})

        self.assertIn(exception_string, str(e.exception))

    @parameterized.expand(
        [('returned_1_record', [{"results": generate_records(1, 1)}], False),
        ('returned_records_equals_record_limit', [{"results": generate_records(RECORD_LIMIT, 0)},
                                                  {"results": generate_records(1, RECORD_LIMIT)}], True),
        ('returned_records_more_than_record_limit', [{"results": generate_records(RECORD_LIMIT+2, 0)}], True),])
    @mock.patch("tap_pendo.streams.EventsBase.remove_last_timestamp_records")
    @mock.patch("tap_pendo.streams.EventsBase.get_first_parameter_value", return_value=1000)
    @mock.patch("tap_pendo.streams.EventsBase.set_request_body_filters")
    @mock.patch("tap_pendo.streams.EventsBase.set_time_series_first")
    @mock.patch("tap_pendo.streams.Stream.request")
    def test_record_limit(self,
                           name,
                           test_data,
                           expected_loop_for_records,
                           mock_request,
                           mock_set_time_series,
                           mock_set_request_filter,
                           mock_get_first_parameter,
                           mock_remove_last_records):
        """
            Verify that custom pagination mechanism loops as exepcted depending on the record limit set
            Testing the boundry values for the record limits
        """
        event_obj = EventsBase({})
        mock_state = {}
        event_obj.record_limit = 2
        mock_start_date = strptime_to_utc("2021-01-01T00:00:00Z")
        mock_request.side_effect = test_data
        mock_remove_last_records.return_value = test_data[0]["results"], []
        _, actual_loop_for_records = event_obj.sync(mock_state, mock_start_date, "PARENT-ID")
        self.assertEqual(actual_loop_for_records, expected_loop_for_records)


    @parameterized.expand(
        [("lesser_than_record_limit", API_RECORD_LIMIT-1, API_RECORD_LIMIT),
         ("equal_to_record_limit", API_RECORD_LIMIT, API_RECORD_LIMIT),
         ("greater_than_record_limit", API_RECORD_LIMIT+1, API_RECORD_LIMIT+1)])
    def test_remove_last_timestamp_records(self, name, record_limit, expected_record_limit):
        # create records with same replication value
        number_of_records = 100
        source_records = [{event_obj.replication_key: 100}] * number_of_records

        event_obj = EventsBase({"record_limit": record_limit})
        event_obj.replication_key = "hour"
        event_obj.record_limit = record_limit

        records, last_processed = event_obj.remove_last_timestamp_records(records=source_records)
        self.assertEquals(len(records), 0)
        self.assertEquals(len(last_processed), number_of_records)
        self.assertEquals(event_obj.record_limit, expected_record_limit)

