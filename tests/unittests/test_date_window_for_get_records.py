import unittest
import tap_pendo.streams as streams
from unittest import mock
from singer.utils import strftime, now
from datetime import timedelta

@mock.patch("tap_pendo.streams.Stream.update_bookmark")
@mock.patch("tap_pendo.streams.Stream.request")
class TestDateWindowing(unittest.TestCase):

    def test_get_records_date_window(self, mocked_request, mocked_update_bookmark):
        """
            Verify that the get_records() function is writing a bookmark after each date_window call.(30-day default window size)
            Verify that all data from each page are returned.
        """
        # Config and test_data for requests
        current_time = now()
        config = {"start_date": strftime(current_time - timedelta(days=50))} # Start date of 50 days ago for 2 date_window call

        mocked_request.side_effect = [
            [   # First page of test-data with 40 days ago replication-key
                {'id': 1, 'metadata': {'auto': {"lastupdated": (current_time - timedelta(days=40)).timestamp() * 1000}}},
                {'id': 2, 'metadata': {'auto': {"lastupdated": (current_time - timedelta(days=40)).timestamp() * 1000}}}
            ],
            [   # Second page of test-data with 10 days ago replication-key
                {'id': 3, 'metadata': {'auto': {"lastupdated": (current_time - timedelta(days=10)).timestamp() * 1000}}}
                
            ]
        ]

        # Configure stream object and call get_records() function
        stream_instance = streams.Visitors(config)
        stream_instance.name = "test"
        stream_instance.replication_key = "lastupdated"
        stream_instance.get_body = mock.Mock()
        # Call get_records with `test` stream and bookmark of 50 days ago
        records = list(stream_instance.get_records({}, current_time - timedelta(days=50), now()))


        expected_records = [
            {'id': 1, 'metadata': {'auto': {'lastupdated':(current_time - timedelta(days=40)).timestamp() * 1000}}},
            {'id': 2, 'metadata': {'auto': {'lastupdated':(current_time - timedelta(days=40)).timestamp() * 1000}}},
            {'id': 3, 'metadata': {'auto': {'lastupdated':(current_time - timedelta(days=10)).timestamp() * 1000}}}
        ]
        expected_update_bookmark_calls = [
            # Bookmark update after first page of data(40 days ago)
            mock.call({}, 'test', strftime(current_time - timedelta(days=40)), 'lastupdated'),
            # Bookmark update after first page of data(10 days ago)
            mock.call({}, 'test', strftime(current_time - timedelta(days=10)), 'lastupdated')
        ]

        # Verify that update_bookmark() is called after each page of data with expected values
        self.assertEquals(mocked_update_bookmark.mock_calls, expected_update_bookmark_calls)
        # Verify that all expected records from pages are returned
        self.assertEquals(records, expected_records)