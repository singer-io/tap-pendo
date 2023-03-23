from asyncio import streams
from datetime import datetime as dt

import tap_tester.connections as connections
import tap_tester.runner as runner
from base import TestPendoBase
from tap_tester.logger import LOGGER

class PendoStartDateTest(TestPendoBase):
    """Instantiate start date according to the desired data set and run the test"""

    def get_properties(self, *args, **kwargs):
        props = super().get_properties(*args, **kwargs)
        props.pop('lookback_window')
        return props

    start_date_1 = ""
    start_date_2 = ""
    streams = None

    def name(self):
        return "pendo_start_date_test"

    def test_run(self):
        # All streams have test records with different timestamps, taking older start date takes longer time to finish the test
        # So defining individual start dates for each streams to limit the execution time
        self.run_test("2021-09-09T00:00:00Z", "2022-06-20T00:00:00Z", {"accounts"})
        self.run_test("2021-09-09T00:00:00Z", "2022-05-01T00:00:00Z", {"metadata_visitors", "metadata_accounts"})
        self.run_test("2020-09-01T00:00:00Z", "2021-03-01T00:00:00Z", {"events"})
        self.run_test("2021-09-09T00:00:00Z", "2021-09-16T00:00:00Z", {"guides", "guide_events"})

        # All these streams have similar implementation like guides and guide_events so removing this test to limit the execution time
        # self.run_test("2020-09-01T00:00:00Z", "2021-03-01T00:00:00Z", {"features", "feature_events", "pages", "page_events", "events", "track_types", "track_events"})
        
        # Visitors history can be retrieved only for 180 days so to reduce execution time setting first start time older than 180 days back
        self.run_test(
            start_date_1="2022-06-25T00:00:00Z",
            start_date_2="2022-07-20T00:00:00Z",
            streams={"visitors", "visitor_history"})

    def expected_metadata(self):
        visitor_history = {
            # Add back when visitor_history stream causing this test to take 4+ hours is solved,
            # tracked in this JIRA: https://stitchdata.atlassian.net/browse/SRCE-4755
            # Improvised the execution
            #   - Added filtering visitors based on last updated which will reduce the execution time
            #   - Testing visitors streams separately with latest start time
            "visitor_history": {
                self.PRIMARY_KEYS: {'visitor_id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'modified_ts'}
            }
        }

        metadata = super().expected_metadata()
        if self.streams == {"visitors", "visitor_history"}:
            metadata.update(visitor_history)
        else:
            metadata = super().expected_metadata()

        return metadata

    def run_test(self, start_date_1, start_date_2, streams):
        """
        Test that the start_date configuration is respected
        • verify that a sync with a later start date has at least one record synced
        and less records than the 1st sync with a previous start date
        • verify that each stream has less records than the earlier start date sync
        • verify all data from later start data has bookmark values >= start_date
        """

        self.start_date_1 = start_date_1
        self.start_date_2 = start_date_2
        self.streams = streams
        
        self.start_date = self.start_date_1

        expected_streams = streams

        ##########################################################################
        # First Sync
        ##########################################################################

        # instantiate connection
        conn_id_1 = connections.ensure_connection(self, original_properties=False)

        # run check mode
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        # table and field selection
        test_catalogs_1_all_fields = [catalog for catalog in found_catalogs_1
                                      if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(
            conn_id_1, test_catalogs_1_all_fields, select_all_fields=True)

        # run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        # Update START DATE Between Syncs
        ##########################################################################
        
        LOGGER.info("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(
            self.start_date, self.start_date_2))
        self.start_date = self.start_date_2

        ##########################################################################
        # Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id_2 = connections.ensure_connection(self, original_properties=False)

        # run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs_2
                                      if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(
            conn_id_2, test_catalogs_2_all_fields, select_all_fields=True)

        # run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)
        synced_records_2 = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_pks()[stream]
                expected_start_date_1 = self.timedelta_formatted(self.start_date_1, -1)
                expected_start_date_2 = self.timedelta_formatted(self.start_date_2, -1)

                # collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = record_count_by_stream_2.get(stream, 0)

                primary_keys_list_1 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_1.get(stream, {}).get('messages', [])
                                       if message.get('action') == 'upsert']
                primary_keys_list_2 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_2.get(stream, {}).get('messages', [])
                                       if message.get('action') == 'upsert']

                primary_keys_sync_1 = set(primary_keys_list_1)
                primary_keys_sync_2 = set(primary_keys_list_2)

                if self.is_incremental(stream):

                    # collect information specific to incremental streams from syncs 1 & 2
                    expected_replication_key = next(
                        iter(self.expected_replication_keys().get(stream, [])))
                    replication_dates_1 = [row.get('data').get(expected_replication_key) for row in
                                        synced_records_1.get(stream, {'messages': []}).get('messages', [])
                                        if row.get('data')]
                    replication_dates_2 = [row.get('data').get(expected_replication_key) for row in
                                        synced_records_2.get(stream, {'messages': []}).get('messages', [])
                                        if row.get('data')]

                    # Verify replication key is greater or equal to start_date for sync 1
                    for replication_date in replication_dates_1:
                        self.assertGreaterEqual(
                            self.parse_date(replication_date), self.parse_date(
                                expected_start_date_1),
                            msg="Report pertains to a date prior to our start date.\n" +
                            "Sync start_date: {}\n".format(expected_start_date_1) +
                                "Record date: {} ".format(replication_date)
                        )

                    # Verify replication key is greater or equal to start_date for sync 2
                    for replication_date in replication_dates_2:
                        self.assertGreaterEqual(
                            self.parse_date(replication_date), self.parse_date(
                                expected_start_date_2),
                            msg="Report pertains to a date prior to our start date.\n" +
                            "Sync start_date: {}\n".format(expected_start_date_2) +
                                "Record date: {} ".format(replication_date)
                        )

                    # Verify the number of records replicated in sync 1 is greater than the number
                    # of records replicated in sync 2
                    self.assertGreaterEqual(record_count_sync_1, record_count_sync_2)

                    # Verify the records replicated in sync 2 were also replicated in sync 1
                    self.assertTrue(
                        primary_keys_sync_2.issubset(primary_keys_sync_1))

                else:

                    # Verify that the 2nd sync with a later start date replicates the same number of
                    # records as the 1st sync.
                    self.assertEqual(record_count_sync_2, record_count_sync_1)

                    # Verify by primary key the same records are replicated in the 1st and 2nd syncs
                    self.assertSetEqual(primary_keys_sync_1, primary_keys_sync_2)
