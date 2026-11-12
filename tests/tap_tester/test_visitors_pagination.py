import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
from base import TestPendoBase


class PendoAllFieldsTest(TestPendoBase):
    start_date = "2019-09-10T00:00:00Z"
    record_limit = 50
    include_anonymous_visitors = False
    def name(self):
        return "pendo_visitors_pagination_test"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return {
            # To reduce the execution time to test this stream taking recently start_date
            "start_date": self.start_date,
            "lookback_window": "1",
            "period": "dayRange",
            "record_limit": self.record_limit,
            "include_anonymous_visitors": self.include_anonymous_visitors
        }

    def test_run(self):
        # Verify that visitors pagination logic work as expected named and anonymous visitors
        # without impacting other stream replication
        # Note: there are 21000+ named and anonymous visitors
        self.run_pagination_test(expected_streams= {"accounts", "features", "feature_events", "visitors"},
                                 start_date="2019-09-10T00:00:00Z",
                                 record_limit=10000,
                                 include_anonymous_visitors="true")

        # Verify with visitors pagination, we are able to sync child stream records i.e. visitor_history
        # Note: there are only 58 named and anonymous visitors but only recently updated visitors will be synced
        self.run_pagination_test(expected_streams={"visitors"},
                                 start_date=self.START_DATE_VISTOR_HISTORY,
                                 record_limit=50,
                                 include_anonymous_visitors="false")


    def run_pagination_test(self, expected_streams, start_date, record_limit, include_anonymous_visitors):
        """
        This is a canary test to verify pagination implementation for the Visitors stream.
        """
        self.streams_to_test = expected_streams
        self.start_date = start_date
        self.record_limit = record_limit
        self.include_anonymous_visitors = include_anonymous_visitors

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields)

        # Grab metadata after performing table-and-field selection to set expectations
        # used for asserting all fields are replicated
        stream_to_all_catalog_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(
                fields_from_field_level_md)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())

        self.assertSetEqual(expected_streams, synced_stream_names)
        for stream in expected_streams:
            with self.subTest(stream=stream):
                self.assertGreaterEqual(record_count_by_stream.get(stream), 1)
