import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
from base import TestPendoBase

# As we can't find the below fields in the docs and also
# it won't be generated by pendo APIs now so expected.
# TDL-22484: Skip missing fields in 'events' and 'visitor_history'
# Remove skipped fields once bug is fixed
MISSING_FILEDS = {"events": {"hour", "feature_id", "parameters"},
                  "guide_events": {"poll_response", "poll_id"},
                  "feature_events": {"hour", "parameters"},
                  "page_events": {"hour"},
                  "track_events": {"hour", "properties"}}
                #  "visitor_history": {"feature_id", "untagged_url"}

class PendoAllFieldsTest(TestPendoBase):
    def name(self):
        return "pendo_all_fields_test"

    def test_run(self):
        self.run_test(self.expected_streams() - {"visitors", "visitor_history"})
        self.run_test({"visitors", "visitor_history"})

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        if self.streams_to_test == {"visitors", "visitor_history"}:
            return_value = {
                # To reduce the execution time to test this stream taking recently start_date
                "start_date": self.START_DATE_VISTOR_HISTORY,
                "lookback_window": "1",
                "period": "dayRange",
            }
            if original:
                return return_value

            return return_value
        else:
            return super().get_properties()

    def run_test(self, expected_streams, start_date=None):
        """
        • Verify no unexpected streams were replicated
        • Verify that more than just the automatic fields are replicated for each stream.
        • verify all fields for each stream are replicated
        """

        self.start_date = start_date
        self.streams_to_test = expected_streams
        self.app_ids = None

        expected_automatic_fields = self.expected_automatic_fields()
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields)

        # grab metadata after performing table-and-field selection to set expectations
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

        self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())

        # Skipping below streams due to zero records for given start date
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # expected values
                expected_all_keys = stream_to_all_catalog_fields[stream]
                expected_automatic_keys = expected_automatic_fields.get(
                    stream, set())

                # collect actual values
                messages = synced_records.get(stream)
                actual_all_keys = [set(message['data'].keys()) for message in messages['messages']
                                   if message['action'] == 'upsert']
                actual_all_keys = set().union(*actual_all_keys)

                # Verify that more than just the automatic fields are replicated for each stream.
                self.assertTrue(expected_automatic_keys.issubset(
                    expected_all_keys), msg='{} is not in "expected_all_keys"'.format(expected_automatic_keys-expected_all_keys))

                expected_all_keys = expected_all_keys - MISSING_FILEDS.get(stream, set())

                # verify all fields for each stream are replicated
                self.assertSetEqual(expected_all_keys, actual_all_keys)
