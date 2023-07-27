import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
from base import TestPendoBase


class PendoMultiAppIdsTest(TestPendoBase):
    def name(self):
        return "pendo_multi_app_ids_test"

    def test_run(self):
        expected_streams = {"features", "pages", "events", "guides", "track_types", "track_events"}
        self.run_test(expected_streams=expected_streams, app_ids="-323232", is_multi_apps=False)
        self.run_test(expected_streams=expected_streams, app_ids=None, is_multi_apps=True)
        self.run_test(expected_streams={"visitors", "visitor_history"}, app_ids=None, is_multi_apps=True)

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

    def run_test(self, expected_streams, app_ids, is_multi_apps, start_date=None):
        """
        - Verify tap syncs records for multiple app_ids if no app_ids provided
        - Verify tap syncs records for specific app_id if single app_id is provided
        """

        self.start_date = start_date
        self.streams_to_test = expected_streams
        self.app_ids = app_ids

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

        for stream in expected_streams:
            # below four streams are independent of the app_id
            if stream in ["accounts", "visitors", "metadata_accounts", "metadata_visitors"]:
                continue

            with self.subTest(stream=stream):
                records_appid_set = set([message.get('data').get('app_id') for message in synced_records.get(stream).get("messages")])
                self.assertEqual(len(records_appid_set)>1, is_multi_apps)
