import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
from base import TestPendoBase


class PendoAllFieldsTest(TestPendoBase):
    def name(self):
        return "pendo_all_fields_test"

    def test_run(self):
        self.run_test({'accounts', 'events', 'feature_events', 'features', 'guide_events', 'guides', 'page_events', 'pages', 'poll_events', 'track_events', 'track_types'})
        self.run_test({"visitors", "visitor_history"}, start_date="2022-06-20T00:00:00Z")

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

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            "start_date": "2019-09-10T00:00:00Z",
            "lookback_window": "1",
            "period": "dayRange",
        }

        if self.streams == {"visitors", "visitor_history"}:
            return_value["start_date"] = self.start_date

        if original:
            return return_value

        return return_value

    def run_test(self, expected_streams, start_date=None):
        """
        • Verify no unexpected streams were replicated
        • Verify that more than just the automatic fields are replicated for each stream. 
        • verify all fields for each stream are replicated
        """
        
        self.start_date = start_date
        self.streams = expected_streams
        
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

        actual_fields_by_stream = runner.examine_target_output_for_fields()

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
                                
                # As we can't find the below fields in the docs and also
                # it won't be generated by pendo APIs now so expected.
                if stream == "feature_events" or stream == "page_events":
                    expected_all_keys = expected_all_keys - {'hour'}
                elif stream == "events":
                    expected_all_keys = expected_all_keys - {'hour', "feature_id"}
                elif stream == "track_events":
                    expected_all_keys = expected_all_keys - {'hour', "properties"}
                elif stream == "guide_events":
                    expected_all_keys = expected_all_keys - {'poll_response', "poll_id"}
                # elif stream == "features":
                #     expected_all_keys = expected_all_keys - {'page_id'}
                # elif stream == "guides":
                #     expected_all_keys = expected_all_keys - {'audience'}
                    
                # verify all fields for each stream are replicated
                self.assertSetEqual(expected_all_keys, actual_all_keys)
