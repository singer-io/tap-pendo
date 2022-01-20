from tap_tester import connections, runner
from base import TestPendoBase
from datetime import datetime

class PendoChildStreamStartDateTest(TestPendoBase):

    def name(self):
        return "pendo_child_stream_start_date_test"

    def test_run(self):

        streams_to_test = {"features", "feature_events"}

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                      if catalog.get('tap_stream_id') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(conn_id,test_catalogs_all_fields)

        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # check if all streams have collected records
        for stream in streams_to_test:
            self.assertGreater(record_count_by_stream.get(stream, -1), 0,
                msg="failed to replicate any data for stream : {}".format(stream))

        # collect "feature" and "feature_events" data
        features = synced_records.get("features")
        feature_events = synced_records.get("feature_events")

        # find the first feature's id
        first_feature_id = features.get("messages")[0].get("data").get("id")

        first_feature_ids_events = []
        rest_feature_events = []

        # seperate feature events based on feature id
        for feature_event in feature_events.get("messages"):
            if feature_event.get("data").get("feature_id") == first_feature_id:
                first_feature_ids_events.append(feature_event.get("data"))
            else:
                rest_feature_events.append(feature_event.get("data"))

        replication_key_for_feature_events = next(iter(self.expected_replication_keys().get("feature_events")))

        # find the maximun bookmark date for first feature's events
        sorted_first_feature_ids_events = sorted(first_feature_ids_events, key=lambda i: i[replication_key_for_feature_events], reverse=True)
        max_bookmark = sorted_first_feature_ids_events[0].get(replication_key_for_feature_events)

        # used for verifying if we synced feature events before
        # than the maximum bookmark of first feature's events
        synced_older_data = False
        for rest_feature_event in rest_feature_events:
            event_time = datetime.strptime(rest_feature_event.get(replication_key_for_feature_events), "%Y-%m-%dT%H:%M:%S.%fZ")
            max_bookmark_time = datetime.strptime(max_bookmark, "%Y-%m-%dT%H:%M:%S.%fZ")
            if event_time < max_bookmark_time:
                synced_older_data = True
                break

        self.assertTrue(synced_older_data)
