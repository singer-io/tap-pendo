from tap_tester import connections, runner
from base import TestPendoBase
from datetime import datetime

class PendoChildStreamStartDateTest(TestPendoBase):

    def name(self):
        return "pendo_child_stream_start_date_test"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            "start_date": "2020-09-10T00:00:00Z",
            "lookback_window": "1",
            "period": "dayRange",
            "record_limit": 1000
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    def test_run(self):

        streams_to_test = {"guides", "guide_events"}

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

        # collect "guide" and "guide_events" data
        guides = synced_records.get("guides")
        guide_events = synced_records.get("guide_events")

        # find the first guide's id
        for guide in guides.get("messages"):
            guide_id = guide.get("data").get("id")
            guide_id_events = []
            rest_guide_events = []

            # seperate guide events based on guide id
            for guide_event in guide_events.get("messages"):
                if guide_event.get("data").get("guide_id") == guide_id:
                    guide_id_events.append(guide_event.get("data"))
                else:
                    rest_guide_events.append(guide_event.get("data"))

            if len(guide_id_events) > 0:
                break

        replication_key_for_guide_events = next(iter(self.expected_replication_keys().get("guide_events")))

        # find the maximun bookmark date for first guide's events
        sorted_guide_ids_events = sorted(guide_id_events, key=lambda i: i[replication_key_for_guide_events], reverse=True)
        max_bookmark = sorted_guide_ids_events[0].get(replication_key_for_guide_events)

        # used for verifying if we synced guide events before
        # than the maximum bookmark of first guide's events
        synced_older_data = False
        for rest_guide_event in rest_guide_events:
            event_time = datetime.strptime(rest_guide_event.get(replication_key_for_guide_events), "%Y-%m-%dT%H:%M:%S.%fZ")
            max_bookmark_time = datetime.strptime(max_bookmark, "%Y-%m-%dT%H:%M:%S.%fZ")
            if event_time < max_bookmark_time:
                synced_older_data = True
                break

        self.assertTrue(synced_older_data)
