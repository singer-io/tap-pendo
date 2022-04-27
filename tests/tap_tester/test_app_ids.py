import tap_tester.connections as connections
import tap_tester.runner as runner
from base import TestPendoBase

class PendoAPPIDSTest(TestPendoBase):
    """Instantiate app_ids with value and None then run the test"""

    def get_properties(self, *args, **kwargs):
        props = super().get_properties(*args, **kwargs)
        props.pop('lookback_window')
        return props

    app_ids_1 = ""
    app_ids_2 = ""

    def name(self):
        return "pendo_app_ids_test"

    def test_run(self):
        self.run_test("-323232", None, {"features", "feature_events", "pages", "page_events", "events", "guides", "guide_events", "track_types", "track_events"})

    def run_test(self, app_ids_1, app_ids_2, streams):
        """
        Test that the app_ids configuration is respected
        • verify that a sync with app_ids have one app's data 
        • verify that a sync with None app_ids get more than one app's data
        """
        
        self.app_ids = app_ids_1
        self.start_date = "2020-09-10T00:00:00Z"

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
        synced_records_1 = runner.get_records_from_target_output()
        
        for stream in expected_streams:
            messages_1 = synced_records_1.get(stream)
            records_appid_set = set([message.get('data').get('app_id') for message in messages_1.get("messages")])
            self.assertEqual(len(records_appid_set), 1, msg=f"We are getting more than one app's records for {stream}")
        

        ##########################################################################
        # Update APP IDs Between Syncs
        ##########################################################################
        
        print("Start syncing with no app_ids in config.json")
        # for no app_ids given None value in self.app_ids
        self.app_ids = app_ids_2
        

        # ##########################################################################
        # # Second Sync
        # ##########################################################################

        # create a new connection with the new config where app_ids are not given
        conn_id_2 = connections.ensure_connection(
            self, original_properties=False)

        # run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs_2
                                      if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(
            conn_id_2, test_catalogs_2_all_fields, select_all_fields=True)

        # run sync
        synced_records_2 = runner.get_records_from_target_output()
        
        for stream in expected_streams:
            messages_2 = synced_records_2.get(stream)
            records_appid_set = set([message.get('data').get('app_id') for message in messages_2.get("messages")])
            self.assertGreater(len(records_appid_set), 1, msg=f"We have only one app's records for {stream}")
