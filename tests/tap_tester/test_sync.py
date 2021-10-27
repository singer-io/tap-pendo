import unittest
from datetime import datetime as dt

from datetime import timedelta
import os
from tap_tester import menagerie
import tap_tester.runner as runner
import tap_tester.connections as connections


class TestSyncNonReportStreams(unittest.TestCase):
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"

    """ Test the non-report streams """

    @staticmethod
    def name():
        return "test_sync"

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-pendo"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.pendo"

    def expected_check_streams(self):
        return set(self.expected_pks().keys())

    def expected_sync_streams(self):
        return set(self.expected_pks().keys())

    @staticmethod
    def expected_pks():
        return {
            "accounts": {"account_id"},
            "features": {"id"},
            "guides": {"id"},
            "pages": {"id"},
            # Add back when visitor_history stream causing this test to take
            # 4+ hours is solved, tracked in this JIRA:
            # https://stitchdata.atlassian.net/browse/SRCE-4755
            # "visitor_history": {"visitor_id"},

            "visitors": {"visitor_id"},
            "track_types": {"id"},
            "feature_events": {"visitor_id", "account_id", "server", "remote_ip"},
            "events": {"visitor_id", "account_id", "server", "remote_ip"},
            "page_events": {"visitor_id", "account_id", "server", "remote_ip"},
            "guide_events": {"visitor_id", "account_id", "server_name", "remote_ip"},
            "poll_events": {"visitor_id", "account_id", "server_name", "remote_ip"},
            "track_events": {"visitor_id", "account_id", "server", "remote_ip"},
            "metadata_accounts": {},
            "metadata_visitors": {},
        }

    def get_properties(self):
        return {
            "start_date": self.get_start_date(),
            "lookback_window": "1",
            "period": "dayRange",
        }

    def get_start_date(self):
        if not hasattr(self, 'start_date'):
            # updated start date as the tap will collect only records
            # modified after the start date rather than syncing all records
            self.start_date = "2020-09-10T00:00:00Z"

        return self.start_date

    @staticmethod
    def get_credentials():
        return {
            "x_pendo_integration_key": os.getenv("TAP_PENDO_INTEGRATION_KEY")
        }

    def setUp(self):
        missing_envs = [x for x in [
            "TAP_PENDO_INTEGRATION_KEY",
        ] if os.getenv(x) is None]

        if missing_envs:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    def test_run(self):

        conn_id = connections.ensure_connection(self, payload_hook=None)

        # Run the tap in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify the check's exit status
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify that there are catalogs found
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_check_streams().issubset(found_catalog_names)
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog, extra streams={}".format(self.expected_check_streams().difference(found_catalog_names)))
        #
        # # Select some catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]
        for catalog in our_catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, schema, [], [])

        # # Verify that all streams sync at least one row for initial sync
        # # This test is also verifying access token expiration handling. If test fails with
        # # authentication error, refresh token was not replaced after expiring.
        menagerie.set_state(conn_id, {})
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(),
                                                                   self.expected_pks())

        # Verify that all streams sync at least one row for initial sync
        for stream in self.expected_sync_streams().difference({
                'feature_events',
                'events',
                'page_events',
                'guide_events',
                'poll_events',
                'track_events',
                'track_types',
        }):
            with self.subTest(stream=stream):
                self.assertLess(0, record_count_by_stream[stream])

        # TODO run the remaining assertions against all incremental streams

        # Verify that bookmark values are correct after incremental sync
        start_date = self.get_properties()['start_date']
        current_state = menagerie.get_state(conn_id)
        test_bookmark = current_state['bookmarks']['accounts']

        # Verify a bookmark is present for accounts
        self.assertIn('bookmarks', current_state.keys())
        self.assertIn('accounts', current_state['bookmarks'].keys())

        # # BUG | https://jira.talendforge.org/browse/TDL-13470
        # # Verify the bookmarked value is correct after incremental sync for accounts
        # self.assertGreater(test_bookmark['lastupdated'], start_date)
