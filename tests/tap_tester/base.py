import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

import dateutil.parser
import pytz

import tap_tester.connections as connections
import tap_tester.runner as runner
from tap_tester import menagerie


class TestPendoBase(unittest.TestCase):
    
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    BOOKMARK_COMPARISON_FORMAT = "%Y-%m-%dT%H:%M%S%z"
    start_date = ""
    is_day_range = True
    
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
    
    def expected_metadata(self):
        """The expected streams and metadata about the streams"""
        return {
            "accounts": {
                self.PRIMARY_KEYS: {'account_id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'lastupdated'}
            },
            "features": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'last_updated_at'}
            },
            "guides": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'last_updated_at'}
            },
            "pages": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'last_updated_at'}
            },
            # Add back when visitor_history stream causing this test to take
            # 4+ hours is solved, tracked in this JIRA:
            # https://stitchdata.atlassian.net/browse/SRCE-4755
            # "visitor_history": {
            #     self.PRIMARY_KEYS: {'visitor_id'},
            #     self.REPLICATION_METHOD: self.INCREMENTAL,
            #     self.REPLICATION_KEYS: {'modified_ts'}
            # },

            "visitors": {
                self.PRIMARY_KEYS: {'visitor_id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'lastupdated'}
            },
            "track_types": {
                self.PRIMARY_KEYS: {'id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'last_updated_at'}
            },
            "feature_events":{
                self.PRIMARY_KEYS: {"feature_id", "visitor_id", "account_id", "server", "remote_ip", "user_agent", "day" if self.is_day_range else "hour"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'day' if self.is_day_range else 'hour'}
            },
            "events": {
                self.PRIMARY_KEYS: {"visitor_id", "account_id", "server", "remote_ip"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'day' if self.is_day_range else 'hour'}
            },
            "page_events": {
                self.PRIMARY_KEYS: {"visitor_id", "account_id", "server", "remote_ip"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'day' if self.is_day_range else 'hour'}
            },
            "guide_events": {
                self.PRIMARY_KEYS: {"visitor_id", "account_id", "server_name", "remote_ip"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'browser_time'}
            },
            "poll_events":{
                self.PRIMARY_KEYS: {"visitor_id", "account_id", "server_name", "remote_ip"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'browser_time'}
            },
            "track_events": {
                self.PRIMARY_KEYS: {"visitor_id", "account_id", "server", "remote_ip"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'day' if self.is_day_range else 'hour'}
            },
            "metadata_accounts": {
                self.REPLICATION_METHOD: self.FULL_TABLE,
            },
            "metadata_visitors": {
                self.REPLICATION_METHOD: self.FULL_TABLE,
            },
        }
        
    def setUp(self):
        missing_envs = [x for x in [
            "TAP_PENDO_INTEGRATION_KEY",
        ] if os.getenv(x) is None]

        if missing_envs:
            raise Exception("Missing environment variables: {}".format(missing_envs))
        
    @staticmethod
    def get_credentials():
        """Authentication information for the test account"""
        return {
            "x_pendo_integration_key": os.getenv("TAP_PENDO_INTEGRATION_KEY")
        }
        
    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            "start_date": "2020-09-10T00:00:00Z",
            "lookback_window": "1",
            "period": "dayRange" if self.is_day_range else "hourRange",
        }
        if original:
            return return_value
        
        return_value["start_date"] = self.start_date
        return return_value
    
    
    def expected_streams(self):
        """A set of expected stream names"""
    
        return set(self.expected_metadata().keys())
    
    def expected_pks(self):
        """return a dictionary with key of table name and value as a set of primary key fields"""
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """return a dictionary with key of table name and value as a set of replication key fields"""
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_method(self):
        """return a dictionary with key of table name nd value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def expected_automatic_fields(self):
        """return a dictionary with key of table name and value as a set of automatic key fields"""
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set()) \
                | v.get(self.FOREIGN_KEYS, set())
        return auto_fields
    

    #########################
    #   Helper Methods      #
    #########################

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.
        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(
            found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(
            map(lambda c: c['stream_name'], found_catalogs))

        subset = self.expected_streams().issubset(found_catalog_names)
        self.assertTrue(
            subset, msg="Expected check streams are not subset of discovered catalog")
        print("discovered schemas are OK")

        return found_catalogs
    
    def run_and_verify_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_pks())
        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        print("total replicated row count: {}".format(
            sum(sync_record_count.values())))

        return sync_record_count

    def perform_and_verify_table_and_field_selection(self, conn_id, test_catalogs, select_all_fields=True):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters.
        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(
            conn_id, test_catalogs, select_all_fields)

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('stream_name') for tc in test_catalogs]

        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(
                conn_id, cat['stream_id'])

            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(
                cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected:
                self.assertFalse(
                    selected, msg="Stream selected, but not testable.")
                continue  # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    print("\tValidating selection on {}.{}: {}".format(
                        cat['stream_name'], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(
                    cat['stream_name'])
                selected_fields = self.get_selected_fields_from_metadata(
                    catalog_entry['metadata'])
                self.assertEqual(expected_automatic_fields, selected_fields)
                
    def get_selected_fields_from_metadata(self, metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            
            inclusion_automatic_or_selected = (
                field['metadata'].get('selected') is True or
                field['metadata'].get('inclusion') == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields

    def select_all_streams_and_fields(self, conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(
                conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)
            
    def calculated_states_by_stream(self, current_state):
        timedelta_by_stream = {stream: [0,0,0,5]  # {stream_name: [days, hours, minutes, seconds], ...}
                               for stream in self.expected_streams()}
        
        stream_to_calculated_state = {stream: "" for stream in current_state['bookmarks'].keys()}
        for stream, state in current_state['bookmarks'].items():
            state_key, state_value = next(iter(state.keys())), next(iter(state.values()))
            state_as_datetime = dateutil.parser.parse(state_value)

            days, hours, minutes, seconds = timedelta_by_stream[stream]
            calculated_state_as_datetime = state_as_datetime - timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

            state_format = '%Y-%m-%dT%H:%M:%S-00:00'
            calculated_state_formatted = dt.strftime(calculated_state_as_datetime, state_format)

            stream_to_calculated_state[stream] = {state_key: calculated_state_formatted}

        return stream_to_calculated_state
    
    def parse_date(self, date_value):
        """
        Pass in string-formatted-datetime, parse the value, and return it as an unformatted datetime object.
        """
        date_formats = {
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f+00:00",
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%d"
        }
        for date_format in date_formats:
            try:
                date_stripped = dt.strptime(date_value, date_format)
                return date_stripped
            except ValueError:
                continue

        raise NotImplementedError(
            "Tests do not account for dates of this format: {}".format(date_value))
    
    ##########################################################################
    # Tap Specific Methods
    ##########################################################################

    def convert_state_to_utc(self, date_str):
        """
        Convert a saved bookmark value of the form '2020-08-25T13:17:36-07:00' to
        a string formatted utc datetime,
        in order to compare aginast json formatted datetime values
        """
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return dt.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")
    
    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, "%Y-%m-%dT%H:%M:%SZ")
            return_date = date_stripped + timedelta(days=days)

            return dt.strftime(return_date, "%Y-%m-%dT%H:%M:%SZ")

        except ValueError:
            try:
                date_stripped = dt.strptime(dtime, self.BOOKMARK_COMPARISON_FORMAT)
                return_date = date_stripped + timedelta(days=days)

                return dt.strftime(return_date, self.BOOKMARK_COMPARISON_FORMAT)

            except ValueError:
                return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))
            
    def is_incremental(self, stream):
        return self.expected_metadata().get(stream).get(self.REPLICATION_METHOD) == self.INCREMENTAL
    
    def is_event(self, stream):
        return stream.endswith('events')