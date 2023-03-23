import unittest
from unittest import mock
import humps

from parameterized import parameterized
from tap_pendo.streams import *

default_config = {
        "record_limit": 10,
        "request_timeout": 10,
        "period": "hourRage"
    }

default_kwargs = {"key_id": "key_id", "period": "hour", "first": 1}


class TestPendoParentStreams(unittest.TestCase):
    last_processed_index = 0
    test_records = []
    record_limit = 0

    def generate_records(self, stream_obj, num_records, num_same_replication_value=0):
        test_records = []
        offset = 1000000
        replication_key = humps.decamelize(stream_obj.replication_key)

        if isinstance(stream_obj, Accounts):
            test_records = [{"appID": 10000, "metadata": {"auto": {"lastupdated": offset+i}}} for i in range(num_records)]
            for i in range(num_records, num_records+num_same_replication_value):
                test_records.append({"appID": 10000, "metadata": {"auto": {"lastupdated": offset+num_records+10}}})
        else:
            test_records = [{"appID": 10000, replication_key: offset+i} for i in range(num_records)]
            for i in range(num_records, num_records+num_same_replication_value):
                test_records.append({"id": 10000, replication_key: offset+num_records})

        return test_records

    @parameterized.expand(
        [(Accounts, "accounts", None, "metadata.auto.lastupdated", None),
        (Features, "features", None, "lastUpdatedAt", None),
        (TrackTypes, "trackTypes", None, "lastUpdatedAt", None),
        (Guides, "guides", None, "lastUpdatedAt", None),
        (Pages, "pages", None, "lastUpdatedAt", None),
        (Events, "events", None, "hour", default_kwargs),
        (GuideEvents, "guideEvents", {"guideId": "key_id",}, "browser_time", default_kwargs),
        (FeatureEvents, "featureEvents", {"featureId": "key_id",}, "hour", default_kwargs),
        (PollEvents, "pollEvents", None, "browser_time", default_kwargs),
        (TrackEvents, "trackEvents", {"trackTypeId": "key_id",}, "hour", default_kwargs)])
    def test_get_body(self, stream_class, exp_event_type, exp_event_type_value, exp_filter_key, kwargs):
        """Verify get_body method returns an expected request body object"""
        stream_obj = stream_class(default_config)
        # body = stream_obj.get_body()
        body = stream_obj.get_body(**kwargs) if kwargs else stream_obj.get_body()
        sort_index = stream_obj.get_pipeline_key_index(body, "sort")
        filter_index = stream_obj.get_pipeline_key_index(body, "filter")
        limit_index = stream_obj.get_pipeline_key_index(body, "limit")

        self.assertIn(exp_event_type, body["request"]["pipeline"][0]["source"])

        if kwargs:
            self.assertIn("timeSeries", body["request"]["pipeline"][0]["source"])

        self.assertEqual(body["request"]["pipeline"][0]["source"][exp_event_type], exp_event_type_value)
        self.assertEqual(body["request"]["pipeline"][sort_index]["sort"], [humps.camelize(exp_filter_key)])
        self.assertEqual(body["request"]["pipeline"][limit_index]["limit"], stream_obj.record_limit)

        if filter_index:
            self.assertEqual(body["request"]["pipeline"][filter_index]["filter"], f"{humps.camelize(exp_filter_key)}>=1")

    @parameterized.expand(
        [(Accounts, "metadata.auto.lastupdated", None),
        (Features, "lastUpdatedAt", None),
        (TrackTypes,"lastUpdatedAt", None),
        (Events, "hour", default_kwargs),
        (PollEvents, "browserTime", default_kwargs),
        (GuideEvents, "browserTime", default_kwargs)])
    def test_set_request_body_filters(self, stream_class, exp_filter_key, kwargs):
        """Verify set_request_body_filters method sets filter parameter correctly in request body object"""
        stream_obj = stream_class(default_config)
        body = stream_obj.get_body(**kwargs) if kwargs else stream_obj.get_body()
        test_records = self.generate_records(stream_obj, 10)
        body = stream_obj.set_request_body_filters(body=body,
                                                   start_time=100000,
                                                   records=test_records)

        self.assertEqual(body, body)
        filter_index = stream_obj.get_pipeline_key_index(body, "filter")
        camelized_replication_key = humps.camelize(exp_filter_key)
        decamelized_replication_key = humps.decamelize(exp_filter_key)

        if isinstance(stream_obj, Accounts):
            replication_key_value = test_records[-1]["metadata"]["auto"]["lastupdated"]
        else:
            replication_key_value = test_records[-1][decamelized_replication_key]

        if filter_index:
            self.assertEqual(body["request"]["pipeline"][filter_index]["filter"],
                            f"{camelized_replication_key}>={replication_key_value}")


    @parameterized.expand(
        [(Accounts,),
        (Features,),
        (TrackTypes,),
        (Events,),
        (PollEvents,),
        (GuideEvents,)])
    def test_remove_last_timestamp_records(self, stream_class):
        """ Verify response records are manipulated/removed as expected """
        stream_obj = stream_class(default_config)
        record_limit = stream_obj.record_limit  # original record limit value

        # Verify if records have distinct replication key value then only one record is removed
        test_records = self.generate_records(stream_obj, 10)
        self.assertEqual(len(stream_obj.remove_last_timestamp_records(test_records)), 1)
        self.assertEqual(stream_obj.record_limit, record_limit)

        # Verify if records have 'nn distinct replication key value then 'n' record is removed
        test_records = self.generate_records(stream_obj, 10, 5)
        self.assertEqual(len(stream_obj.remove_last_timestamp_records(test_records)), 5)
        self.assertEqual(stream_obj.record_limit, record_limit)

        # Verify if all records have equal replication key value then record limit is set to default
        test_records = self.generate_records(stream_obj, 0, record_limit)
        self.assertEqual(len(stream_obj.remove_last_timestamp_records(test_records)), record_limit)
        self.assertEqual(stream_obj.record_limit, API_RECORD_LIMIT)
