import json
import os
import unittest
from unittest import mock

from singer import Transformer, metadata

import tap_pendo
from tap_pendo.discover import build_account_visitor_metadata, discover_streams
from tap_pendo.streams import STREAMS

SCHEMAS_DIR = os.path.join(os.path.dirname(tap_pendo.__file__), 'schemas')


def load_schema(name):
    with open(os.path.join(SCHEMAS_DIR, name + '.json')) as schema_file:
        return json.load(schema_file)


class TestAgentFields(unittest.TestCase):
    agent_fields = {
        "trial_status": {"type": "string"},
        "licensedays": {"type": "integer"},
        "firstvisit": {"type": "time"},
        "stp": {"type": "boolean"},
        "tags": {"type": "list"},
    }

    def test_agent_fields_added_to_accounts_schema(self):
        schema = load_schema('accounts')
        build_account_visitor_metadata({}, schema, {}, {'agent': self.agent_fields})

        agent = schema['properties']['metadata_agent']
        self.assertEqual(agent['properties']['trial_status'], {"type": ["null", "string"]})
        self.assertEqual(agent['properties']['firstvisit'],
                         {"type": ["null", "string"], "format": "date-time"})
        self.assertEqual(agent['properties']['stp'], {"type": ["null", "boolean"]})
        # unknown types must not fail discovery
        self.assertEqual(agent['properties']['tags'], {})

    def test_agent_fields_are_merged_into_visitors_schema(self):
        schema = load_schema('visitors')
        build_account_visitor_metadata({}, schema, {}, {'agent': {"role": {"type": "string"}}})

        properties = schema['properties']['metadata_agent']['properties']
        self.assertEqual(properties['role'], {"type": ["null", "string"]})
        # statically declared agent fields are preserved
        self.assertIn('email', properties)

    def test_agent_fields_are_not_removed_by_transformer(self):
        schema = load_schema('accounts')
        build_account_visitor_metadata({}, schema, {}, {'agent': self.agent_fields})

        record = {"account_id": "a1", "metadata_agent": {"trial_status": "active", "licensedays": 5}}
        with Transformer() as transformer:
            transformed = transformer.transform(record, schema)
        self.assertEqual(transformed['metadata_agent'], {"trial_status": "active", "licensedays": 5})

    def test_accounts_transform_keeps_agent_metadata(self):
        record = {"accountId": "a1", "metadata": {"auto": {"lastupdated": 1}, "agent": {"locale": "en"}}}
        transformed = STREAMS['accounts']({"app_ids": []}).transform(record)
        self.assertEqual(transformed['metadata_agent'], {"locale": "en"})

    def test_poll_events_schema_has_guide_step_id_and_url(self):
        properties = load_schema('poll_events')['properties']
        self.assertEqual(properties['guide_step_id'], {"type": ["null", "string"]})
        self.assertEqual(properties['url'], {"type": ["null", "string"]})

    @mock.patch('tap_pendo.discover._apply_access_checks')
    @mock.patch('tap_pendo.streams.MetadataVisitors.get_fields')
    @mock.patch('tap_pendo.streams.MetadataAccounts.get_fields')
    def test_discover_adds_agent_and_custom_fields(self, mock_accounts, mock_visitors, mock_access):
        mock_accounts.return_value = {
            "agent": {"trial_status": {"type": "string"}},
            "custom": {"accountindustry": {"type": "string"}},
        }
        mock_visitors.return_value = {"agent": {"role": {"type": "string"}}, "auto": {"newautofield": {"type": "string"}},
                                      "pendo": {"designerenabled": {"type": "boolean"}}}
        mock_access.return_value = ({'accounts', 'metadata_accounts', 'visitors', 'metadata_visitors'}, set())

        catalog = {s['tap_stream_id']: s for s in discover_streams({"app_ids": []})}

        accounts = catalog['accounts']['schema']['properties']
        self.assertIn('trial_status', accounts['metadata_agent']['properties'])
        self.assertIn('accountindustry', accounts['metadata_custom']['properties'])
        visitors = catalog['visitors']['schema']['properties']
        self.assertIn('role', visitors['metadata_agent']['properties'])
        self.assertEqual(visitors['metadata_pendo']['properties']['designerenabled'],
                         {"type": ["null", "boolean"]})
        # 'auto' is declared statically and is not extended by discovery
        self.assertNotIn('newautofield', visitors['metadata_auto']['properties'])
        mdata = metadata.to_map(catalog['accounts']['metadata'])
        self.assertEqual(mdata[('properties', 'metadata_agent')]['inclusion'], 'available')
