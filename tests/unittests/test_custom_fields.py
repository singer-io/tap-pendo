import unittest
from unittest import mock
from singer import utils, metadata
from singer.utils import strptime_to_utc, strftime
from tap_pendo.discover import build_metadata_metadata, discover_streams
from tap_pendo.streams import Stream


class TestCustomFields(unittest.TestCase):
    @mock.patch("tap_pendo.discover.metadata.write")
    def test_build_account_visitor_metadata(self, mock_metadata_write):
        custom_account_fields = {
            "testaccountcfield1": {
                "type": "boolean",
                "display_name": "testAccountCField1",
                "element_type": "",
                "element_format": "",
                "dirty": True,
                "is_hidden": False,
                "is_deleted": False,
                "is_calculated": False,
                "is_per_app": False,
                "never_index": False
            },
            "testaccountcustomfield": {
                "type": "string",
                "display_name": "test account custom field",
                "element_type": "",
                "element_format": "",
                "dirty": True,
                "is_hidden": False,
                "is_deleted": False,
                "is_calculated": False,
                "is_per_app": False,
                "never_index": False
            }
        }
        custom_visitor_fields = {
            "testcustomfield": {
                "type": "string",
                "display_name": "testCustomField",
                "element_type": "",
                "element_format": "",
                "dirty": True,
                "is_hidden": False,
                "is_deleted": False,
                "is_calculated": False,
                "is_per_app": False,
                "never_index": False
            }
        }
        mdata = {}
        schema = {'properties': {}}
        build_metadata_metadata(mdata, schema, custom_account_fields)
        self.assertEqual(mock_metadata_write.call_count, 2)
        build_metadata_metadata(mdata, schema, custom_visitor_fields)
        self.assertEqual(mock_metadata_write.call_count, 3)
