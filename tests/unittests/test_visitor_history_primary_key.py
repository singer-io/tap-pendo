import unittest
from tap_pendo.streams import VisitorHistory


def page_entry(**overrides):
    # Shaped like a decamelized response entry, with 'visitorId' as sync() sets it
    record = {
        "visitorId": "visitor-1",
        "type": "page",
        "uri": "https://app.pendo.io/api/s/1/page/page-1",
        "parsed_user_agent": {"name": "Chrome", "version": "120.0.0", "os": "Mac OS X"},
        "app_id": -323232,
        "page_id": "page-1",
        "ts": 1784377140000,
        "last_ts": 1784377200000,
        "duration": 60000,
    }
    record.update(overrides)
    return record


class TestVisitorHistoryPrimaryKey(unittest.TestCase):

    def setUp(self):
        self.stream = VisitorHistory({})

    def test_primary_key(self):
        self.assertEqual(self.stream.key_properties, ['_sdc_record_hash'])

    def test_record_hash_in_schema(self):
        self.assertIn('_sdc_record_hash', self.stream.load_schema()['properties'])

    def test_transform_adds_record_hash(self):
        record = self.stream.transform(page_entry())
        self.assertEqual(record['visitor_id'], 'visitor-1')
        self.assertEqual(len(record['_sdc_record_hash']), 64)
        self.assertEqual(record['modified_ts'], 1784377200000)

    def test_record_hash_ignores_fields_that_grow(self):
        """A page entry that is still open has a larger last_ts and duration on the next sync."""
        first = self.stream.transform(page_entry())
        later = self.stream.transform(page_entry(last_ts=1784377320000, duration=180000))
        self.assertEqual(first['_sdc_record_hash'], later['_sdc_record_hash'])

    def test_record_hash_differs_for_different_entries(self):
        base = self.stream.transform(page_entry())['_sdc_record_hash']
        variants = [
            page_entry(visitorId="visitor-2"),
            page_entry(ts=1784377200000),
            page_entry(page_id="page-2", uri="https://app.pendo.io/api/s/1/page/page-2"),
            page_entry(parsed_user_agent={"name": "Safari", "version": "17.0", "os": "iOS"}),
            page_entry(app_id=12345),
            page_entry(type="untagged", page_id=None, uri=None, untagged_url="https://example.com/a"),
        ]
        ids = {self.stream.transform(v)['_sdc_record_hash'] for v in variants}
        self.assertEqual(len(ids), len(variants))
        self.assertNotIn(base, ids)

    def test_guide_steps_get_distinct_ids(self):
        def guide_entry(step_id):
            # Guide entries have no last_ts, duration or page_id, as the API returns them
            entry = page_entry(type="guide", uri=None, guide_id="guide-1", guide_step_id=step_id)
            for field in ('last_ts', 'duration', 'page_id'):
                del entry[field]
            return entry
        one = self.stream.transform(guide_entry("step-1"))
        two = self.stream.transform(guide_entry("step-2"))
        self.assertNotEqual(one['_sdc_record_hash'], two['_sdc_record_hash'])
