#!/usr/bin/env python3
import os
import json
import singer
import backoff
import requests
from requests.exceptions import HTTPError
from singer import metadata, utils
from tap_pendo import utils as tap_pendo_utils

KEY_PROPERTIES = ['id']
BASE_URL = "https://app.pendo.io"

endpoints = {
    "account": {
        "method": "GET",
        "endpoint": "/api/v1/account/{accountId}"
    },
    "accounts": {
        "method": "POST",
        "endpoint": "/api/v1/aggregation",
        "data": {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "name": "all-accounts",
                "pipeline": [
                    {
                        "source": {
                            "accounts": None
                        }
                    }
                ],
                "requestId": "all-accounts"
            }
        }
    },
    "features": {
        "method": "GET",
        "endpoint": "/api/v1/feature"
    },
    "guides": {
        "method": "GET",
        "endpoint": "/api/v1/guide"
    },
    "metadata_account": {
        "method": "GET",
        "endpoint": "/api/v1/metadata/schema/account"
    },
    "metadata_visitor": {
        "method": "GET",
        "endpoint": "/api/v1/metadata/schema/visitor"
    },
    "pages": {
        "method": "GET",
        "endpoint": "/api/v1/page"
    },
    "reports": {
        "method": "GET",
        "endpoint": "/api/v1/report"
    },
    "visitor": {
        "method": "GET",
        "endpoint": "/api/v1/visitor/{visitorId}"
    },
    "visitors": {
        "method": "POST",
        "endpoint": "/api/v1/aggregation",
        "data": {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "name": "all-visitors",
                "pipeline": [
                    {
                        "source": {
                            "visitors": None
                        }
                    }
                ],
                "requestId": "all-visitors"
            }
        }
    },
    "visitor_history": {
        "method": "GET",
        "endpoint": "/api/v1/visitor/{visitorId}/history",
        "headers": {
            'content-type': 'application/x-www-form-urlencoded'
        },
        "params": {
            "starttime": "1564056526000"
        }
    },
}

LOGGER = singer.get_logger()
session = requests.Session()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_url(endpoint, **kwargs):
    return BASE_URL + endpoints[endpoint]['endpoint'].format(**kwargs)


def get_method(endpoint):
    return endpoints[endpoint]['method']


def get_data(endpoint):
    return endpoints[endpoint].get('data', {})


def get_headers(endpoint):
    return endpoints[endpoint].get('headers', {})


def get_params(endpoint):
    return endpoints[endpoint].get('params', {})


class Stream():
    name = None
    replication_method = None
    replication_key = None
    key_properties = KEY_PROPERTIES
    stream = None

    def __init__(self, config=None):
        self.config = config

    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.RequestException),
                          max_tries=5,
                          giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                          factor=2)
    @tap_pendo_utils.ratelimit(1, 2)
    def request(self, endpoint, **kwargs):
        # params = params or {}
        headers = {
            'x-pendo-integration-key': self.config['x_pendo_integration_key'],
            'content-type': 'application/json'
        }

        request_kwargs = {
            'url': get_url(endpoint, **kwargs),
            'method': get_method(endpoint),
            'headers': headers,
            'params': get_params(endpoint)
        }

        headers = get_headers(endpoint)
        if headers:
            request_kwargs['headers'].update(headers)

        data = get_data(endpoint)
        if data:
            request_kwargs['data'] = json.dumps(data)

        req = requests.Request(**request_kwargs).prepare()
        LOGGER.info("{} {} {} {}".format(
            request_kwargs['method'],
            request_kwargs['url'],
            request_kwargs['headers'],
            request_kwargs['params'],
            request_kwargs.get('data', None)
        ))
        resp = session.send(req)

        if 'Too Many Requests' in resp.reason:
            # retry_after = int(resp.headers['Retry-After'])
            retry_after = 30
            LOGGER.info(
                "Rate limit reached. Sleeping for {} seconds".format(retry_after))
            time.sleep(retry_after)
            return request(url, params)

        resp.raise_for_status()

        return resp

    def get_bookmark(self, state):
        return utils.strptime_with_tz(singer.get_bookmark(state, self.name, self.replication_key))

    def update_bookmark(self, state, value):
        current_bookmark = self.get_bookmark(state)
        if value and utils.strptime_with_tz(value) > current_bookmark:
            singer.write_bookmark(
                state, self.name, self.replication_key, value)

    def load_schema(self):
        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)
        return self._add_custom_fields(schema)

    def _add_custom_fields(self, schema):  # pylint: disable=no-self-use
        return schema

    def load_metadata(self):
        schema = self.load_schema()
        mdata = metadata.new()

        mdata = metadata.write(
            mdata, (), 'table-key-properties', self.key_properties)
        mdata = metadata.write(
            mdata, (), 'forced-replication-method', self.replication_method)

        if self.replication_key:
            mdata = metadata.write(
                mdata, (), 'valid-replication-keys', [self.replication_key])

        for field_name in schema['properties'].keys():
            if field_name in self.key_properties or field_name == self.replication_key:
                mdata = metadata.write(
                    mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(
                    mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)

    def is_selected(self):
        return self.stream is not None


class Accounts(Stream):
    name = "accounts"
    replication_method = "INCREMENTAL"
    replication_key = "metadata.auto.lastupdated"

    def sync(self, state):
        # bookmark = self.get_bookmark(state)
        accounts = self.request(self.name).json()['results']
        for account in accounts:
            yield (self.stream, account)


class Features(Stream):
    name = "features"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def sync(self, state):
        # bookmark = self.get_bookmark(state)
        features = self.request(self.name).json()
        for feature in features:
            yield (self.stream, feature)


class Guides(Stream):
    name = "guides"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def sync(self, state):
        # bookmark = self.get_bookmark(state)
        guides = self.request(self.name).json()
        for guide in guides:
            yield (self.stream, guide)


class Pages(Stream):
    name = "pages"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def sync(self, state):
        # bookmark = self.get_bookmark(state)
        pages = self.request(self.name).json()
        for page in pages:
            yield (self.stream, page)


class Reports(Stream):
    name = "reports"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def sync(self, state):
        # bookmark = self.get_bookmark(state)
        reports = self.request(self.name).json()
        for report in reports:
            yield (self.stream, report)


class VisitorHistory(Stream):
    name = "visitor_history"
    replication_method = "INCREMENTAL"
    replication_key = "ts"

    def sync(self, state, visitor_id):
        # bookmark = self.get_bookmark(state)
        visitor_history = self.request(
            endpoint=self.name, visitorId=visitor_id).json()
        for visit in visitor_history:
            visit['visitorId'] = visitor_id
            yield (self.stream, visit)


class Visitors(Stream):
    name = "visitors"
    replication_method = "INCREMENTAL"
    replication_key = "metadata.auto.lastupdated"

    def sync(self, state):
        # bookmark = self.get_bookmark(state)
        visitors = self.request(self.name).json()['results']

        visitor_history_stream = VisitorHistory(self.config)

        def emit_sub_stream_metrics(sub_stream):
            if sub_stream.is_selected():
                singer.metrics.log(LOGGER, Point(metric_type='counter',
                                                 metric=singer.metrics.Metric.record_count,
                                                 value=sub_stream.count,
                                                 tags={'endpoint': sub_stream.stream.tap_stream_id}))
                sub_stream.count = 0

        if visitor_history_stream.is_selected():
            LOGGER.info("Syncing visitor_history per visitor...")

        for visitor in visitors:
            if visitor_history_stream.is_selected():
                try:
                    visitor_history_stream.sync(state, visitor["visitorId"])
                except HTTPError:
                    LOGGER.warning("Unable to retrieve visitor_history for visitor (ID: {})".format(
                        visitor["visitorId"]))

            yield (self.stream, visitor)


STREAMS = {
    "accounts": Accounts,
    "features": Features,
    "guides": Guides,
    "pages": Pages,
    "reports": Reports,
    "visitor_history": VisitorHistory,
    "visitors": Visitors
}

SUB_STREAMS = {
    'visitors': ['visit_history']
}
