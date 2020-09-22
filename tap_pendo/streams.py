# pylint: disable=E1101,R0201
#!/usr/bin/env python3
import json
import os
import time
from datetime import timedelta

import backoff
import dateutil.parser
import humps
import pytz
import requests
import singer
import singer.metrics as metrics
from requests.exceptions import HTTPError
from singer import Transformer, metadata, utils
from singer.utils import now, strptime_to_utc
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
                "pipeline": [{
                    "source": {
                        "accounts": None
                    }
                }],
                "requestId": "all-accounts"
            }
        }
    },
    "features": {
        "method": "GET",
        "endpoint": "/api/v1/feature"
    },
    "guide_events": {
        "method": "POST",
        "endpoint": "/api/v1/aggregation",
    },
    "feature_events": {
        "method": "POST",
        "endpoint": "/api/v1/aggregation",
        "data": {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "pipeline": [{
                    "source": {
                        "featureEvents": {
                            "featureId": "{featureId}"
                        },
                        "timeSeries": {
                            "period": "dayRange",
                            "first": 1598920967000,
                            "last": "now()"
                        }
                    }
                }]
            }
        }
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
    "events": {
        "method": "POST",
        "endpoint": "/api/v1/aggregation",
    },
    "pages": {
        "method": "GET",
        "endpoint": "/api/v1/page"
    },
    "page_events": {
        "method": "POST",
        "endpoint": "/api/v1/aggregation",
    },
    "poll_events": {
        "method": "POST",
        "endpoint": "/api/v1/aggregation",
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
                "pipeline": [{
                    "source": {
                        "visitors": None
                    }
                }],
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


def get_data(endpoint, **kwargs):
    return endpoints[endpoint].get('data', {})


def get_headers(endpoint):
    return endpoints[endpoint].get('headers', {})


def get_params(endpoint):
    return endpoints[endpoint].get('params', {})


def strptime_to_utc(dtimestr):
    d_object = dateutil.parser.parse(dtimestr)
    if d_object.tzinfo is None:
        return d_object.replace(tzinfo=pytz.UTC)
    else:
        return d_object.astimezone(tz=pytz.UTC)


# Determine absolute start and end times w/ attribution_window constraint
# abs_start/end and window_start/end must be rounded to nearest hour or day (granularity)
# Graph API enforces max history of 28 days
def get_absolute_start_end_time(last_dttm):
    now_dttm = now()
    delta_days = (now_dttm - last_dttm).days
    # 28 days NOT including current
    if delta_days > 179:
        start = now_dttm - timedelta(179)
        LOGGER.info(
            'Start date exceeds max. Setting start date to %s', start)
    else:
        start = last_dttm

    abs_start, abs_end = round_times(start=start, end=now_dttm)
    return abs_start, abs_end


def round_times(start=None, end=None):
    start_rounded = None
    end_rounded = None
    # Round min_start, max_end to hours or dates
    start_rounded = remove_hours_local(start)
    end_rounded = remove_hours_local(end)
    return start_rounded, end_rounded


def remove_hours_local(dttm):
    new_dttm = dttm.replace(hour=0, minute=0, second=0, microsecond=0)
    return new_dttm


class Stream():
    name = None
    replication_method = None
    replication_key = None
    key_properties = KEY_PROPERTIES
    stream = None
    method = "GET"

    def __init__(self, config=None):
        self.config = config

    @backoff.on_exception(backoff.expo, (requests.exceptions.RequestException),
                          max_tries=5,
                          giveup=lambda e: e.response is not None and 400 <= e.
                          response.status_code < 500,
                          factor=2)
    @tap_pendo_utils.ratelimit(1, 2)
    def request(self, endpoint, params=None, data=None, **kwargs):
        # params = params or {}
        headers = {
            'x-pendo-integration-key': self.config['x_pendo_integration_key'],
            'content-type': 'application/json'
        }

        request_kwargs = {
            'url': get_url(endpoint, **kwargs),
            'method': get_method(endpoint),
            'headers': headers,
            'params': params
        }

        headers = get_headers(endpoint)
        if headers:
            request_kwargs['headers'].update(headers)

        if data:
            request_kwargs['data'] = json.dumps(data)

        req = requests.Request(**request_kwargs).prepare()
        LOGGER.info("%s %s %s %s", request_kwargs['method'],
                    request_kwargs['url'], request_kwargs['headers'],
                    request_kwargs['params'], request_kwargs.get('data', None))
        resp = session.send(req)

        if 'Too Many Requests' in resp.reason:
            retry_after = 30
            LOGGER.info("Rate limit reached. Sleeping for %s seconds", retry_after)
            time.sleep(retry_after)
            return request(url, params)

        resp.raise_for_status()

        return resp

    def get_bookmark(self, state, stream, default):
        if (state is None) or ('bookmarks' not in state):
            return default
        return (state.get('bookmarks', {}).get(stream, default))

    def update_bookmark(self, state, value):
        current_bookmark = self.get_bookmark(state)
        if value and utils.strptime_with_tz(value) > current_bookmark:
            singer.write_bookmark(state, self.name, self.replication_key,
                                  value)

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

        mdata = metadata.write(mdata, (), 'table-key-properties',
                               self.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method',
                               self.replication_method)

        if self.replication_key:
            mdata = metadata.write(mdata, (), 'valid-replication-keys',
                                   [self.replication_key])

        for field_name in schema['properties'].keys():
            if field_name in self.key_properties or field_name == self.replication_key:
                mdata = metadata.write(mdata, ('properties', field_name),
                                       'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name),
                                       'inclusion', 'available')

        # For period stream adjust schema for time period
        if hasattr(self, 'period') and self.period == 'hourRange':
            mdata.pop(('properties', 'day'))
        elif hasattr(self, 'period') and self.period == 'dayRange':
            mdata.pop(('properties', 'hour'))

        return metadata.to_list(mdata)

    def is_selected(self):
        return self.stream is not None


class Accounts(Stream):
    name = "accounts"
    replication_method = "INCREMENTAL"
    replication_key = "metadata.auto.lastupdated"
    key_properties = ["account_id"]
    method = "POST"

    def get_body(self):
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "name": "all-accounts",
                "pipeline": [{
                    "source": {
                        "accounts": None
                    }
                }],
                "requestId": "all-accounts"
            }
        }

    def sync(self, state):
        body = self.get_body()
        accounts = self.request(self.name, data=body).json()['results']
        for account in accounts:
            yield (self.stream, account)


class Features(Stream):
    name = "features"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def sync(self, state):
        features = self.request(self.name).json()
        feature_events_stream = FeatureEvents(self.config)
        for feature in features:
            if feature_events_stream.is_selected():
                LOGGER.info("Syncing Feature Events per Feature...")
                try:
                    with metrics.record_counter(
                            feature_events_stream.name
                    ) as counter, Transformer() as transformer:
                        feature_events = feature_events_stream.sync(
                            state, feature["id"])
                        for (sub_stream, feature_event) in feature_events:
                            counter.increment()

                            schema_dict = sub_stream.schema.to_dict()
                            stream_metadata = metadata.to_map(
                                sub_stream.metadata)

                            transformed_feature_event = humps.decamelize(
                                feature_event)

                            try:
                                transformed_record = transformer.transform(
                                    transformed_feature_event, schema_dict,
                                    stream_metadata)
                            except Exception as err:
                                LOGGER.error('Error: %s', err)
                                LOGGER.error(
                                    ' for schema: %s',
                                    json.dumps(schema_dict,
                                               sort_keys=True,
                                               indent=2))
                                raise err

                            singer.write_record(sub_stream.tap_stream_id,
                                                transformed_record)
                            # NB: We will only write state at the end of a stream's sync:
                            #  We may find out that there exists a sync that takes too long and can never emit a bookmark
                            #  but we don't know if we can guarentee the order of emitted records.
                except HTTPError:
                    LOGGER.warning(
                        "Unable to retrieve Feature Event for Feature (ID: {})"
                        .format(feature["featureId"]))
            yield (self.stream, feature)


class EventsBase(Stream):
    DATE_WINDOW_SIZE = 1
    replication_method = "INCREMENTAL"

    def __init__(self, config=None):
        super().__init__(config)
        self.config = config
        self.period = config.get('period')
        self.replication_key = "day" if self.period == 'dayRange' else "hour"

    def sync(self, state, key_id):
        startdate = self.get_bookmark(state, self.name,
                                      self.config.get('start_date'))
        last_dttm = strptime_to_utc(startdate)
        ts = int(last_dttm.timestamp()) * 1000
        period = self.config.get('period')
        if period == 'dayRange':
            count = 1
        else:
            count = 24

        abs_start, abs_end = get_absolute_start_end_time(last_dttm)

        window_next = abs_start
        while window_next != abs_end:
            ts = int(window_next.timestamp()) * 1000
            body = self.get_body(key_id, period, ts, count)
            events = self.request(self.name,
                                  data=body).json().get('results') or []

            for event in events:
                yield (self.stream, event)
            window_next = window_next + timedelta(days=self.DATE_WINDOW_SIZE)


class Events(EventsBase):
    name = "events"
    key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip']

    def __init__(self, config=None):
        super().__init__(config)
        self.config = config
        self.period = config.get('period')
        self.replication_key = "day" if self.period == 'dayRange' else "hour"

    def get_body(self, period, first, count):
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "pipeline": [{
                    "source": {
                        "events": None,
                        "timeSeries": {
                            "period": period,
                            "first": first,
                            "count": count
                        }
                    }
                }]
            }
        }

    def sync(self, state):
        startdate = self.get_bookmark(state, self.name,
                                      self.config.get('start_date'))
        last_dttm = strptime_to_utc(startdate.get(self.replication_key))
        ts = int(last_dttm.timestamp()) * 1000
        period = self.config.get('period')
        if period == 'dayRange':
            count = 1
        else:
            count = 24

        abs_start, abs_end = get_absolute_start_end_time(last_dttm)

        window_next = abs_start
        while window_next != abs_end:
            ts = int(window_next.timestamp()) * 1000
            body = self.get_body(period, ts, count)
            events = self.request(self.name,
                                  data=body).json().get('results') or []

            for event in events:
                yield (self.stream, event)
            window_next = window_next + timedelta(days=self.DATE_WINDOW_SIZE)


class PollEvents(EventsBase):
    name = "poll_events"
    key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip']

    def __init__(self, config=None):
        super().__init__(config)
        self.config = config
        self.period = config.get('period')
        self.replication_key = "day" if self.period == 'dayRange' else "hour"

    def get_body(self, period, first, count):
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "pipeline": [{
                    "source": {
                        "pollEvents": None,
                        "timeSeries": {
                            "period": period,
                            "first": first,
                            "count": count
                        }
                    }
                }]
            }
        }

    def sync(self, state):
        startdate = self.get_bookmark(state, self.name,
                                      self.config.get('start_date'))
        last_dttm = strptime_to_utc(startdate.get(self.replication_key))
        ts = int(last_dttm.timestamp()) * 1000
        period = self.config.get('period')
        if period == 'dayRange':
            count = 1
        else:
            count = 24

        abs_start, abs_end = get_absolute_start_end_time(last_dttm)

        window_next = abs_start
        while window_next != abs_end:
            ts = int(window_next.timestamp()) * 1000
            body = self.get_body(period, ts, count)
            events = self.request(self.name,
                                  data=body).json().get('results') or []

            for event in events:
                yield (self.stream, event)
            window_next = window_next + timedelta(days=self.DATE_WINDOW_SIZE)


class GuideEvents(EventsBase):
    name = "guide_events"
    key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip']

    def __init__(self, config=None):
        super().__init__(config)
        self.config = config
        self.period = config.get('period')
        self.replication_key = "day" if self.period == 'dayRange' else "hour"

    def get_body(self, period, first, count):
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "pipeline": [{
                    "source": {
                        "guideEvents": None,
                        "timeSeries": {
                            "period": period,
                            "first": first,
                            "count": count
                        }
                    }
                }]
            }
        }

    def sync(self, state):
        startdate = self.get_bookmark(state, self.name,
                                      self.config.get('start_date'))
        last_dttm = strptime_to_utc(startdate.get(self.replication_key))
        ts = int(last_dttm.timestamp()) * 1000
        period = self.config.get('period')
        if period == 'dayRange':
            count = 1
        else:
            count = 24

        abs_start, abs_end = get_absolute_start_end_time(last_dttm)

        window_next = abs_start
        while window_next != abs_end:
            ts = int(window_next.timestamp()) * 1000
            body = self.get_body(period, ts, count)
            events = self.request(self.name,
                                  data=body).json().get('results') or []

            for event in events:
                yield (self.stream, event)
            window_next = window_next + timedelta(days=self.DATE_WINDOW_SIZE)


class FeatureEvents(EventsBase):
    name = "feature_events"
    replication_method = "INCREMENTAL"
    key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip']

    def __init__(self, config=None):
        super().__init__(config)
        self.config = config
        self.period = config.get('period')
        self.replication_key = "day" if self.period == 'dayRange' else "hour"

    def get_body(self, key_id, period, first, count):
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "pipeline": [{
                    "source": {
                        "featureEvents": {
                            "featureId": key_id
                        },
                        "timeSeries": {
                            "period": period,
                            "first": first,
                            "count": count
                        }
                    }
                }]
            }
        }


class PageEvents(EventsBase):
    name = "page_events"
    replication_method = "INCREMENTAL"
    key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip']

    def __init__(self, config=None):
        super().__init__(config)
        self.config = config
        self.period = config.get('period')
        self.replication_key = "day" if self.period == 'dayRange' else "hour"

    def get_body(self, key_id, period, first, count):
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "pipeline": [{
                    "source": {
                        "pageEvents": {
                            "pageId": key_id
                        },
                        "timeSeries": {
                            "period": period,
                            "first": first,
                            "count": count
                        }
                    }
                }]
            }
        }


class Guides(Stream):
    name = "guides"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def sync(self, state):
        guides = self.request(self.name).json()
        for guide in guides:
            yield (self.stream, guide)


class Pages(Stream):
    name = "pages"
    replication_method = "INCREMENTAL"
    replication_key = "last_updated_at"

    def sync(self, state):
        pages = self.request(self.name).json()
        page_events_stream = PageEvents(self.config)
        for page in pages:
            if page_events_stream.is_selected():
                LOGGER.info("Syncing Page Events per Page...")
                try:
                    with metrics.record_counter(
                            page_events_stream.name) as counter, Transformer(
                            ) as transformer:
                        page_events = page_events_stream.sync(
                            state, page["id"])
                        for (sub_stream, page_event) in page_events:
                            counter.increment()

                            schema_dict = sub_stream.schema.to_dict()
                            stream_metadata = metadata.to_map(
                                sub_stream.metadata)

                            transformed_page_event = humps.decamelize(
                                page_event)

                            try:
                                transformed_record = transformer.transform(
                                    transformed_page_event, schema_dict,
                                    stream_metadata)
                            except Exception as err:
                                LOGGER.error('Error: %s', err)
                                LOGGER.error(
                                    ' for schema: %s',
                                    json.dumps(schema_dict,
                                               sort_keys=True,
                                               indent=2))
                                raise err

                            singer.write_record(sub_stream.tap_stream_id,
                                                transformed_record)
                            # NB: We will only write state at the end of a stream's sync:
                            #  We may find out that there exists a sync that takes too long and can never emit a bookmark
                            #  but we don't know if we can guarentee the order of emitted records.
                except HTTPError:
                    LOGGER.warning(
                        "Unable to retrieve Feature Event for Feature (ID: %s)",
                        page["page_id"])
            yield (self.stream, page)


class Reports(Stream):
    name = "reports"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def sync(self, state):
        reports = self.request(self.name).json()
        for report in reports:
            yield (self.stream, report)


class MetadataVisitor(Stream):
    name = "metadata_visitor"
    replication_method = "FULL_TABLE"

    def sync(self, state):
        reports = self.request(self.name).json()
        for report in reports:
            yield (self.stream, report)


class VisitorHistory(Stream):
    name = "visitor_history"
    replication_method = "INCREMENTAL"
    replication_key = "ts"
    key_properties = ['visitor_id']
    DATE_WINDOW_SIZE = 1

    def get_params(self, start_time):
        return {"starttime": start_time}

    def sync(self, state, visitor_id):
        startdate = self.get_bookmark(state, self.name,
                                      self.config.get('start_date'))
        last_dttm = strptime_to_utc(startdate)
        abs_start, abs_end = get_absolute_start_end_time(last_dttm)

        window_next = abs_start
        while window_next != abs_end:
            ts = int(window_next.timestamp()) * 1000
            params = self.get_params(start_time=ts)
            visitor_history = self.request(endpoint=self.name,
                                           visitorId=visitor_id,
                                           params=params).json()
            for visit in visitor_history:
                visit['visitorId'] = visitor_id
                yield (self.stream, visit)
            window_next = window_next + timedelta(days=self.DATE_WINDOW_SIZE)


class Visitors(Stream):
    name = "visitors"
    replication_method = "INCREMENTAL"
    replication_key = "metadata.auto.lastupdated"
    key_properties = ["visitor_id"]
    method = "POST"

    def get_endpoint():
        return "/api/v1/aggregation"

    def get_body():
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "name": "all-visitors",
                "pipeline": [{
                    "source": {
                        "visitors": None
                    }
                }],
                "requestId": "all-visitors"
            }
        }

    def sync(self, state):
        visitors = self.request(self.name, data=self.get_body()).json()['results']

        visitor_history_stream = VisitorHistory(self.config)

        def emit_sub_stream_metrics(sub_stream):
            if sub_stream.is_selected():
                singer.metrics.log(
                    LOGGER,
                    Point(metric_type='counter',
                          metric=singer.metrics.Metric.record_count,
                          value=sub_stream.count,
                          tags={'endpoint': sub_stream.stream.tap_stream_id}))
                sub_stream.count = 0

        if visitor_history_stream.is_selected():
            LOGGER.info("Syncing visitor_history per visitor...")

        for visitor in visitors:
            if visitor_history_stream.is_selected():
                try:
                    with metrics.record_counter(
                            visitor_history_stream.name
                    ) as counter, Transformer() as transformer:
                        visitor_histories = visitor_history_stream.sync(
                            state, visitor["visitorId"])
                        for (sub_stream, visitor_history) in visitor_histories:
                            counter.increment()

                            schema_dict = sub_stream.schema.to_dict()
                            stream_metadata = metadata.to_map(
                                sub_stream.metadata)

                            transformed_visitor_history = humps.decamelize(
                                visitor_history)

                            try:
                                transformed_record = transformer.transform(
                                    transformed_visitor_history, schema_dict,
                                    stream_metadata)
                            except Exception as err:
                                LOGGER.error('Error: %s', err)
                                LOGGER.error(
                                    ' for schema: %s',
                                    json.dumps(schema_dict,
                                               sort_keys=True,
                                               indent=2))
                                raise err

                            singer.write_record(sub_stream.tap_stream_id,
                                                transformed_record)
                            # NB: We will only write state at the end of a stream's sync:
                            #  We may find out that there exists a sync that takes too long and can never emit a bookmark
                            #  but we don't know if we can guarentee the order of emitted records.
                except HTTPError:
                    LOGGER.warning(
                        "Unable to retrieve visitor_history for visitor (ID: %s)", visitor["visitorId"])

            yield (self.stream, visitor)


STREAMS = {
    "accounts": Accounts,
    "features": Features,
    "guides": Guides,
    "pages": Pages,
    "reports": Reports,
    "visitor_history": VisitorHistory,
    "visitors": Visitors,
    "feature_events": FeatureEvents,
    "events": Events,
    "page_events": PageEvents,
    "guide_events": GuideEvents,
    "poll_events": PollEvents
}

SUB_STREAMS = {
    'visitors': ['visitor_history'],
    'features': ['feature_events'],
    'pages': ['page_events']
}
