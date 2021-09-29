# pylint: disable=E1101,R0201,W0613

#!/usr/bin/env python3
import itertools
import json
import os
import time
from datetime import timedelta

import backoff
import humps
import ijson
import requests
import singer
import singer.metrics as metrics
from requests.exceptions import HTTPError
from singer import Transformer, metadata
from singer.utils import now, strftime, strptime_to_utc
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
                        "accounts": "null"
                    }
                }],
                "requestId": "all-accounts"
            }
        }
    },
    "features": {
        "method": "POST",
        "endpoint": "/api/v1/aggregation",
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
        "method": "POST",
        "endpoint": "/api/v1/aggregation",
    },
    "metadata_accounts": {
        "method": "GET",
        "endpoint": "/api/v1/metadata/schema/account"
    },
    "metadata_visitors": {
        "method": "GET",
        "endpoint": "/api/v1/metadata/schema/visitor"
    },
    "events": {
        "method": "POST",
        "endpoint": "/api/v1/aggregation",
    },
    "pages": {
        "method": "POST",
        "endpoint": "/api/v1/aggregation",
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
        "endpoint": "/api/v1/aggregation"

    },
    "visitor_history": {
        "method": "GET",
        "endpoint": "/api/v1/visitor/{visitorId}/history",
        "headers": {
            'content-type': 'application/x-www-form-urlencoded'
        },
        "params": {
            "starttime": "start_time"
        }
    },
    "track_types": {
        "method": "POST",
        "endpoint": "/api/v1/aggregation"
    },
    "track_events": {
        "method": "POST",
        "endpoint": "/api/v1/aggregation"
    }
}

LOGGER = singer.get_logger()
session = requests.Session()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_url(endpoint, **kwargs):
    return BASE_URL + endpoints[endpoint]['endpoint'].format(**kwargs)


def get_method(endpoint):
    return endpoints[endpoint]['method']


def get_headers(endpoint):
    return endpoints[endpoint].get('headers', {})


def get_params(endpoint):
    return endpoints[endpoint].get('params', {})

# Determine absolute start and end times w/ attribution_window constraint
# abs_start/end and window_start/end must be rounded to nearest hour or day (granularity)
# Graph API enforces max history of 28 days
def get_absolute_start_end_time(last_dttm):
    now_dttm = now()
    delta_days = (now_dttm - last_dttm).days
    # 28 days NOT including current
    if delta_days > 179:
        start = now_dttm - timedelta(179)
        LOGGER.info('Start date exceeds max. Setting start date to %s', start)
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


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)



class Server42xRateLimitError(Exception):
    pass

class Stream():
    name = None
    replication_method = None
    replication_key = None
    key_properties = KEY_PROPERTIES
    stream = None
    method = "GET"
    period = None

    def __init__(self, config=None):
        self.config = config

    def send_request_get_results(self, req):
        resp = session.send(req)

        if 'Too Many Requests' in resp.reason:
            retry_after = 30
            LOGGER.info("Rate limit reached. Sleeping for %s seconds",
                        retry_after)
            time.sleep(retry_after)
            raise Server42xRateLimitError(resp.reason)

        resp.raise_for_status()

        dec = humps.decamelize(resp.json())
        return dec

    @backoff.on_exception(backoff.expo, (requests.exceptions.RequestException, Server42xRateLimitError),
                          max_tries=5,
                          giveup=lambda e: e.response is not None and 400 <= e.
                          response.status_code < 500,
                          factor=2)
    @tap_pendo_utils.ratelimit(1, 2)
    def request(self, endpoint, params=None, **kwargs):
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

        if kwargs.get('data'):
            request_kwargs['data'] = json.dumps(kwargs.get('data'))

        if kwargs.get('json'):
            request_kwargs['json'] = kwargs.get('json')

        req = requests.Request(**request_kwargs).prepare()

        LOGGER.info("%s %s %s", request_kwargs['method'],
                    request_kwargs['url'], request_kwargs['params'])

        return self.send_request_get_results(req)

    def get_bookmark(self, state, stream, default, key=None):
        if (state is None) or ('bookmarks' not in state):
            return default
        if not state.get('bookmarks').get(stream):
            state['bookmarks'][stream] = {}
        if key:
            return (state.get('bookmarks', {}).get(stream,
                                                   {}).get(key, default))
        return state.get('bookmarks', {}).get(stream, default)

    def update_bookmark(self,
                        state,
                        stream,
                        bookmark_value,
                        bookmark_key=None):
        # NOTE: Bookmarking can never be updated to not get the most
        # recent thing it saw the next time you run, because the querying
        # only allows greater than or equal semantics.
        singer.write_bookmark(
            state,
            # name is overridden by some substreams
            stream,
            bookmark_key or self.replication_key,
            bookmark_value)
        singer.write_state(state)


    def load_shared_schema_refs(self):
        shared_schemas_path = get_abs_path('schemas/shared')

        shared_file_names = [
            f for f in os.listdir(shared_schemas_path)
            if os.path.isfile(os.path.join(shared_schemas_path, f))
        ]

        shared_schema_refs = {}
        for shared_file in shared_file_names:
            with open(os.path.join(shared_schemas_path, shared_file)) as data_file:
                shared_schema_refs[shared_file] = json.load(data_file)

        return shared_schema_refs

    def resolve_schema_references(self, schema, key, refs):
        if isinstance(schema, dict):
            for k, v in schema.items():
                if isinstance(v, (dict, list)):
                    if "$ref" in v:
                        schema[k] = refs.get(v.get('$ref'))
                    else:
                        self.resolve_schema_references(v, key, refs)


    def load_schema(self):
        refs = self.load_shared_schema_refs()

        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)
        self.resolve_schema_references(schema, "$ref", refs)
        return schema

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
        if self.replication_key == 'day' or self.replication_key == 'hour':
            if hasattr(self, 'period') and self.period == 'hourRange':
                mdata.pop(('properties', 'day'))
            elif hasattr(self, 'period') and self.period == 'dayRange':
                mdata.pop(('properties', 'hour'))

        return metadata.to_list(mdata)

    def is_selected(self):
        return self.stream is not None

    def transform(self, record):
        return humps.decamelize(record)

    def sync_substream(self, state, parent, sub_stream, parent_response):
        bookmark_date = self.get_bookmark(state, sub_stream.name,
                                          self.config.get('start_date'),
                                          sub_stream.replication_key)
        # If last sync was interrupted, get last processed parent record
        last_processed = self.get_bookmark(state,
                                           sub_stream.name,
                                           None,
                                           key="last_processed")
        bookmark_dttm = strptime_to_utc(bookmark_date)
        new_bookmark = bookmark_dttm

        singer.write_schema(sub_stream.name,
                            sub_stream.stream.schema.to_dict(),
                            sub_stream.key_properties)

        # Slice response for >= last processed
        if last_processed:
            i = 0
            for response in parent_response:
                if response.get(parent.key_properties[0]) == last_processed:
                    LOGGER.info("Resuming %s sync with %s", sub_stream.name, response.get(parent.key_properties[0]))
                    if isinstance(parent_response, list):
                        parent_response = parent_response[i:]
                    else:
                        parent_response = itertools.chain([response], parent_response)
                    break
                i += 1

        for record in parent_response:
            try:
                with metrics.record_counter(
                        sub_stream.name) as counter, Transformer(
                            integer_datetime_fmt=
                            "unix-milliseconds-integer-datetime-parsing"
                        ) as transformer:
                    stream_events = sub_stream.sync(state, new_bookmark,
                                                    record.get(parent.key_properties[0]))
                    for event in stream_events:
                        counter.increment()

                        schema_dict = sub_stream.stream.schema.to_dict()
                        stream_metadata = metadata.to_map(
                            sub_stream.stream.metadata)

                        transformed_event = sub_stream.transform(event)

                        try:
                            transformed_record = transformer.transform(
                                transformed_event, schema_dict,
                                stream_metadata)
                        except Exception as err:
                            LOGGER.error('Error: %s', err)
                            LOGGER.error(
                                ' for schema: %s',
                                json.dumps(schema_dict,
                                           sort_keys=True,
                                           indent=2))
                            raise err

                        event_time = strptime_to_utc(
                            transformed_record.get(sub_stream.replication_key))

                        new_bookmark = max(new_bookmark, event_time)
                        singer.write_record(sub_stream.stream.tap_stream_id,
                                            transformed_record)

            except HTTPError:
                LOGGER.warning(
                    "Unable to retrieve %s Event for Stream (ID: %s)",
                    sub_stream.name, record[parent.key_properties[0]])

            # All events for all parents processed; can removed last processed
            self.update_bookmark(state=state, stream=sub_stream.name, bookmark_value=record.get(parent.key_properties[0]), bookmark_key="last_processed")
            self.update_bookmark(state=state, stream=sub_stream.name, bookmark_value=strftime(new_bookmark), bookmark_key=sub_stream.replication_key)

        # After processing for all parent ids we can remove our resumption state
        state.get('bookmarks').get(sub_stream.name).pop('last_processed')
        update_currently_syncing(state, None)


    def sync(self, state, start_date=None, key_id=None):
        stream_response = self.request(self.name, json=self.get_body())['results'] or []

        if STREAMS.get(SUB_STREAMS.get(self.name)):
            sub_stream = STREAMS.get(SUB_STREAMS.get(self.name))(self.config)
        else:
            sub_stream = None

        if stream_response and sub_stream and sub_stream.is_selected():
            self.sync_substream(state, self, sub_stream, stream_response)

        update_currently_syncing(state, None)
        return (self.stream, stream_response)

    def lookback_window(self):
        lookback_window = self.config.get('lookback_window') or '0'
        if not lookback_window.isdigit():
            raise TypeError("lookback_window '{}' is not numeric. Check your configuration".format(lookback_window))
        return int(lookback_window)

class LazyAggregationStream(Stream):
    def send_request_get_results(self, req):
        with session.send(req, stream=True) as resp:
            if 'Too Many Requests' in resp.reason:
                retry_after = 30
                LOGGER.info("Rate limit reached. Sleeping for %s seconds",
                            retry_after)
                time.sleep(retry_after)
                raise Server42xRateLimitError(resp.reason)

            resp.raise_for_status()

            for item in ijson.items(resp.raw, 'results.item'):
                yield humps.decamelize(item)

    def sync(self, state, start_date=None, key_id=None):
        stream_response = self.request(self.name, json=self.get_body()) or []

        if STREAMS.get(SUB_STREAMS.get(self.name)):
            sub_stream = STREAMS.get(SUB_STREAMS.get(self.name))(self.config)
        else:
            sub_stream = None

        if stream_response and sub_stream and sub_stream.is_selected():
            self.sync_substream(state, self, sub_stream, stream_response)

        update_currently_syncing(state, None)
        return (self.stream, stream_response)

class EventsBase(Stream):
    DATE_WINDOW_SIZE = 1
    key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip']
    replication_method = "INCREMENTAL"

    def __init__(self, config):
        super().__init__(config=config)
        self.config = config
        self.period = config.get('period')
        self.replication_key = "day" if self.period == 'dayRange' else "hour"

    def sync(self, state, start_date=None, key_id=None):
        update_currently_syncing(state, self.name)

        lookback = start_date - timedelta(
            days=self.lookback_window())
        ts = int(lookback.timestamp()) * 1000

        # Period always amounts to a day either aggegated by day or hours in a day
        period = self.config.get('period')
        body = self.get_body(key_id, period, ts)
        events = self.request(self.name, json=body).get('results') or []
        update_currently_syncing(state, None)
        return events


class Accounts(Stream):
    name = "accounts"
    replication_method = "INCREMENTAL"
    replication_key = "lastupdated"
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
                "requestId": "all-accounts",
                "sort": ["accountId"]
            }
        }

    def transform(self, record):
        record['lastupdated'] = record.get('metadata').get('auto').get(
            'lastupdated')
        transformed = record
        for key in record['metadata'].keys():
            denested_key = 'metadata_' + key
            transformed[denested_key] = record['metadata'][key]
        del transformed['metadata']

        return super().transform(transformed)


class Features(Stream):
    name = "features"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def get_body(self):
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "name":
                "all-features",
                "pipeline": [{
                    "source": {
                        "features": None
                    }
                }, {
                    "sort": ["id"]
                }],
                "requestId":
                "all-features"
            }
        }


class FeatureEvents(EventsBase):
    name = "feature_events"
    replication_method = "INCREMENTAL"
    key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip']

    def get_body(self, key_id, period, first):
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
                            "last": "now()"
                        }
                    }
                }, {
                    "sort": [self.replication_key]
                }]
            }
        }


class Events(LazyAggregationStream):
    name = "events"
    DATE_WINDOW_SIZE = 1
    key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip']
    replication_method = "INCREMENTAL"

    def __init__(self, config):
        super().__init__(config=config)
        self.config = config
        self.period = config.get('period')
        self.replication_key = "day" if self.period == 'dayRange' else "hour"

    def sync(self, state, start_date=None, key_id=None):
        update_currently_syncing(state, self.name)

        bookmark_date = self.get_bookmark(state, self.name,
                                          self.config.get('start_date'),
                                          self.replication_key)
        bookmark_dttm = strptime_to_utc(bookmark_date)

        lookback = bookmark_dttm - timedelta(
            days=self.lookback_window())
        ts = int(lookback.timestamp()) * 1000

        period = self.config.get('period')
        body = self.get_body(period, ts)
        events = self.request(self.name, json=body) or []
        update_currently_syncing(state, None)
        return self.stream, events

    def transform(self, record):
        return humps.decamelize(record)

    def get_body(self, period, first):
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
                            "last": "now()"
                        }
                    }
                }, {
                    "sort": [self.replication_key]
                }]
            }
        }

class PollEvents(Stream):
    replication_method = "INCREMENTAL"
    name = "poll_events"
    key_properties = ['visitor_id', 'account_id', 'server_name', 'remote_ip']

    def __init__(self, config):
        super().__init__(config=config)
        self.config = config
        self.period = config.get('period')
        self.replication_key = 'browser_time'

    def get_body(self, period, first):
        sort = humps.camelize(self.replication_key)
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
                            "last": "now()"
                        }
                    }
                }, {
                    "sort": [sort]
                }]
            }
        }

    def sync(self, state, start_date=None, key_id=None):
        update_currently_syncing(state, self.name)

        bookmark_date = self.get_bookmark(state, self.name,
                                          self.config.get('start_date'),
                                          self.replication_key)
        bookmark_dttm = strptime_to_utc(bookmark_date)

        lookback = bookmark_dttm - timedelta(
            days=self.lookback_window())
        ts = int(lookback.timestamp()) * 1000

        period = self.config.get('period')
        body = self.get_body(period, ts)
        events = self.request(self.name, json=body).get('results') or []
        return self.stream, events

class TrackEvents(EventsBase):
    replication_method = "INCREMENTAL"
    name = "track_events"
    key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip']


    def get_body(self, key_id, period, first):
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "pipeline": [{
                    "source": {
                        "trackEvents": {
                            "trackTypeId": key_id
                        },
                        "timeSeries": {
                            "period": period,
                            "first": first,
                            "last": "now()"
                        }
                    }
                }, {
                    "sort": [self.replication_key]
                }]
            }
        }

class GuideEvents(EventsBase):
    replication_method = "INCREMENTAL"
    name = "guide_events"
    key_properties = ['visitor_id', 'account_id', 'server_name', 'remote_ip']

    def __init__(self, config):
        super().__init__(config=config)
        self.config = config
        self.period = config.get('period')
        self.replication_key = 'browser_time'

    def get_body(self, key_id, period, first):
        sort = humps.camelize(self.replication_key)
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "pipeline": [
                    {
                        "source": {
                            "guideEvents": {
                                "guideId": key_id
                            },
                            "timeSeries": {
                                "period": period,
                                "first": first,
                                "last": "now()"
                                }
                        }
                    },
                    {
                        "sort": [sort]
                    }
                ]
            }
        }


class TrackTypes(Stream):
    name = "track_types"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def get_body(self):
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "name": "all-track-types",
                "pipeline": [{
                    "source": {
                        "trackTypes": None
                    }
                }, {
                    "sort": ["id"]
                }],
                "requestId": "all-track-types"
            }
        }


class Guides(Stream):
    name = "guides"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def get_body(self):
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "name":
                "all-guides",
                "pipeline": [{
                    "source": {
                        "guides": None
                    }
                }, {
                    "sort": ["id"]
                }],
                "requestId":
                "all-guides"
            }
        }


class Pages(Stream):
    name = "pages"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def get_body(self):
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "name":
                "all-pages",
                "pipeline": [{
                    "source": {
                        "pages": None
                    }
                }, {
                    "sort": ["id"]
                }],
                "requestId":
                "all-pages"
            }
        }


class PageEvents(EventsBase):
    name = "page_events"
    replication_method = "INCREMENTAL"
    key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip']

    def get_body(self, key_id, period, first):
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
                            "last": "now()"
                        }
                    }
                }, {
                    "sort": [self.replication_key]
                }]
            }
        }


class Reports(Stream):
    name = "reports"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def sync(self, state, start_date=None, key_id=None):
        reports = self.request(self.name)
        for report in reports:
            yield (self.stream, report)


class MetadataVisitor(Stream):
    name = "metadata_visitor"
    replication_method = "FULL_TABLE"

    def sync(self, state, start_date=None, key_id=None):
        reports = self.request(self.name)
        for report in reports:
            yield (self.stream, report)


class VisitorHistory(Stream):
    name = "visitor_history"
    replication_method = "INCREMENTAL"
    replication_key = "modified_ts"
    key_properties = ['visitor_id']
    DATE_WINDOW_SIZE = 1

    def get_params(self, start_time):
        return {"starttime": start_time}

    def sync(self, state, start_date=None, key_id=None):
        update_currently_syncing(state, self.name)

        bookmark_date = self.get_bookmark(state, self.name,
                                          self.config.get('start_date'),
                                          self.replication_key)
        bookmark_dttm = strptime_to_utc(bookmark_date)

        abs_start, abs_end = get_absolute_start_end_time(bookmark_dttm)
        lookback = abs_start - timedelta(days=self.lookback_window())
        window_next = lookback

        while window_next <= abs_end:
            ts = int(window_next.timestamp()) * 1000
            params = self.get_params(start_time=ts)
            visitor_history = self.request(endpoint=self.name,
                                           params=params,
                                           visitorId=key_id)
            for visitor in visitor_history:
                visitor['visitorId'] = key_id
                yield visitor
            window_next = window_next + timedelta(days=self.DATE_WINDOW_SIZE)

    def transform(self, record):
        max_value = max(record.get('ts', 0), record.get('last_ts', 0))
        record['modified_ts'] = max_value
        return super().transform(record)


class Visitors(LazyAggregationStream):
    name = "visitors"
    replication_method = "INCREMENTAL"
    replication_key = "lastupdated"
    key_properties = ["visitor_id"]
    method = "POST"

    def get_endpoint(self):
        return "/api/v1/aggregation"

    def get_body(self):
        include_anonymous_visitors = bool(self.config.get('include_anonymous_visitors', 'false').lower() == 'true')
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "name":
                "all-visitors",
                "pipeline": [{
                    "source": {
                        "visitors": {
                            "identified": not include_anonymous_visitors
                        }
                    }
                }],
                "requestId": "all-visitors",
                "sort": [
                    "visitorId"
                ]
            }
        }

    def transform(self, record):
        record['lastupdated'] = record.get('metadata').get('auto').get(
            'lastupdated')
        transformed = record
        for key in record['metadata'].keys():
            denested_key = 'metadata_' + key
            transformed[denested_key] = record['metadata'][key]
        del transformed['metadata']

        return super().transform(transformed)


class MetadataAccounts(Stream):
    name = "metadata_accounts"
    replication_method = "FULL_TABLE"
    key_properties = []

    def get_body(self):
        return None

    def sync(self, state, start_date=None, key_id=None):
        stream_response = self.request(self.name, json=self.get_body())

        if STREAMS.get(SUB_STREAMS.get(self.name)):
            sub_stream = STREAMS.get(SUB_STREAMS.get(self.name))(self.config)
        else:
            sub_stream = None

        if stream_response and sub_stream and sub_stream.is_selected():
            self.sync_substream(state, self, sub_stream, stream_response)

        update_currently_syncing(state, None)
        return (self.stream, [stream_response])

    def get_fields(self):
        return self.request(self.name, json=self.get_body())

class MetadataVisitors(Stream):
    name = "metadata_visitors"
    replication_method = "FULL_TABLE"
    key_properties = []

    def get_body(self):
        return None

    def sync(self, state, start_date=None, key_id=None):
        stream_response = self.request(self.name, json=self.get_body())

        if STREAMS.get(SUB_STREAMS.get(self.name)):
            sub_stream = STREAMS.get(SUB_STREAMS.get(self.name))(self.config)
        else:
            sub_stream = None

        if stream_response and sub_stream and sub_stream.is_selected():
            self.sync_substream(state, self, sub_stream, stream_response)

        update_currently_syncing(state, None)
        return (self.stream, [stream_response])

    def get_fields(self):
        return self.request(self.name, json=self.get_body())



STREAMS = {
    "accounts": Accounts,
    "features": Features,
    "guides": Guides,
    "pages": Pages,
    "visitor_history": VisitorHistory,
    "visitors": Visitors,
    "feature_events": FeatureEvents,
    "events": Events,
    "page_events": PageEvents,
    "guide_events": GuideEvents,
    "poll_events": PollEvents,
    "track_types": TrackTypes,
    "track_events": TrackEvents,
    "metadata_accounts": MetadataAccounts,
    "metadata_visitors": MetadataVisitors
}

SUB_STREAMS = {
    'visitors': 'visitor_history',
    'features': 'feature_events',
    'pages': 'page_events',
    'guides': 'guide_events',
    'track_types': 'track_events'
}
