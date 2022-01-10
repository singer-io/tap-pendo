# pylint: disable=E1101,R0201,W0613

#!/usr/bin/env python3
import itertools
import json
import os
import time
from datetime import datetime, timedelta, timezone

import backoff
import humps
import ijson
import requests
import singer
import singer.metrics as metrics
from requests.exceptions import HTTPError
from requests.models import ProtocolError
from singer import Transformer, metadata
from singer.utils import now, strftime, strptime_to_utc
from tap_pendo import utils as tap_pendo_utils

KEY_PROPERTIES = ['id']
BASE_URL = "https://app.pendo.io"

LOGGER = singer.get_logger()
session = requests.Session()
# timeout request after 300 seconds
REQUEST_TIMEOUT = 300

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

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
    # Return start and end time with rounded time(removed hours local)
    start_rounded = None
    end_rounded = None
    # Round min_start, max_end to hours or dates
    start_rounded = remove_hours_local(start)
    end_rounded = remove_hours_local(end)
    return start_rounded, end_rounded


def remove_hours_local(dttm):
    # Remove hours local from provided datetime
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


class Endpoints():
    endpoint = ""
    method = ""
    headers = {}
    params = {}

    def __init__(self, endpoint, method, headers=None, params=None):
        self.endpoint = endpoint
        self.method = method
        self.headers = headers
        self.params = params

    def get_url(self, **kwargs):
        """
        Concatenate  and format the dynamic values to the BASE_URL
        """
        return BASE_URL + self.endpoint.format(**kwargs)


class Stream():
    """
    Base Stream class that works as a parent for child stream classes.
    """
    name = None
    replication_method = None
    replication_key = None
    key_properties = KEY_PROPERTIES
    stream = None
    period = None
    # initialized the endpoint attribute which can be overriden by child streams based on
    # the different parameters used by the stream.
    endpoint = Endpoints("/api/v1/aggregation", "POST")

    def __init__(self, config=None):
        self.config = config

    def send_request_get_results(self, req):
        # Set request timeout to config param `request_timeout` value.
        # If value is 0,"0", "" or None then it will set default to default to 300.0 seconds if not passed in config.
        config_request_timeout = self.config.get('request_timeout')
        request_timeout = config_request_timeout and float(config_request_timeout) or REQUEST_TIMEOUT # pylint: disable=consider-using-ternary
        resp = session.send(req, timeout=request_timeout)

        # Sleep for provided time and retry if rate limit exceeded
        if 'Too Many Requests' in resp.reason:
            retry_after = 30
            LOGGER.info("Rate limit reached. Sleeping for %s seconds",
                        retry_after)
            time.sleep(retry_after)
            raise Server42xRateLimitError(resp.reason)

        resp.raise_for_status() # Check for requests status and raise exception in failure

        dec = humps.decamelize(resp.json())
        return dec

    # backoff for Timeout error is already included in "requests.exceptions.RequestException"
    # as it is the parent class of "Timeout" error
    @backoff.on_exception(backoff.expo, (requests.exceptions.RequestException, Server42xRateLimitError),
                          max_tries=5,
                          giveup=lambda e: e.response is not None and 400 <= e.
                          response.status_code < 500,
                          factor=2)
    @backoff.on_exception(backoff.expo, (ConnectionError, ProtocolError), # backoff error
                          max_tries=5,
                          factor=2)
    @tap_pendo_utils.ratelimit(1, 2)
    def request(self, endpoint, params=None, **kwargs):
        # Set requests headers, url, methods, params and extra provided arguments
        # params = params or {}
        headers = {
            'x-pendo-integration-key': self.config['x_pendo_integration_key'],
            'content-type': 'application/json'
        }

        request_kwargs = {
            'url': self.endpoint.get_url(**kwargs),
            'method': self.endpoint.method,
            'headers': headers,
            'params': params
        }

        headers = self.endpoint.headers
        if headers:
            request_kwargs['headers'].update(headers)

        if kwargs.get('data'):
            request_kwargs['data'] = json.dumps(kwargs.get('data'))

        if kwargs.get('json'):
            request_kwargs['json'] = kwargs.get('json')

        req = requests.Request(**request_kwargs).prepare() # Prepare request

        LOGGER.info("%s %s %s", request_kwargs['method'],
                    request_kwargs['url'], request_kwargs['params'])

        return self.send_request_get_results(req)

    def get_bookmark(self, state, stream, default, key=None):
        # Return default value if no bookmark present in state for provided stream
        if (state is None) or ('bookmarks' not in state):
            return default
        # Initialize bookmark if not present
        if not state.get('bookmarks').get(stream):
            state['bookmarks'][stream] = {}
        # Look for bookmark with key for stream
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
        # Load and return dictionary of referenced schemas from 'schemas/shared'
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
        # Resolve $ref of schemas with provided referenced schema
        if isinstance(schema, dict):
            for k, v in schema.items():
                if isinstance(v, (dict, list)):
                    if "$ref" in v:
                        schema[k] = refs.get(v.get('$ref'))
                    else:
                        self.resolve_schema_references(v, key, refs)


    def load_schema(self):
        refs = self.load_shared_schema_refs() # Load references scheamas

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

        # Write key properties and replication method to metadata
        mdata = metadata.write(mdata, (), 'table-key-properties',
                               self.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method',
                               self.replication_method)

        # Write replication key to metadata
        if self.replication_key:
            # for a certain stream like "features, track_types, pages, guides"
            # the replication key in schema is "last_updated_at" and in class variable
            # of stream it is "lastUpdatedAt" so rather than updating the replication key
            # value in the class variable used "humps.decamelize" for backward compatibility
            # as for previous syncs the value in the bookmark will contain "lastUpdatedAt"
            mdata = metadata.write(mdata, (), 'valid-replication-keys',
                                   [humps.decamelize(self.replication_key)])

        # Make inclusion automatic for the key properties and replication keys
        for field_name in schema['properties'].keys():
            # for a certain stream like "features, track_types, pages, guides"
            # the replication key in schema is "last_updated_at" and in class variable
            # of stream it is "lastUpdatedAt" so rather than updating the replication key
            # value in the class variable used "humps.decamelize" for backward compatibility
            # as for previous syncs the value in the bookmark will contain "lastUpdatedAt"
            if field_name in self.key_properties or field_name == humps.decamelize(self.replication_key):
                mdata = metadata.write(mdata, ('properties', field_name),
                                       'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name),
                                       'inclusion', 'available')

        # For period stream adjust schema for time period
        if self.replication_key in ('day', 'hour'):
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
        # Get bookmark from state or start date for the stream
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

        # Loop over records of parent stream
        for record in parent_response:
            try:
                # Initialize counter and transformer with specific datetime format
                with metrics.record_counter(
                        sub_stream.name) as counter, Transformer(
                            integer_datetime_fmt=
                            "unix-milliseconds-integer-datetime-parsing"
                        ) as transformer:
                    # syncing child streams from start date or state file date
                    stream_events = sub_stream.sync(state, bookmark_dttm,
                                                    record.get(parent.key_properties[0]))
                    # Loop over data of sub-stream
                    for event in stream_events:
                        counter.increment()

                        # Get metadata for the stream to use in transform
                        schema_dict = sub_stream.stream.schema.to_dict()
                        stream_metadata = metadata.to_map(
                            sub_stream.stream.metadata)

                        transformed_event = sub_stream.transform(event)

                        # Transform record as per field selection in metadata
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

                        # Check for replication_value from record and if value found then use it for updating bookmark
                        replication_value = transformed_record.get(sub_stream.replication_key)
                        if replication_value:
                            event_time = strptime_to_utc(replication_value)
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

        # Get and intialize sub-stream for the current stream
        if STREAMS.get(SUB_STREAMS.get(self.name)):
            sub_stream = STREAMS.get(SUB_STREAMS.get(self.name))(self.config)
        else:
            sub_stream = None

        # Sync substream if the current stream has sub-stream and selected in the catalog
        if stream_response and sub_stream and sub_stream.is_selected():
            self.sync_substream(state, self, sub_stream, stream_response)

        update_currently_syncing(state, None)
        return (self.stream, stream_response)

    def lookback_window(self):
        # Get lookback window from config and verify value
        lookback_window = self.config.get('lookback_window') or '0'
        if not lookback_window.isdigit():
            raise TypeError("lookback_window '{}' is not numeric. Check your configuration".format(lookback_window))
        return int(lookback_window)

class LazyAggregationStream(Stream):
    def send_request_get_results(self, req):
        # Set request timeout to config param `request_timeout` value.
        # If value is 0,"0", "" or None then it will set default to default to 300.0 seconds if not passed in config.
        config_request_timeout = self.config.get('request_timeout')
        request_timeout = config_request_timeout and float(config_request_timeout) or REQUEST_TIMEOUT # pylint: disable=consider-using-ternary
        # Send request for stream data directly(without 'with' statement) so that
        # exception handling and yielding can be utilized properly below
        resp = session.send(req, stream=True, timeout=request_timeout)

        if 'Too Many Requests' in resp.reason:
            retry_after = 30
            LOGGER.info("Rate limit reached. Sleeping for %s seconds",
                        retry_after)
            time.sleep(retry_after)
            raise Server42xRateLimitError(resp.reason)

        resp.raise_for_status() # Check for requests status and raise exception in failure

        # Separated yielding of records into a new function and called that here
        # so that any exception raised from the session.send can be thrown from here and
        # handled properly using backoff on request function.
        return self.send_records(resp)

    def send_records(self, resp):
        # Yielding records from results
        for item in ijson.items(resp.raw, 'results.item'):
            yield humps.decamelize(item)
        resp.close()

    def sync(self, state, start_date=None, key_id=None):
        stream_response = self.request(self.name, json=self.get_body()) or []

        # Get and intialize sub-stream for the current stream
        if STREAMS.get(SUB_STREAMS.get(self.name)):
            sub_stream = STREAMS.get(SUB_STREAMS.get(self.name))(self.config)
        else:
            sub_stream = None

        # Sync substream if the current stream has sub-stream and selected in the catalog
        if stream_response and sub_stream and sub_stream.is_selected():
            self.sync_substream(state, self, sub_stream, stream_response)

            # Get parent data again as stream_response returned from request() is a generator
            # which flush out during sync_substream call above
            stream_response = self.request(self.name, json=self.get_body()) or []

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

        # Calculate lookback window
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
        # Transform data of accounts into one level dictionary with following transformation
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

        # Set lookback window
        lookback = bookmark_dttm - timedelta(
            days=self.lookback_window())

        # get events data
        events = self.get_events(lookback, state, bookmark_dttm)
        update_currently_syncing(state, None)
        return (self.stream, events)

    def get_events(self, window_start_date, state, bookmark_dttm):
        # initialize start date as max bookmark
        max_bookmark = bookmark_dttm
        # Get period type from config and make request for event's data
        period = self.config.get('period')
        # date format to filter
        date = "date({}, {}, {})"
        # get events date window size
        events_date_window_size = int(self.config.get('events_date_window', 30))
        update_currently_syncing(state, self.name)

        while True:

            # get year, month and day from the start date
            # if the start date is '2021-01-01' and 'events_date_window' is 25 days
            # then the window end date will be '2021-01-26'
            start_date, end_date = round_times(window_start_date, window_start_date + timedelta(days=events_date_window_size))

            # create start filter
            start = date.format(start_date.year, start_date.month, start_date.day)
            # create end filter
            end = date.format(end_date.year, end_date.month, end_date.day)

            body = self.get_body(period, start, end)
            events = self.request(self.name, json=body) or []
            event = None

            # loop over every event and yield
            for event in events:
                yield event

            # as we are fetching the data in sorted manner (ascending),
            # the last event will contain the highest bookmark value
            if event:
                replication_value = event.get(humps.decamelize(self.replication_key))
                bookmark_value = strptime_to_utc(strftime(datetime.fromtimestamp(replication_value / 1000, timezone.utc)))
                max_bookmark = max(bookmark_value, max_bookmark)
                self.update_bookmark(state, self.name, strftime(max_bookmark), self.replication_key)

            if self.period == 'dayRange':
                # for 'dayRange' the data is aggregated by day,
                # hence adding 1 day more in the next start date ie.
                # Old Start date = date(2021, 1, 1), Old End date = date(2021, 1, 31)
                # New Start date = date(2021, 2, 1), New End date = date(2021, 3, 3)
                window_start_date = end_date + timedelta(days=1)
            else:
                # for 'hourRange' the data is aggregated by hour, adding 1 day in the next start date
                # results in data loss hence keeping previous end date as the next start date
                # Old Start date = date(2021, 1, 1), Old End date = date(2021, 1, 31)
                # New Start date = date(2021, 1, 31), New End date = date(2021, 3, 2)
                # Note: This may result into data duplication of boundary hours of
                #   consecutive date windows which will be handled at the target side
                window_start_date = end_date

            # break the loop if the starting window is greater than now
            # the Pendo API supports passing future date, the data will be collected till the current date
            if window_start_date > now():
                break

    def transform(self, record):
        return humps.decamelize(record)

    def get_body(self, period, first, end):
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
                            "last": end
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

        # Get bookmark from state or start date for the stream
        bookmark_date = self.get_bookmark(state, self.name,
                                          self.config.get('start_date'),
                                          self.replication_key)
        bookmark_dttm = strptime_to_utc(bookmark_date)

        # Set lookback window
        lookback = bookmark_dttm - timedelta(
            days=self.lookback_window())
        ts = int(lookback.timestamp()) * 1000

        # Get period type from config and make request for event's data
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
    # the endpoint attribute overriden and re-initialized with different endpoint URL and method
    endpoint = Endpoints("/api/v1/report", "GET")

    def sync(self, state, start_date=None, key_id=None):
        reports = self.request(self.name)
        for report in reports:
            yield (self.stream, report)


class MetadataVisitor(Stream):
    name = "metadata_visitor"
    replication_method = "FULL_TABLE"
    # the endpoint attribute overriden and re-initialized with different endpoint URL and method
    endpoint = Endpoints("/api/v1/metadata/schema/visitor", "GET")

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
    headers = {
        'content-type': 'application/x-www-form-urlencoded'
    }
    params = {
        "starttime": "start_time"
    }
    # the endpoint attribute overriden and re-initialized with different endpoint URL, method, headers and params
    # the visitorId parameter will be formatted in the get_url() function of the endpoints class
    endpoint = Endpoints(
        "/api/v1/visitor/{visitorId}/history", "GET", headers, params)

    def get_params(self, start_time):
        return {"starttime": start_time}

    def sync(self, state, start_date=None, key_id=None):
        update_currently_syncing(state, self.name)

        # using "start_date" that is passed and not using the bookmark
        # value stored in the state file, as it will be updated after
        # every sync of child stream for parent stream
        abs_start, abs_end = get_absolute_start_end_time(start_date)
        lookback = abs_start - timedelta(days=self.lookback_window())
        window_next = lookback

        # Get data with sliding window upto abs_end
        while window_next <= abs_end:
            ts = int(window_next.timestamp()) * 1000
            params = self.get_params(start_time=ts)
            visitor_history = self.request(endpoint=self.name,
                                           params=params,
                                           visitorId=key_id)
            for visitor in visitor_history:
                visitor['visitorId'] = key_id
                yield visitor
            window_next = window_next + timedelta(days=self.DATE_WINDOW_SIZE) # Update window for next call

    def transform(self, record):
        max_value = max(record.get('ts', 0), record.get('last_ts', 0))
        record['modified_ts'] = max_value
        return super().transform(record)


class Visitors(LazyAggregationStream):
    name = "visitors"
    replication_method = "INCREMENTAL"
    replication_key = "lastupdated"
    key_properties = ["visitor_id"]

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
        # Transform data of accounts into one level dictionary with following transformation
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
    # the endpoint attribute overriden and re-initialized with different endpoint URL and method
    endpoint = Endpoints("/api/v1/metadata/schema/account", "GET")

    def get_body(self):
        return None

    def sync(self, state, start_date=None, key_id=None):
        stream_response = self.request(self.name, json=self.get_body())

        # Get and intialize sub-stream for the current stream
        if STREAMS.get(SUB_STREAMS.get(self.name)):
            sub_stream = STREAMS.get(SUB_STREAMS.get(self.name))(self.config)
        else:
            sub_stream = None

        # Sync substream if the current stream has sub-stream and selected in the catalog
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
    # the endpoint attribute overriden and re-initialized with different endpoint URL and method
    endpoint = Endpoints("/api/v1/metadata/schema/visitor", "GET")

    def get_body(self):
        return None

    def sync(self, state, start_date=None, key_id=None):
        stream_response = self.request(self.name, json=self.get_body())

        # Get and intialize sub-stream for the current stream
        if STREAMS.get(SUB_STREAMS.get(self.name)):
            sub_stream = STREAMS.get(SUB_STREAMS.get(self.name))(self.config)
        else:
            sub_stream = None

        # Sync substream if the current stream has sub-stream and selected in the catalog
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
