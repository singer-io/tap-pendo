# pylint: disable=E1101,R0201,R0904,W0222,W0613

#!/usr/bin/env python3
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from ast import literal_eval

import backoff
import humps
import ijson
import requests
import singer
from urllib3.exceptions import ReadTimeoutError
import singer.metrics as metrics
from requests.exceptions import HTTPError
from requests.models import ProtocolError
from singer import Transformer, metadata
from singer.utils import now, strftime, strptime_to_utc
from tap_pendo import utils as tap_pendo_utils


KEY_PROPERTIES = ['id']
US_BASE_URL = "https://app.pendo.io"
EU_BASE_URL = "https://app.eu.pendo.io"
API_RECORD_LIMIT = 100_000

LOGGER = singer.get_logger()
session = requests.Session()
# timeout request after 300 seconds
REQUEST_TIMEOUT = 300
DEFAULT_INCLUDE_ANONYMOUS_VISITORS = 'false'

BACKOFF_FACTOR = 5

def to_giveup(error):
    """
        Boolean function to return if we want to give up retrying based on error response
    """
    return error.response is not None and 400 <= error.response.status_code < 500

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


def retry_handler(details):
    """Sets request retry count which is used to set request timeout in subsequent request"""
    _, exception, _ = sys.exc_info()
    if isinstance(exception, (ReadTimeoutError, requests.exceptions.RequestException)):
        Stream.request_retry_count = details['tries'] + 1


def reset_request_retry_count(details):
    """Reset the Stream retry count in case we continue execution even after max. retries"""
    Stream.request_retry_count = 1


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

    def get_url(self, integration_key, **kwargs):
        """
        Concatenate  and format the dynamic values to the BASE_URL
        """
        # Update url if integration_key ends with `.eu`.
        if integration_key[-3:] == ".eu":
            return EU_BASE_URL + self.endpoint.format(**kwargs)
        else:
            return US_BASE_URL + self.endpoint.format(**kwargs)



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
    request_retry_count = 1
    last_processed = None

    # initialized the endpoint attribute which can be overriden by child streams based on
    # the different parameters used by the stream.
    endpoint = Endpoints("/api/v1/aggregation", "POST")

    def __init__(self, config=None):
        self.config = config
        self.record_limit = self.get_default_record_limit()
        self.empty_replication_key_records = []

        # If value is 0,"0", "" or None then it will set default to default to 300.0 seconds if not passed in config.
        config_request_timeout = self.config.get('request_timeout')
        if config_request_timeout and float(config_request_timeout) > 0:
            self.request_timeout = float(config_request_timeout)
        else:
            self.request_timeout = REQUEST_TIMEOUT

    def get_default_record_limit(self):
        # Record limit will throttle the number of records getting replicated
        # This limit will resolve request timeouts and will reduce the peak memory consumption
        record_limit = str(self.config.get('record_limit', API_RECORD_LIMIT)).strip()
        try:
            # Defualt record limit will be set for None and Whitespaces
            # Whitespaces before and after will be trimmed around valid numeric strings
            return int(literal_eval(record_limit) if record_limit.strip() else API_RECORD_LIMIT)
        except (NameError, SyntaxError, ValueError) as e:
            raise ValueError("Invalid numeric value: " + str(self.config.get('record_limit'))) from e

    def send_request_get_results(self, req, endpoint, params, count, **kwargs):
        # Increament timeout duration based on retry attempt
        resp = session.send(req, timeout=Stream.request_retry_count*self.request_timeout)

        # Sleep for provided time and retry if rate limit exceeded
        if 'Too Many Requests' in resp.reason:
            retry_after = 30
            LOGGER.info("Rate limit reached. Sleeping for %s seconds",
                        retry_after)
            time.sleep(retry_after)
            raise Server42xRateLimitError(resp.reason)

        resp.raise_for_status() # Check for requests status and raise exception in failure
        Stream.request_retry_count = 1    # Reset retry count after success

        dec = humps.decamelize(resp.json())
        return dec

    # backoff for Timeout error is already included in "requests.exceptions.RequestException"
    # as it is the parent class of "Timeout" error
    @backoff.on_exception(backoff.expo, (requests.exceptions.RequestException, Server42xRateLimitError),
                          max_tries=7,
                          giveup=lambda e: e.response is not None and 400 <= e.
                          response.status_code < 500,
                          factor=BACKOFF_FACTOR,
                          jitter=None,
                          on_backoff=retry_handler,
                          on_giveup=reset_request_retry_count)
    @backoff.on_exception(backoff.expo, (ConnectionError, ProtocolError, ReadTimeoutError), # backoff error
                          max_tries=7,
                          factor=2,
                          jitter=None)
    @tap_pendo_utils.ratelimit(1, 2)
    def request(self, endpoint, params=None, count=1, **kwargs):
        # Set requests headers, url, methods, params and extra provided arguments
        # params = params or {}
        headers = {
            'x-pendo-integration-key': self.config['x_pendo_integration_key'],
            'content-type': 'application/json'
        }

        request_kwargs = {
            'url': self.endpoint.get_url(self.config['x_pendo_integration_key'], **kwargs),
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

        return self.send_request_get_results(req, endpoint, params, count, **kwargs)

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

        schema_file = f"schemas/{self.name}.json"
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

    def get_last_processed(self, state, sub_stream):
        return self.get_bookmark(state,
                                 sub_stream.name,
                                 None,
                                 key="last_processed")

    def update_child_stream_bookmarks(self, state, sub_stream, last_processed_value, new_bookmark, previous_sync_completed_ts):
        """Updates the bookmark keys for the child streams"""

        # Last processed is none when all events/history of all parents is processed
        if last_processed_value:
            self.update_bookmark(state=state, stream=sub_stream.name, bookmark_value=last_processed_value, bookmark_key="last_processed")

        # This bookmark handles the intermmediate bookmarking of child streams
        self.update_bookmark(state=state, stream=sub_stream.name, bookmark_value=new_bookmark, bookmark_key=sub_stream.replication_key)

        # On ressuming, once replication of bookmarked parent_id is done, for next parent_id replication should start from original bookmark
        self.update_bookmark(state=state, stream=sub_stream.name, bookmark_value=previous_sync_completed_ts, bookmark_key="previous_sync_completed_ts")

    def sync_substream(self, state, parent, sub_stream, parent_response):
        # If last sync was interrupted, get last processed parent record
        last_processed = self.get_last_processed(state, sub_stream)

        # If last replication was interrupted, interrupted parent_id replication resumes from this bookmark
        last_replication_date = self.get_bookmark(state,
                                                  sub_stream.name,
                                                  self.config.get('start_date'),
                                                  key=sub_stream.replication_key)

        # If last replication was interrupted, next parent_id replication resumes from this bookmark
        previous_sync_completed_ts = self.get_bookmark(state,
                                                       sub_stream.name,
                                                       None,
                                                       key="previous_sync_completed_ts")
        previous_sync_completed_ts = previous_sync_completed_ts if previous_sync_completed_ts else last_replication_date

        # Set latest value of this bookmark on successful replication of stream
        final_bookmark = strptime_to_utc(previous_sync_completed_ts)

        singer.write_schema(sub_stream.name,
                            sub_stream.stream.schema.to_dict(),
                            sub_stream.key_properties)

        # Loop over records of parent stream
        for record in parent_response:
            try:
                if last_processed and record.get(parent.key_properties[0]) < last_processed:
                    # Skipping last synced parent ids
                    continue

                if record.get(parent.key_properties[0]) == last_processed:
                    # Set bookmark to ressume last processed parent id replication
                    bookmark_dttm = strptime_to_utc(last_replication_date)
                else:
                    # Reset bookmark of next parent id to original bookmark
                    bookmark_dttm = strptime_to_utc(previous_sync_completed_ts)

                # It will be used while setting up
                new_bookmark = bookmark_dttm

                # Filtering the visitors based on last updated to improve performance of visitor_history replication
                if isinstance(parent, Visitors):
                    if record['metadata'].get('pendo', {'donotprocess': False}).get('donotprocess'):
                        # If any visitor is set the Do Not Process flag then Pendo will stop collecting events
                        # from that visitor and its records will be missing required keys, so we must skip it.
                        LOGGER.info("Record marked as 'Do Not Process': %s", record[parent.key_properties[0]])
                        continue

                    last_processed = self.get_last_processed(state, sub_stream)
                    # If there is no last_update key then set 'last_updated=now()' and fetch the records from recent bookmark
                    parent_last_updated = datetime.fromtimestamp(float(record['metadata']['auto'].get(
                        self.replication_key, now().timestamp() * 1000)) / 1000.0, timezone.utc)

                if isinstance(parent, Visitors) and bookmark_dttm > parent_last_updated + timedelta(days=1):
                    LOGGER.info("No new updated records for visitor id: %s", record[parent.key_properties[0]])
                else:
                    # Initialize counter and transformer with specific datetime format
                    with metrics.record_counter(
                            sub_stream.name) as counter, Transformer(
                                integer_datetime_fmt=
                                "unix-milliseconds-integer-datetime-parsing"
                            ) as transformer:

                        # During historical syncs, child streams may have large amount of records which may lead to OOM issues
                        # To avoid that we will spit-out records in batches into the target
                        loop_for_records = True
                        while loop_for_records:
                            # syncing child streams from start date or state file date
                            if isinstance(parent, Visitors):
                                stream_events = sub_stream.sync(state, bookmark_dttm,
                                                                record.get(parent.key_properties[0]),
                                                                parent_last_updated)
                                # Visitor_history API extracts records per day so we will extract all the records at once
                                loop_for_records = False
                            else:
                                (_, stream_events), loop_for_records = sub_stream.sync(state, bookmark_dttm,
                                                                                       record.get(parent.key_properties[0]))

                            # Loop over data of sub-stream
                            for event in stream_events:

                                # Get metadata for the stream to use in transform
                                schema_dict = sub_stream.stream.schema.to_dict()
                                stream_metadata = metadata.to_map(sub_stream.stream.metadata)

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
                                        json.dumps( schema_dict,
                                                    sort_keys=True,
                                                    indent=2))
                                    raise err

                                # Check for replication_value from record and if value found then use it for updating bookmark
                                replication_value = transformed_record.get(sub_stream.replication_key)
                                if replication_value:
                                    event_time = strptime_to_utc(replication_value)

                                    # visitor_history stream replicates duplicate records older than bookmark value
                                    # Skip older events than the previous sync
                                    if isinstance(parent, Visitors):
                                        previous_sync_completed_time = strptime_to_utc(
                                            previous_sync_completed_ts) - timedelta(days=self.lookback_window())
                                        if event_time < previous_sync_completed_time:
                                            continue

                                    new_bookmark = max(new_bookmark, event_time)

                                counter.increment()
                                singer.write_record(sub_stream.stream.tap_stream_id,
                                                    transformed_record)

                            self.update_child_stream_bookmarks(state=state,
                                                               sub_stream=sub_stream,
                                                               last_processed_value=record.get(parent.key_properties[0]),
                                                               new_bookmark=strftime(new_bookmark),
                                                               previous_sync_completed_ts=previous_sync_completed_ts)

                            # When all sync completes, set this as new bookmark
                            final_bookmark = max(final_bookmark, new_bookmark)

            except HTTPError:
                LOGGER.warning(
                    "Unable to retrieve %s Event for Stream (ID: %s)",
                    sub_stream.name, record[parent.key_properties[0]])

        # After processing for all parent ids we can remove our resumption state
        state.get('bookmarks').get(sub_stream.name).pop('last_processed')

        self.update_child_stream_bookmarks(state=state,
                                           sub_stream=sub_stream,
                                           last_processed_value=None,
                                           new_bookmark=strftime(final_bookmark),
                                           previous_sync_completed_ts=strftime(final_bookmark))
        update_currently_syncing(state, None)

    def get_pipeline_key_index(self, body, search_key):
        for index, param in enumerate(body['request']['pipeline']):
            if list(param.keys())[0] == search_key:
                return index
        return None

    def get_body(self, key_id=None, period=None, first=None):
        """This method will be overriden in the child class"""
        return {}

    def set_request_body_filters(self, body, start_time, records=None):
        """Sets the filter parameter in the request body"""
        # Note: even after we set filter value as first, while processing it will be set to the nearest day/hour range slot
        # so we need to provide record filter to avoid any duplicate record
        # Also we are using limit parameter as well which takes first N records for processing, rest records get discarded
        # Considering this we may need to increase record limit in case record limit has reached in last response

        limit_index = self.get_pipeline_key_index(body, 'limit')
        filter_index = self.get_pipeline_key_index(body, 'filter')

        body['request']['pipeline'][limit_index]['limit'] = self.record_limit
        if isinstance(self, Accounts):
            replication_key = 'metadata.auto.lastupdated'
            replication_key_value = records[-1]['metadata']['auto']['lastupdated'] if records and len(records) > 0 else None
        else:
            replication_key = humps.camelize(self.replication_key)
            replication_key_value = records[-1].get(humps.decamelize(self.replication_key)) if records and len(records) > 0 else None

        # non-event based streams don't have filter parameter applied
        if filter_index:
            body['request']['pipeline'][filter_index]['filter'] = f'{replication_key}>={replication_key_value or start_time}'

        return body

    def get_replication_key_value(self, record):
        """Returns the replication key value stored in the record"""
        if isinstance(self, Accounts):
            return record['metadata']['auto']['lastupdated']

        decamelized_replication_key = humps.decamelize(self.replication_key)
        return record.get(decamelized_replication_key)

    def remove_empty_replication_key_records(self, records):
        """Removes the none replication_value records to avoid duplicates records"""
        if len(records) > 0:
            for index in range(len(records)-1, -1, -1):
                if self.get_replication_key_value(records[index]):
                    continue

                last_record = records.pop(index)
                if last_record not in self.empty_replication_key_records:
                    # add removed records to be synced at the end
                    self.empty_replication_key_records.append(last_record)

    def remove_last_timestamp_records(self, records):
        """Removes the overlapping records with last timestamp value. This avoids possibilty of duplicates"""
        last_processed = []

        if len(records) > 0:
            if records and self.last_processed:
                # Previously removed records get duplicated in subsequent api response which needs to be removed
                records = [record for record in records if self.get_replication_key_value(
                    record) >= self.get_replication_key_value(self.last_processed[0])]

            if records:
                timestamp = self.get_replication_key_value(records[-1])
                while records and timestamp == self.get_replication_key_value(records[-1]):
                    last = records.pop()
                    last_processed.append(last)

        # This is a corner cases where all records in the set have same timestamp
        # This can occur if record limit is set very smaller compared to the max record limit
        # In this case we will try to set record limit to max limit to make it harder to run into this issue
        # But still can't completely dismiss the minor possibility of this issue occurring
        if len(records) == 0 or not last_processed:
            self.record_limit = API_RECORD_LIMIT

        return records, last_processed

    def get_first_parameter_value(self, body):
        return body['request']['pipeline'][0]['source']['timeSeries'].get('first', 0)

    def set_time_series_first(self, body, records, first=None):
        """Sets the timeSeries 'first' parameter in request body"""
        if len(records) > 1:
            # This condition considers that within current time window there could be some more records
            # So we will set last record timestamp as first of time series
            # Note: even if we set browser time as first but while processing it will set to nearest day/hour range slot
            body['request']['pipeline'][0]['source']['timeSeries']['first'] = records[-1].get(self.replication_key)
        elif first:
            body['request']['pipeline'][0]['source']['timeSeries']['first'] = self.get_first_parameter_value(body)
        else:
            body['request']['pipeline'][0]['source']['timeSeries']['first'] = int(datetime.now().timestamp() * 1000)

        return body

    def sync(self, state, start_date=None, key_id=None, parent_last_updated=None):
        update_currently_syncing(state, self.name)

        if STREAMS.get(SUB_STREAMS.get(self.name)):
            sub_stream = STREAMS.get(SUB_STREAMS.get(self.name))(self.config)
        else:
            sub_stream = None

        # If last processed records exists, then set first to timestamp of first record
        if self.last_processed:
            first = self.get_replication_key_value(self.last_processed[0])
        else:
            first = int(start_date.timestamp()) * 1000

        # Setup body for first request
        body = self.get_body()
        self.set_request_body_filters(body, first, [])

        stream_records = []
        loop_for_records = False
        while True:
            # Loop breaks when paged response returns lesser records than set record limit
            records = self.request(self.name, json=body).get('results') or []

            if len(records) > 1:
                self.remove_empty_replication_key_records(records)
                records, removed_records = self.remove_last_timestamp_records(records)
                stream_records += records

                # Set first and filters for next request
                self.set_request_body_filters(body, first, records)

                if self.last_processed == removed_records:
                    stream_records += removed_records
                    self.last_processed = None
                    break

                self.last_processed = removed_records

            elif len(records) <= 1:
                stream_records += records
                self.last_processed = None
                break

            # If record limit set is reached, then return the extracted records
            # Set the last processed records to ressume the extraction
            if len(stream_records) >= self.record_limit:
                loop_for_records = True
                break

        # These is a corner cases where this limit may get changed so reseeting it before next iteration
        self.record_limit = self.get_default_record_limit()

        if not loop_for_records:
            # Add none replication_value records into records with valid replication_value
            # before syncing the sud-stream records
            stream_records.extend(self.empty_replication_key_records)
            self.empty_replication_key_records = []

        # Sync substream if the current stream has sub-stream and selected in the catalog
        if stream_records and sub_stream and sub_stream.is_selected():
            self.sync_substream(state, self, sub_stream, stream_records)

        update_currently_syncing(state, None)
        return (self.stream, stream_records), loop_for_records

    def lookback_window(self):
        # Get lookback window from config and verify value
        lookback_window = self.config.get('lookback_window') or '0'
        if not lookback_window.isdigit():
            raise TypeError(f"lookback_window '{lookback_window}' is not numeric. Check your configuration")
        return int(lookback_window)


class LazyAggregationStream(Stream):
    def send_request_get_results(self, req, endpoint, params, count, **kwargs):
        # Set request timeout to config param `request_timeout` value.
        # If value is 0,"0", "" or None then it will set default to default to 300.0 seconds if not passed in config.
        try:
            # Send request for stream data directly(without 'with' statement) so that
            # exception handling and yielding can be utilized properly below
            # Increament timeout duration based on retry attempt
            resp = session.send(req, stream=True, timeout=count*self.request_timeout)

            if 'Too Many Requests' in resp.reason:
                retry_after = 30
                LOGGER.info("Rate limit reached. Sleeping for %s seconds",
                            retry_after)
                time.sleep(retry_after)
                raise Server42xRateLimitError(resp.reason)

            resp.raise_for_status() # Check for requests status and raise exception in failure]

            # Get records from the raw response
            for item in ijson.items(resp.raw, 'results.item'):
                count = 1   # request succeeded resetting the retry count to 0
                yield humps.decamelize(item)
            resp.close()
        except (ConnectionError, ProtocolError, ReadTimeoutError, requests.exceptions.RequestException, Server42xRateLimitError) as e:
            # Catch requestException and raise errors if we have to give up for certain conditions
            if isinstance(e, requests.exceptions.RequestException) and to_giveup(e):
                raise e from None
            # Raise error if we have retried for 7 times
            if count == 7:
                LOGGER.error("Giving up request(...) after 7 tries (%s: %s)", e.__class__.__name__, str(e))
                raise e from None

            # Sleep for [0.5, 1, 2, 4, 10, 10, ... ] minutes
            backoff_time = min(2 ** (count - 1) * BACKOFF_FACTOR, 600)
            LOGGER.info("Backing off request(...) for %ss (%s: %s)", backoff_time, e.__class__.__name__, str(e))

            time.sleep(backoff_time)
            count += 1
            # Request retry
            yield from self.request(endpoint, params, count, **kwargs)

    def sync(self, state, start_date=None, key_id=None, parent_last_updated=None):
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

        return (self.stream, stream_response), False

class EventsBase(Stream):
    DATE_WINDOW_SIZE = 1
    key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip']
    replication_method = "INCREMENTAL"

    def __init__(self, config):
        super().__init__(config=config)
        self.config = config
        self.period = config.get('period')
        self.replication_key = "day" if self.period == 'dayRange' else "hour"
        self.last_processed = None


    def get_body(self, key_id, period, first):
        """This method returns generic request body of events steams"""

        sort_key = humps.camelize(self.replication_key)
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "pipeline": [
                    {
                        "source": {
                            "timeSeries": {
                                "period": period,
                                "first": first,
                                "last": "now()"
                            }
                        }
                    }, {
                        "sort": [sort_key]
                    }, {
                        "filter": f"{sort_key}>=1"
                    }, {
                        "limit": self.record_limit
                    }
                ]
            }
        }

    def get_first_parameter_value(self, body):
        return body['request']['pipeline'][0]['source']['timeSeries'].get('first', 0)

    def sync(self, state, start_date=None, key_id=None, parent_last_updated=None):
        update_currently_syncing(state, self.name)

        # Calculate lookback window
        lookback = start_date - timedelta(
            days=self.lookback_window())

        # Period always amounts to a day either aggegated by day or hours in a day
        period = self.config.get('period')

        # If last processed records exists, then set first to timestamp of first record
        try:
            first = self.last_processed[0][humps.decamelize(
                self.replication_key)] if self.last_processed else int(lookback.timestamp()) * 1000
        except Exception as e:
            LOGGER.info(str(e))

        # Setup body for first request
        body = self.get_body(key_id, period, first)
        self.set_time_series_first(body, [], first)
        self.set_request_body_filters(body, first, [])

        ts_now = int(datetime.now().timestamp() * 1000)
        events = []
        while self.get_first_parameter_value(body) <= ts_now:
            records = self.request(self.name, json=body).get('results') or []
            self.set_time_series_first(body, records)

            if len(records) > 1:
                if len(records) < self.record_limit:
                    # If response returns less records than record limit means there are no more records to sync
                    events += records
                    self.last_processed = None
                    break

                # Remove last processed and none replication_value records
                self.remove_empty_replication_key_records(records)
                records, removed_records = self.remove_last_timestamp_records(records)
                if len(records) > 0:
                    events += records

                    if self.last_processed == removed_records:
                        events += removed_records
                        self.last_processed = None
                        break

                    self.last_processed = removed_records
                else:
                    # This block handles race condition where all records have same replication key value
                    first = self.last_processed[0][humps.decamelize(
                        self.replication_key)] if self.last_processed else int(lookback.timestamp()) * 1000

                    body = self.get_body(key_id, period, first)
                    continue

            elif len(records) == 1:
                events += records
                self.last_processed = None
                break

            # Set first and filters for next request
            self.set_request_body_filters(
                body,
                self.get_first_parameter_value(body),
                records)

            # If record limit set is reached, then return the extracted records
            # Set the last processed records to ressume the extraction
            if len(events) >= self.record_limit:
                return (self.stream, events), True

        # These is a corner cases where this limit may get changed so reseeting it before next iteration
        self.record_limit = self.get_default_record_limit()

        # Add none replication_value records into records with valid replication_value
        events.extend(self.empty_replication_key_records)
        self.empty_replication_key_records = []

        update_currently_syncing(state, None)
        return (self.stream, events), False


class Accounts(Stream):
    name = "accounts"
    replication_method = "INCREMENTAL"
    replication_key = "lastupdated"
    key_properties = ["account_id"]

    def get_body(self, key_id=None, period=None, first=None):
        return {
            "response": {
                "mimeType": "application/json"
            },
            "request": {
                "name": "all-accounts",
                "pipeline": [{
                    "source": {
                        "accounts": None
                    },
                }, {
                    "sort": ["metadata.auto.lastupdated"]
                }, {
                    "filter": "metadata.auto.lastupdated>=1"
                }, {
                    "limit": self.record_limit
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

    def get_body(self, key_id=None, period=None, first=None):
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
                    "sort": [f"{self.replication_key}"]
                }, {
                    "limit": self.record_limit
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
        body = super().get_body(key_id, period, first)
        body['request']['pipeline'][0]['source'].update({"featureEvents": {"featureId": key_id,}})
        return body


class Events(EventsBase):
    name = "events"
    DATE_WINDOW_SIZE = 1
    key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip']
    replication_method = "INCREMENTAL"

    def __init__(self, config):
        super().__init__(config=config)
        self.config = config
        self.period = config.get('period')
        self.replication_key = "day" if self.period == 'dayRange' else "hour"

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

    def get_body(self, key_id, period, first):
        body = super().get_body(key_id, period, first)
        body['request']['pipeline'][0]['source'].update({"events": None})
        return body



class PollEvents(EventsBase):
    replication_method = "INCREMENTAL"
    name = "poll_events"
    key_properties = ['visitor_id', 'account_id', 'server_name', 'remote_ip']

    def __init__(self, config):
        super().__init__(config=config)
        self.config = config
        self.period = config.get('period')
        self.replication_key = 'browser_time'

    def get_body(self, key_id, period, first):
        body = super().get_body(key_id, period, first)
        body['request']['pipeline'][0]['source'].update({"pollEvents": None})
        return body


class TrackEvents(EventsBase):
    replication_method = "INCREMENTAL"
    name = "track_events"
    key_properties = ['visitor_id', 'account_id', 'server', 'remote_ip']


    def get_body(self, key_id, period, first):
        body = super().get_body(key_id, period, first)
        body['request']['pipeline'][0]['source'].update({"trackEvents": {"trackTypeId": key_id,}})
        return body

class GuideEvents(EventsBase):
    replication_method = "INCREMENTAL"
    name = "guide_events"
    key_properties = ['visitor_id', 'account_id', 'server_name', 'remote_ip']

    def __init__(self, config):
        super().__init__(config=config)
        self.replication_key = 'browser_time'

    def get_body(self, key_id, period, first):
        body = super().get_body(key_id, period, first)
        body['request']['pipeline'][0]['source'].update({"guideEvents": {"guideId": key_id,}})
        return body


class TrackTypes(Stream):
    name = "track_types"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def get_body(self, key_id=None, period=None, first=None):
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
                    "sort": [f"{self.replication_key}"]
                }, {
                    "limit": self.record_limit
                }],
                "requestId": "all-track-types"
            }
        }


class Guides(Stream):
    name = "guides"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def get_body(self, key_id=None, period=None, first=None):
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
                    "sort": [f"{self.replication_key}"]
                }, {
                    "limit": self.record_limit
                }],
                "requestId":
                "all-guides"
            }
        }


class Pages(Stream):
    name = "pages"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"

    def get_body(self, key_id=None, period=None, first=None):
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
                    "sort": [f"{self.replication_key}"]
                }, {
                    "limit": self.record_limit
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
        body = super().get_body(key_id, period, first)
        body['request']['pipeline'][0]['source'].update({"pageEvents": {"pageId": key_id,}})
        return body


class Reports(Stream):
    name = "reports"
    replication_method = "INCREMENTAL"
    replication_key = "lastUpdatedAt"
    # the endpoint attribute overriden and re-initialized with different endpoint URL and method
    endpoint = Endpoints("/api/v1/report", "GET")

    def sync(self, state, start_date=None, key_id=None, parent_last_updated=None):
        reports = self.request(self.name)
        for report in reports:
            yield (self.stream, report)


class MetadataVisitor(Stream):
    name = "metadata_visitor"
    replication_method = "FULL_TABLE"
    # the endpoint attribute overriden and re-initialized with different endpoint URL and method
    endpoint = Endpoints("/api/v1/metadata/schema/visitor", "GET")

    def sync(self, state, start_date=None, key_id=None, parent_last_updated=None):
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

    def sync(self, state, start_date=None, key_id=None, parent_last_updated=None):
        update_currently_syncing(state, self.name)

        # using "start_date" that is passed and not using the bookmark
        # value stored in the state file, as it will be updated after
        # every sync of child stream for parent stream
        abs_start, abs_end = get_absolute_start_end_time(start_date)
        lookback = abs_start - timedelta(days=self.lookback_window())
        window_next = lookback

        # Get data with sliding window upto abs_end
        # After parent last updated there won't be new records
        while window_next <= abs_end+timedelta(days=1) and window_next <= parent_last_updated+timedelta(days=1):
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

    def get_body(self, key_id=None, period=None, first=None):
        include_anonymous_visitors = self.config.get('include_anonymous_visitors') or DEFAULT_INCLUDE_ANONYMOUS_VISITORS
        anons = str(include_anonymous_visitors).lower() == 'true'
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
                            "identified": not anons
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

    def get_body(self, key_id=None, period=None, first=None):
        return None

    def sync(self, state, start_date=None, key_id=None, parent_last_updated=None):
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

    def get_body(self, key_id=None, period=None, first=None):
        return None

    def sync(self, state, start_date=None, key_id=None, parent_last_updated=None):
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



# This dict is ordered so that child stream(s) are occurring just above parent stream.
STREAMS = {
    "accounts": Accounts,
    "events": Events,
    "feature_events": FeatureEvents,
    "features": Features,
    "guide_events": GuideEvents,
    "guides": Guides,
    "metadata_accounts": MetadataAccounts,
    "metadata_visitors": MetadataVisitors,
    "page_events": PageEvents,
    "pages": Pages,
    "poll_events": PollEvents,
    "track_events": TrackEvents,
    "track_types": TrackTypes,
    "visitor_history": VisitorHistory,
    "visitors": Visitors
}

SUB_STREAMS = {
    'visitors': 'visitor_history',
    'features': 'feature_events',
    'pages': 'page_events',
    'guides': 'guide_events',
    'track_types': 'track_events'
}
