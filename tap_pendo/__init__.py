#!/usr/bin/env python3
import json
import os
import sys

import singer
from singer import metadata
from singer import metrics as singer_metrics
from singer import utils
from tap_pendo.discover import discover_streams
from tap_pendo.streams import STREAMS, SUB_STREAMS, update_currently_syncing
from tap_pendo.sync import sync_full_table, sync_stream

REQUIRED_CONFIG_KEYS = ["start_date", "x_pendo_integration_key", "period"]

LOGGER = singer.get_logger()


def do_discover(config):
    LOGGER.info("Starting discover")
    catalog = {"streams": discover_streams(config)}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def get_sub_stream_ids():
    sub_stream_ids = []
    for _, value in SUB_STREAMS.items():
        sub_stream_ids.append(value)
    return sub_stream_ids


class DependencyException(Exception):
    pass


def validate_dependencies(selected_stream_ids):
    errs = []
    msg_tmpl = ("Unable to extract {0} data. "
                "To receive {0} data, you also need to select {1}.")
    for parent_stream_id in SUB_STREAMS:
        sub_stream_id = SUB_STREAMS.get(parent_stream_id)
        # for sub_stream_id in sub_stream_ids:
        if sub_stream_id in selected_stream_ids and parent_stream_id not in selected_stream_ids:
            errs.append(msg_tmpl.format(
                sub_stream_id, parent_stream_id))

    if errs:
        raise DependencyException(" ".join(errs))


def populate_class_schemas(catalog, selected_stream_ids):
    for stream in catalog.streams:
        if stream.tap_stream_id in selected_stream_ids:
            STREAMS[stream.tap_stream_id].stream = stream


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder


def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def get_selected_streams(catalog):
    selected_stream_ids = []
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        if stream_is_selected(mdata):
            selected_stream_ids.append(stream.tap_stream_id)
    return selected_stream_ids


def sync(config, state, catalog):
    LOGGER.info("Starting with state %s", state)
    start_date = config['start_date']

    selected_stream_ids = get_selected_streams(catalog)
    validate_dependencies(selected_stream_ids)
    populate_class_schemas(catalog, selected_stream_ids)
    all_sub_stream_ids = get_sub_stream_ids()

    # Loop over streams in catalog
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        mdata = metadata.to_map(stream.metadata)
        if stream_id not in selected_stream_ids:
            LOGGER.info("%s: Skipping - not selected", stream_id)
            continue            # TODO: sync code for stream goes here...

        LOGGER.info('START Syncing: %s', stream_id)
        update_currently_syncing(state, stream_id)

        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(
            stream_id, stream.schema.to_dict(), key_properties)

        sub_stream_ids = SUB_STREAMS.get(stream_id)

        if sub_stream_ids:
            for sub_stream_id in sub_stream_ids:
                if sub_stream_id not in selected_stream_ids:
                    continue
                sub_stream = STREAMS[sub_stream_id].stream
                sub_mdata = metadata.to_map(sub_stream.metadata)
                sub_key_properties = metadata.get(
                    sub_mdata, (), 'table-key-properties')
                singer.write_schema(
                    sub_stream.tap_stream_id, sub_stream.schema.to_dict(), sub_key_properties)

        # parent stream will sync sub stream
        if stream_id in all_sub_stream_ids:
            continue

        LOGGER.info("Stream %s: Starting sync", stream_id)
        instance = STREAMS[stream_id](config)

        counter_value = 0
        if instance.replication_method == "INCREMENTAL":
            counter_value = sync_stream(state, start_date, instance)
        else:
            counter_value = sync_full_table(state, instance)
        singer.write_state(state)
        LOGGER.info("Stream %s: Completed sync (%s rows)", stream_id, counter_value)
        update_currently_syncing(state, None)
        singer.write_state(state)
    LOGGER.info("Finished sync")

@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover(args.config)
    elif args.catalog:
        state = args.state
        sync(args.config, state, args.catalog)


if __name__ == "__main__":
    main()
